package fetcher

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type FetchState struct {
	lock            sync.Mutex
	bf              *BatchFetcher
	req             *kafkaprotocol.FetchRequest
	resp            kafkaprotocol.FetchResponse
	partitionStates map[int]map[int]*PartitionFetchState
	completionFunc  func(resp *kafkaprotocol.FetchResponse) error
	timeoutTimer    *time.Timer
	bytesFetched    int
	first           bool
}

func newFetchState(authContext *auth.Context, batchFetcher *BatchFetcher, req *kafkaprotocol.FetchRequest,
	completionFunc func(response *kafkaprotocol.FetchResponse) error) (*FetchState, error) {
	fetchState := &FetchState{
		bf:              batchFetcher,
		req:             req,
		partitionStates: map[int]map[int]*PartitionFetchState{},
		completionFunc:  completionFunc,
		first:           true,
	}
	fetchState.resp.Responses = make([]kafkaprotocol.FetchResponseFetchableTopicResponse, len(fetchState.req.Topics))
	for i, topicData := range fetchState.req.Topics {
		fetchState.resp.Responses[i].Topic = topicData.Topic
		partitionResponses := make([]kafkaprotocol.FetchResponsePartitionData, len(topicData.Partitions))
		fetchState.resp.Responses[i].Partitions = partitionResponses
		topicName := *topicData.Topic
		topicInfo, topicExists, err := fetchState.bf.topicProvider.GetTopicInfo(topicName)
		if err != nil {
			return nil, err
		}
		authorised := true
		if topicExists && authContext != nil {
			authorised, err = authContext.Authorize(acls.ResourceTypeTopic, topicName, acls.OperationRead)
			if err != nil {
				return nil, err
			}
		}
		topicPartitionFetchStates := map[int]*PartitionFetchState{}
		if topicExists {
			topicPartitionFetchStates = map[int]*PartitionFetchState{}
			fetchState.partitionStates[topicInfo.ID] = topicPartitionFetchStates
		}
		if !topicExists {
			log.Warnf("fetcher: topic %s does not exist", topicName)
		}
		for j, partitionData := range topicData.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			partitionResponses[j].Records = []byte{} // client does not like nil records
			partitionID := int(partitionData.Partition)
			if !topicExists {
				partitionResponses[j].ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			} else if !authorised {
				partitionResponses[j].ErrorCode = int16(kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
			} else if partitionID < 0 || partitionID >= topicInfo.PartitionCount {
				partitionResponses[j].ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			} else {
				partHash, err := fetchState.bf.partitionHashes.GetPartitionHash(topicInfo.ID, partitionID)
				if err != nil {
					return nil, err
				}
				topicPartitionFetchStates[partitionID] = &PartitionFetchState{
					fs:                 fetchState,
					partitionFetchReq:  &partitionData,
					partitionFetchResp: &partitionResponses[j],
					topicID:            topicInfo.ID,
					partitionID:        partitionID,
					partitionHash:      partHash,
					fetchOffset:        partitionData.FetchOffset,
				}
			}
		}
	}
	return fetchState, nil
}

func (f *FetchState) read() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.completionFunc == nil {
		// Response already sent
		return nil
	}
	wouldExceedRequestMax := false
outer:
	for topicID, partitionFetchStates := range f.partitionStates {
		for partitionID, partitionFetchState := range partitionFetchStates {
			log.Debugf("handling fetch for topic %d partition %d", topicID, partitionID)
			var err error
			var wouldExceedPartitionMax bool
			wouldExceedRequestMax, wouldExceedPartitionMax, err = partitionFetchState.read()
			if err != nil {
				log.Warnf("failed to fetch from partition %v", err)
				if common.IsUnavailableError(err) {
					partitionFetchState.partitionFetchResp.ErrorCode = kafkaprotocol.ErrorCodeLeaderNotAvailable
				} else {
					partitionFetchState.partitionFetchResp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
				}
			}
			if wouldExceedRequestMax {
				break outer
			}
			if err != nil || wouldExceedPartitionMax {
				delete(partitionFetchStates, partitionID)
				if len(partitionFetchStates) == 0 {
					delete(f.partitionStates, topicID)
				}
			}
		}
	}
	if wouldExceedRequestMax || len(f.partitionStates) == 0 {
		// We either exceeded request max size or exceeded partition max size on all partitions, or errored on all
		// partitions so the request is complete
		if err := f.sendResponse(); err != nil {
			return err
		}
		return nil
	}
	if f.bytesFetched >= int(f.req.MinBytes) {
		log.Debugf("got enough data, sending response")
		// We fetched enough data
		if err := f.sendResponse(); err != nil {
			return err
		}
		return nil
	}
	// We didn't fetch enough data
	if f.req.MaxWaitMs == 0 {
		// Give up now and return no data
		f.clearFetchedRecords()
		if err := f.sendResponse(); err != nil {
			return err
		}
		return nil
	}
	if f.timeoutTimer == nil {
		// Set a timeout if we haven't already set one - as we need to wait
		f.timeoutTimer = time.AfterFunc(time.Duration(f.req.MaxWaitMs)*time.Millisecond, f.timeout)
	}
	return nil
}

func (f *FetchState) clearFetchedRecords() {
	// Clear any data that was fetched
	for i := 0; i < len(f.resp.Responses); i++ {
		for j := 0; j < len(f.resp.Responses[i].Partitions); j++ {
			f.resp.Responses[i].Partitions[j].Records = []byte{}
		}
	}
}

func (f *FetchState) sendResponse() error {
	if f.completionFunc == nil {
		// response already sent
		return nil
	}
	if err := f.completionFunc(&f.resp); err != nil {
		return err
	}
	if f.timeoutTimer != nil {
		f.timeoutTimer.Stop()
	}
	f.completionFunc = nil
	return nil
}

func (f *FetchState) timeout() {
	f.lock.Lock()
	defer f.lock.Unlock()
	if err := f.sendResponse(); err != nil {
		log.Errorf("failed to send fetch response: %v", err)
	}
}

type PartitionFetchState struct {
	fs                 *FetchState
	partitionFetchReq  *kafkaprotocol.FetchRequestFetchPartition
	partitionFetchResp *kafkaprotocol.FetchResponsePartitionData
	topicID            int
	partitionID        int
	bytesFetched       int
	fetchOffset        int64
	partitionHash      []byte
}

type queryLroGetter struct {
	topicID     int
	partitionID int
	cl          control.Client
	lro         int64
}

func (q *queryLroGetter) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	queryRes, lro, err := q.cl.QueryTablesForPartition(q.topicID, q.partitionID, keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	q.lro = lro
	return queryRes, nil
}

func (p *PartitionFetchState) read() (wouldExceedRequestMax bool, wouldExceedPartitionMax bool, err error) {
	log.Debugf("PartitionFetchState.read - partition %d offset %d", p.partitionID, p.fetchOffset)
	memberID := atomic.LoadInt32(&p.fs.bf.memberID)
	if memberID == -1 {
		return false, false,
			common.NewTektiteErrorf(common.Unavailable, "fetch before fetcher has received cluster state")
	}
	cl, err := p.fs.bf.getClient()
	if err != nil {
		return false, false, err
	}
	keyStart, keyEnd := p.createKeyStartAndEnd(p.fetchOffset)
	queryGetter := queryLroGetter{
		topicID:     p.topicID,
		partitionID: p.partitionID,
		cl:          cl,
	}
	iter, err := queryutils.CreateIteratorForKeyRange(keyStart, keyEnd, &queryGetter, p.fs.bf.tableGetter)
	if err != nil {
		return false, false, err
	}
	lastOffset := queryGetter.lro
	// High watermark is 1 + the offset of the last available message in the partition.
	p.partitionFetchResp.HighWatermark = lastOffset + 1
	p.partitionFetchResp.LastStableOffset = lastOffset + 1
	var batches []byte
	first := true
	for {
		ok, kv, err := iter.Next()
		if err != nil {
			return false, false, err
		}
		if !ok {
			break
		}
		// The sstable can contain record batches for other partitions - we filter those out
		if len(kv.Key) >= 16 && !bytes.Equal(p.partitionHash, kv.Key[:16]) {
			continue
		}
		baseOffset := kafkaencoding.BaseOffset(kv.Value)
		lastOffsetDelta := int64(kafkaencoding.LastOffsetDelta(kv.Value))
		log.Debugf("fetcher got batch with baseoffset %d lastOffsetDelta %d", baseOffset, lastOffsetDelta)
		// It's possible that multiple batches have been compacted into the same sstable and we've already seen
		// some of those batches - so we need to filter them out. we don't currently compact multiple batches into
		lastOffsetInBatch := baseOffset + lastOffsetDelta
		if lastOffsetInBatch > queryGetter.lro {
			// Not readable, we know if the last offset in a batch is not readable then no offsets in the batch are
			// readable, as we release them a table at a time.
			continue
		}
		value := common.RemoveValueMetadata(kv.Value)
		if first {
			if baseOffset < p.fetchOffset {
				// The fetch offset does not align with first offset in batch so we need to trim leading records
				value = trimLeadingRecordsFromBatch(value, p.fetchOffset)
			}
			first = false
		}
		// Note that batchSize is the *uncompressed* size - unlike Kafka which uses the compressed size
		batchSize := len(value)
		if !p.fs.first {
			if p.bytesFetched+batchSize > int(p.partitionFetchReq.PartitionMaxBytes) {
				// Would exceed partition max size
				wouldExceedPartitionMax = true
				break
			}
			if p.fs.bytesFetched+batchSize > int(p.fs.req.MaxBytes) {
				// would exceed total response max size
				wouldExceedRequestMax = true
				break
			}
		}
		value, err = p.compress(value)
		if err != nil {
			return false, false, err
		}
		batches = append(batches, value...)
		p.fs.first = false
		p.bytesFetched += batchSize
		p.fs.bytesFetched += batchSize
	}
	if len(batches) > 0 {
		p.partitionFetchResp.Records = append(p.partitionFetchResp.Records, batches...)
	}
	return
}

func trimLeadingRecordsFromBatch(bytes []byte, fetchOffset int64) []byte {
	baseOffset := int64(binary.BigEndian.Uint64(bytes))
	log.Debugf("trimming leading records from batch fetchOffset %d baseOffset %d", fetchOffset, baseOffset)
	baseTimeStamp := int64(binary.BigEndian.Uint64(bytes[27:]))
	numRecords := int(binary.BigEndian.Uint32(bytes[57:]))
	numToTrim := int(fetchOffset - baseOffset)
	off := 61
	outBaseTimestamp := int64(0)
	outMaxTimestamp := int64(0)
	for i := 0; i < numToTrim; i++ {
		recordStart := off
		recordLength, recordLengthBytesRead := binary.Varint(bytes[off:])
		off += recordLengthBytesRead
		off++ // skip past attributes
		timestampDelta, _ := binary.Varint(bytes[off:])
		recordTimestamp := baseTimeStamp + timestampDelta
		if outBaseTimestamp == 0 {
			outBaseTimestamp = recordTimestamp
		}
		if recordTimestamp > outMaxTimestamp {
			outMaxTimestamp = recordTimestamp
		}
		off = recordStart + int(recordLength) + recordLengthBytesRead
	}
	newBuff := make([]byte, len(bytes)-(off-61))
	copy(newBuff, bytes[:61])
	copy(newBuff[61:], bytes[off:])
	kafkaencoding.SetBaseTimestamp(newBuff, outBaseTimestamp)
	kafkaencoding.SetMaxTimestamp(newBuff, outMaxTimestamp)
	kafkaencoding.SetNumRecords(newBuff, numRecords-numToTrim)
	kafkaencoding.SetBatchLength(newBuff, int32(len(newBuff)-12))
	kafkaencoding.CalcAndSetCrc(newBuff)
	return newBuff
}

func (p *PartitionFetchState) compress(batch []byte) ([]byte, error) {
	currCompressionType := compress.CompressionType(kafkaencoding.CompressionType(batch))
	if currCompressionType != compress.CompressionTypeNone {
		// We also decompress and recompress the entire table so will always be compression type none
		panic(fmt.Sprintf("expected compression type none, got: %s", currCompressionType.String()))
	}
	compressionType := p.fs.bf.compressionType
	if compressionType == compress.CompressionTypeNone {
		return batch, nil
	}
	hdr := batch[:61]
	hdr = common.ByteSliceCopy(hdr)
	// It's only the part after the headers and number of records that is compressed
	compressed, err := compress.Compress(compressionType, hdr, batch[61:])
	if err != nil {
		return nil, err
	}
	// Now set new batch length
	newBatchLen := len(compressed) - 12
	kafkaencoding.SetBatchLength(compressed, int32(newBatchLen))
	// And compression type
	kafkaencoding.SetCompressionType(compressed, byte(compressionType))
	// Recalculate the CRC
	kafkaencoding.CalcAndSetCrc(compressed)
	return compressed, nil
}

func (p *PartitionFetchState) createKeyStartAndEnd(fetchOffset int64) ([]byte, []byte) {
	keyStart := make([]byte, 0, 25)
	keyStart = append(keyStart, p.partitionHash...)
	keyStart = append(keyStart, common.EntryTypeTopicData)
	keyStart = encoding.KeyEncodeInt(keyStart, fetchOffset)
	keyEnd := make([]byte, 0, 25)
	keyEnd = append(keyEnd, p.partitionHash...)
	keyEnd = append(keyEnd, common.EntryTypeTopicData)
	keyEnd = encoding.KeyEncodeInt(keyEnd, math.MaxInt64)
	return keyStart, keyEnd
}

func (p *PartitionFetchState) createIteratorForKeyRange(ids lsm.OverlappingTables, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	if len(ids) == 0 {
		return &iteration.EmptyIterator{}, nil
	}
	tableGetter := p.fs.bf.getTableFromCache
	var iters []iteration.Iterator
	for _, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			info := nonOverLapIDs[0]
			iter, err := sst.NewLazySSTableIterator(info.ID, tableGetter, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tableGetter, keyStart, keyEnd)
				if err != nil {
					return nil, err
				}
				itersInChain[j] = iter
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	var iter iteration.Iterator
	if len(iters) > 1 {
		var err error
		iter, err = iteration.NewMergingIterator(iters, false, math.MaxUint64)
		if err != nil {
			return nil, err
		}
	} else {
		iter = iters[0]
	}
	return iter, nil
}

func (p *PartitionFetchState) createIteratorFromTabIDs(tableIDs []*sst.SSTableID, fromOffset int64, lastReadableOffset int64) (iteration.Iterator, error) {
	if len(tableIDs) == 0 {
		return &iteration.EmptyIterator{}, nil
	}
	keyStart := make([]byte, 0, 25)
	keyStart = append(keyStart, p.partitionHash...)
	keyStart = append(keyStart, common.EntryTypeTopicData)
	keyStart = encoding.KeyEncodeInt(keyStart, fromOffset)
	keyEnd := make([]byte, 0, 25)
	keyEnd = append(keyEnd, p.partitionHash...)
	keyEnd = append(keyEnd, common.EntryTypeTopicData)
	keyEnd = encoding.KeyEncodeInt(keyEnd, lastReadableOffset+1)
	var iter iteration.Iterator
	if len(tableIDs) > 1 {
		iters := make([]iteration.Iterator, len(tableIDs))
		for i, tid := range tableIDs {
			sstIter, err := sst.NewLazySSTableIterator(*tid, p.fs.bf.getTableFromCache, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters[i] = sstIter
		}
		iter = iteration.NewChainingIterator(iters)
	} else {
		var err error
		iter, err = sst.NewLazySSTableIterator(*tableIDs[0], p.fs.bf.getTableFromCache, keyStart, keyEnd)
		if err != nil {
			return nil, err
		}
	}
	return iter, nil
}
