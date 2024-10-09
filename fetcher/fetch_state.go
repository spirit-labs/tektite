package fetcher

import (
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/sst"
	"math"
	"sync"
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
	readExec        *readExecutor
	bytesFetched    int
	first           bool
}

func (f *FetchState) init() error {
	f.resp.Responses = make([]kafkaprotocol.FetchResponseFetchableTopicResponse, len(f.req.Topics))
	for i, topicData := range f.req.Topics {
		f.resp.Responses[i].Topic = topicData.Topic
		partitionResponses := make([]kafkaprotocol.FetchResponsePartitionData, len(topicData.Partitions))
		f.resp.Responses[i].Partitions = partitionResponses
		topicName := *topicData.Topic
		topicInfo, err := f.bf.topicProvider.GetTopicInfo(topicName)
		topicExists := true
		if err != nil {
			if !common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist) {
				log.Warnf("failed to get topic info: %v", err)
			}
			topicExists = false
		}
		topicPartitionFetchStates := map[int]*PartitionFetchState{}
		if topicExists {
			topicPartitionFetchStates = map[int]*PartitionFetchState{}
			f.partitionStates[topicInfo.ID] = topicPartitionFetchStates
		}
		for j, partitionData := range topicData.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			partitionID := int(partitionData.Partition)
			if !topicExists {
				partitionResponses[j].ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			} else if partitionID < 0 || partitionID >= topicInfo.PartitionCount {
				partitionResponses[j].ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			} else {
				partHash, err := f.bf.partitionHashes.GetPartitionHash(topicInfo.ID, partitionID)
				if err != nil {
					return err
				}
				topicPartitionFetchStates[partitionID] = &PartitionFetchState{
					fs:                 f,
					partitionFetchReq:  &partitionData,
					partitionFetchResp: &partitionResponses[j],
					partitionTables:    f.bf.recentTables.getPartitionTables(topicInfo.ID, partitionID),
					topicID:            topicInfo.ID,
					partitionID:        partitionID,
					partitionHash:      partHash,
					lastReadOffset:     -1,
				}
			}
		}
	}
	return nil
}

// We read async on notifications to avoid blocking the transport thread that provides the notification and so we can
// parallelise sending multiple responses and fetching from distributed cache
func (f *FetchState) readAsync() {
	f.readExec.ch <- f
}

func (f *FetchState) read() error {
	ok, err := f.read0()
	if err != nil {
		return err
	}
	if ok {
		// We register our partition states to receive notifications when new data arrives
		// Needs to be done outside lock to prevent deadlock with incoming notification updating iterator
		return f.bf.recentTables.registerPartitionStates(f.partitionStates)
	}
	return nil
}

func (f *FetchState) read0() (bool, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.completionFunc == nil {
		// Response already sent
		return false, nil
	}
	wouldExceedRequestMax := false
outer:
	for topicID, partitionFetchStates := range f.partitionStates {
		for partitionID, partitionFetchState := range partitionFetchStates {
			var err error
			var wouldExceedPartitionMax bool
			wouldExceedRequestMax, wouldExceedPartitionMax, err = partitionFetchState.read()
			if err != nil {
				return false, err
			}
			if wouldExceedRequestMax {
				break outer
			}
			if wouldExceedPartitionMax {
				delete(partitionFetchStates, partitionID)
				if len(partitionFetchStates) == 0 {
					delete(f.partitionStates, topicID)
				}
			}
		}
	}
	if wouldExceedRequestMax || len(f.partitionStates) == 0 {
		// We either exceeded request max size or exceeded partition max size on all partitions, so the request is
		// complete
		if err := f.sendResponse(); err != nil {
			return false, err
		}
		return false, nil
	}
	if f.bytesFetched >= int(f.req.MinBytes) {
		// We fetched enough data
		if err := f.sendResponse(); err != nil {
			return false, err
		}
		return false, nil
	}
	// We didn't fetch enough data
	if f.req.MaxWaitMs == 0 {
		// Give up now and return no data
		f.clearFetchedRecords()
		if err := f.sendResponse(); err != nil {
			return false, err
		}
		return false, nil
	}
	if f.timeoutTimer == nil {
		// Set a timeout if we haven't already set one - as we need to wait
		time.AfterFunc(time.Duration(f.req.MaxWaitMs)*time.Millisecond, f.timeout)
	}
	return true, nil
}

func (f *FetchState) clearFetchedRecords() {
	// Clear any data that was fetched
	for i := 0; i < len(f.resp.Responses); i++ {
		for j := 0; j < len(f.resp.Responses[i].Partitions); j++ {
			f.resp.Responses[i].Partitions[j].Records = nil
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
	f.bf.recentTables.unregisterPartitionStates(f.partitionStates)
	return nil
}

func (f *FetchState) timeout() {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.bytesFetched < int(f.req.MinBytes) {
		f.clearFetchedRecords()
	}
	if err := f.sendResponse(); err != nil {
		log.Errorf("failed to send fetch response: %v", err)
	}
}

type PartitionFetchState struct {
	fs                 *FetchState
	partitionFetchReq  *kafkaprotocol.FetchRequestFetchPartition
	partitionFetchResp *kafkaprotocol.FetchResponsePartitionData
	partitionTables    *PartitionTables
	iter               iteration.Iterator
	topicID            int
	partitionID        int
	partitionHash      []byte
	bytesFetched       int
	lastReadOffset     int64
}

func (p *PartitionFetchState) read() (wouldExceedRequestMax bool, wouldExceedPartitionMax bool, err error) {
	if p.iter == nil {
		fetchOffset := p.partitionFetchReq.FetchOffset
		// See if we can create an iterator from locally cached tables - this would be the case where we are fetching
		// form a very recent offset, and avoids going to the controller to get the table ids
		tabIds, lastReadableOffset := p.partitionTables.getCacheTables(fetchOffset)
		if len(tabIds) > 0 {
			// Yes we have locally cached tables
			iter, err := p.createIteratorFromTabIDs(tabIds, p.partitionFetchReq.FetchOffset, lastReadableOffset)
			if err != nil {
				return false, false, err
			}
			p.iter = iter
		} else {
			// The fetchOffset is too old or there are no cached tables so we need to go to the controller
			// to get table ids
			if err := p.createIteratorFromControllerQuery(fetchOffset); err != nil {
				return false, false, err
			}
		}
	}
	var batches [][]byte
	for {
		ok, kv, err := p.iter.Next()
		if err != nil {
			return false, false, err
		}
		if !ok {
			break
		}
		batchSize := len(kv.Value)
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
		batches = append(batches, kv.Value)
		p.bytesFetched += batchSize
		p.fs.bytesFetched += batchSize
		p.lastReadOffset = kafkaencoding.BaseOffset(kv.Value) + int64(kafkaencoding.NumRecords(kv.Value)) - 1
	}
	if len(batches) > 0 {
		p.partitionFetchResp.Records = append(p.partitionFetchResp.Records, batches...)
		p.fs.first = false
	}
	return
}

func (p *PartitionFetchState) createIteratorFromControllerQuery(offset int64) error {
	cl, err := p.fs.bf.getClient()
	if err != nil {
		return err
	}
	ids, lastReadableOffset, err := cl.FetchTablesForPrefix(p.topicID, p.partitionID, p.partitionHash, offset)
	if err != nil {
		return err
	}
	keyStart := make([]byte, 0, 24)
	keyStart = append(keyStart, p.partitionHash...)
	keyStart = encoding.KeyEncodeInt(keyStart, offset)
	keyEnd := make([]byte, 0, 24)
	keyEnd = append(keyEnd, p.partitionHash...)
	keyEnd = encoding.KeyEncodeInt(keyEnd, lastReadableOffset+1)
	if err := p.createIteratorForKeyRange(ids, keyStart, keyEnd); err != nil {
		return err
	}
	return nil
}

func (p *PartitionFetchState) createIteratorForKeyRange(ids lsm.OverlappingTables, keyStart []byte, keyEnd []byte) error {
	if len(ids) == 0 {
		p.iter = &iteration.EmptyIterator{}
		return nil
	}
	tableGetter := p.fs.bf.getTableFromCache
	var iters []iteration.Iterator
	for _, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			info := nonOverLapIDs[0]
			iter, err := sst.NewLazySSTableIterator(info.ID, tableGetter, keyStart, keyEnd)
			if err != nil {
				return err
			}
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tableGetter, keyStart, keyEnd)
				if err != nil {
					return err
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
			return err
		}
	} else {
		iter = iters[0]
	}
	p.iter = iter
	return nil
}

func (p *PartitionFetchState) createIteratorFromTabIDs(tableIDs []*sst.SSTableID, fromOffset int64, lastReadableOffset int64) (iteration.Iterator, error) {
	keyStart := make([]byte, 0, 24)
	keyStart = append(keyStart, p.partitionHash...)
	keyStart = encoding.KeyEncodeInt(keyStart, fromOffset)
	keyEnd := make([]byte, 0, 24)
	keyEnd = append(keyEnd, p.partitionHash...)
	keyEnd = encoding.KeyEncodeInt(keyEnd, lastReadableOffset+1)
	var iter iteration.Iterator
	if len(tableIDs) > 0 {
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

func (p *PartitionFetchState) updateIterator(entries []RecentTableEntry) (*FetchState, error) {
	p.fs.lock.Lock()
	defer p.fs.lock.Unlock()
	if p.fs.completionFunc == nil {
		// response already sent - probably timed out
		return nil, nil
	}
	// Find tables of interest
	var tableIDs []*sst.SSTableID
	for _, entry := range entries {
		if entry.LastReadableOffset <= p.lastReadOffset {
			// Skip past this table as it cannot have any offsets we are interested in
			// Entries aren't added until all the offsets in a table are readable, so this is safe
			continue
		}
		tableIDs = append(tableIDs, entry.TableID)
	}
	if len(tableIDs) == 0 {
		// Nothing to update
		return nil, nil
	}
	lastReadableOffset := entries[len(entries)-1].LastReadableOffset
	// We know that we must have already iterated to previous last readable offset so we can throw away the previous
	// iterator and replace it with a simple chaining iterator if we have more than one table or just a single iterator
	var offsetStart int64
	if p.lastReadOffset == -1 {
		offsetStart = p.partitionFetchReq.FetchOffset
	} else {
		offsetStart = p.lastReadOffset
	}
	iter, err := p.createIteratorFromTabIDs(tableIDs, offsetStart, lastReadableOffset)
	if err != nil {
		return nil, err
	}
	p.iter = iter
	return p.fs, nil
}
