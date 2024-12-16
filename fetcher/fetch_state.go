package fetcher

import (
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
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
	readExec        *readExecutor
	bytesFetched    int
	first           bool
}

func newFetchState(authContext *auth.Context, batchFetcher *BatchFetcher, req *kafkaprotocol.FetchRequest, readExec *readExecutor,
	completionFunc func(response *kafkaprotocol.FetchResponse) error) (*FetchState, error) {
	fetchState := &FetchState{
		bf:              batchFetcher,
		req:             req,
		partitionStates: map[int]map[int]*PartitionFetchState{},
		completionFunc:  completionFunc,
		readExec:        readExec,
		first:           true,
	}
	fetchState.resp.Responses = make([]kafkaprotocol.FetchResponseFetchableTopicResponse, len(fetchState.req.Topics))
	recentTables := &fetchState.bf.recentTables
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
		partitionMap := recentTables.getPartitionMap(topicInfo.ID)
		for j, partitionData := range topicData.Partitions {
			log.Debugf("fetch for topic %d partition %d", topicInfo.ID, partitionData.Partition)
			partitionResponses[j].PartitionIndex = partitionData.Partition
			partitionResponses[j].Records = [][]byte{} // client does not like nil records
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
					partitionTables:    recentTables.getPartitionTables(partitionMap, partitionID),
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

// We read async on notifications to avoid blocking the transport thread that provides the notification and so we can
// parallelise sending multiple responses and fetching from distributed cache
func (f *FetchState) readAsync() {
	f.readExec.execFetchState(f)
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
			f.resp.Responses[i].Partitions[j].Records = [][]byte{}
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
	// unregister any waiting partition states
	for _, partitionMap := range f.partitionStates {
		for _, partitionState := range partitionMap {
			partitionState.partitionTables.removeListener(partitionState)
		}
	}
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
	topicID            int
	partitionID        int
	bytesFetched       int
	fetchOffset        int64
	partitionHash      []byte
	listening          bool
}

func (p *PartitionFetchState) read() (wouldExceedRequestMax bool, wouldExceedPartitionMax bool, err error) {
	memberID := atomic.LoadInt32(&p.fs.bf.memberID)
	if memberID == -1 {
		return false, false,
			common.NewTektiteErrorf(common.Unavailable, "fetch before fetcher has received cluster state")
	}
	var iter iteration.Iterator
	for {
		if !p.listening {
			p.partitionTables.addListener(p)
			p.listening = true
		}
		tabIds, lastReadableOffset, initialised, isInCachedRange := p.partitionTables.maybeGetRecentTableIDs(p.fetchOffset)
		if !initialised {
			// initialise it - call initialise passing in function for Fetch to prevent race, as executed under
			// partition tables lock
			cl, err := p.fs.bf.getClient()
			var alreadyInitialised bool
			lastReadableOffset, alreadyInitialised, err = p.partitionTables.initialise(func() (int64, error) {
				log.Debugf("registering table listener for topic %d partition %d", p.topicID, p.partitionID)
				return cl.RegisterTableListener(p.topicID, p.partitionID, p.fs.bf.memberID, atomic.LoadInt64(&p.fs.bf.resetSequence))
			})
			if err != nil {
				return false, false, err
			}
			if alreadyInitialised {
				// There is a race to initialise it and another request got there first, we try again
				continue
			}
		}
		p.partitionFetchResp.HighWatermark = 1 + lastReadableOffset
		if isInCachedRange {
			iter, err = p.createIteratorFromTabIDs(tabIds, p.fetchOffset, lastReadableOffset)
			if err != nil {
				return false, false, err
			}
		} else {
			// Query fetch is before start of cached data or newly initialised
			cl, err := p.fs.bf.getClient()
			if err != nil {
				return false, false, err
			}
			keyStart, keyEnd := p.createKeyStartAndEnd(p.fetchOffset, lastReadableOffset)
			iter, err = queryutils.CreateIteratorForKeyRange(keyStart, keyEnd, cl, p.fs.bf.tableGetter)
			if err != nil {
				return false, false, err
			}
		}
		break
	}
	var batches [][]byte
	for {
		ok, kv, err := iter.Next()
		if err != nil {
			return false, false, err
		}
		if !ok {
			break
		}
		value := common.RemoveValueMetadata(kv.Value)
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
		batches = append(batches, value)
		p.fs.first = false
		p.bytesFetched += batchSize
		p.fs.bytesFetched += batchSize
		p.fetchOffset += int64(kafkaencoding.NumRecords(value))
	}
	if len(batches) > 0 {
		p.partitionFetchResp.Records = append(p.partitionFetchResp.Records, batches...)
	}
	return
}

func (p *PartitionFetchState) createKeyStartAndEnd(fetchOffset int64, lro int64) ([]byte, []byte) {
	keyStart := make([]byte, 0, 25)
	keyStart = append(keyStart, p.partitionHash...)
	keyStart = append(keyStart, common.EntryTypeTopicData)
	keyStart = encoding.KeyEncodeInt(keyStart, fetchOffset)
	keyEnd := make([]byte, 0, 25)
	keyEnd = append(keyEnd, p.partitionHash...)
	keyEnd = append(keyEnd, common.EntryTypeTopicData)
	keyEnd = encoding.KeyEncodeInt(keyEnd, lro+1)
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
