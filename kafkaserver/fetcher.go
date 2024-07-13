package kafkaserver

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/kafkaencoding"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"hash"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type fetcher struct {
	lock              sync.RWMutex
	procProvider      processorProvider
	topicFetchers     map[string][]*PartitionFetcher
	allocator         Allocator
	evictBatchesTimer *time.Timer
	stopped           bool
}

func newFetcher(provider processorProvider, streamMgr streamMgr, maxFetchCacheSize int) *fetcher {
	f := &fetcher{
		procProvider:  provider,
		topicFetchers: map[string][]*PartitionFetcher{},
		allocator:     newDefaultAllocator(maxFetchCacheSize),
	}
	streamMgr.RegisterChangeListener(f.streamChanged)
	return f
}

func (f *fetcher) streamChanged(streamName string, _ bool) {
	f.lock.Lock()
	defer f.lock.Unlock()
	// remove the cached state
	delete(f.topicFetchers, streamName)
}

func (f *fetcher) GetPartitionFetcher(topicInfo *TopicInfo, partitionID int32) (*PartitionFetcher, error) {
	lock := lockWrapper{mut: &f.lock}
	lock.RLock()
	defer lock.Unlock()
	var allocator Allocator
	if topicInfo.CanCache {
		allocator = f.allocator
	} else {
		allocator = &directAllocator{}
	}
	fetchers, ok := f.topicFetchers[topicInfo.Name]
	if !ok {
		lock.Lock()
		fetchers, ok = f.topicFetchers[topicInfo.Name]
		if !ok {
			fetchers = make([]*PartitionFetcher, len(topicInfo.Partitions))
			for i := 0; i < len(topicInfo.Partitions); i++ {
				processorID, ok := topicInfo.ConsumerInfoProvider.PartitionScheme().PartitionProcessorMapping[i]
				if !ok {
					panic("no processor for partition")
				}
				processor := f.procProvider.GetProcessor(processorID)
				if processor == nil {
					return nil, errors.NewTektiteErrorf(errors.Unavailable, "processor not available")
				}
				fetchers[i] = NewPartitionFetcher(processor, i, allocator, topicInfo)
			}
			f.topicFetchers[topicInfo.Name] = fetchers
		}
	}
	return fetchers[partitionID], nil
}

func (f *fetcher) checkEvictBatches() {
	for _, topicFetchers := range f.topicFetchers {
		for _, partitionFetcher := range topicFetchers {
			partitionFetcher.CheckEvictBatches()
		}
	}
}

func (f *fetcher) start() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.scheduleCheckEvictBatches()
}

const evictBatchesCheckInterval = 5 * time.Second

func (f *fetcher) scheduleCheckEvictBatches() {
	f.evictBatchesTimer = time.AfterFunc(evictBatchesCheckInterval, func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		if f.stopped {
			return
		}
		f.checkEvictBatches()
		f.scheduleCheckEvictBatches()
	})
}

func (f *fetcher) stop() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.stopped = true
	if f.evictBatchesTimer != nil {
		f.evictBatchesTimer.Stop()
	}
}

type PartitionFetcher struct {
	lock              sync.Mutex
	firstCachedOffset int64
	lastCachedOffset  int64
	highWaterMark     int64
	loadedHwm         bool
	batches           []entry
	waiter            *Waiter
	partitionID       uint64
	topicInfo         *TopicInfo
	crc32             hash.Hash32
	allocator         Allocator
	evictCount        int64
	partitionHash     []byte
	processor         proc.Processor
}

type Waiter struct {
	pf            *PartitionFetcher
	complFunc     func([][]byte, int64, error)
	bytesRequired int
	timer         *common.TimerHandle
	records       [][]byte
	maxWait       time.Duration
}

func (w *Waiter) schedule() {
	w.pf.scheduleWaiter(w)
}

func (w *Waiter) complete() {
	w.pf.completeWaiter(w)
}

func NewPartitionFetcher(processor proc.Processor, partitionID int, allocator Allocator, topicInfo *TopicInfo) *PartitionFetcher {
	partitionHash := proc.CalcPartitionHash(topicInfo.ConsumerInfoProvider.PartitionScheme().MappingID, uint64(partitionID))
	return &PartitionFetcher{
		firstCachedOffset: -1,
		lastCachedOffset:  -1,
		highWaterMark:     -1,
		crc32:             crc32.NewIEEE(),
		partitionID:       uint64(partitionID),
		allocator:         allocator,
		topicInfo:         topicInfo,
		partitionHash:     partitionHash,
		processor:         processor,
	}
}

type entry struct {
	startOffset int64
	endOffset   int64
	recordBatch []byte
}

func (f *PartitionFetcher) evictEntry() {
	if !f.topicInfo.CanCache {
		panic("evictEntry called for no cache fetcher")
	}
	// In order to avoid contention on the mutex, we don't truncate the batches here, instead we increment a count
	// and, they're truncated the next time AddBatch is called
	atomic.AddInt64(&f.evictCount, 1)
}

func (f *PartitionFetcher) Allocate(size int) ([]byte, error) {
	return f.allocator.Allocate(size, f.evictEntry)
}

// CheckEvictBatches is called periodically on a timer as we usually truncate old batches in the AddBatch function
// but if this is not called then batches would not get evicted.
func (f *PartitionFetcher) CheckEvictBatches() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.checkEvictBatches()
}

func (f *PartitionFetcher) checkEvictBatches() {
	toEvict := atomic.LoadInt64(&f.evictCount)
	if toEvict > 0 {
		lfb := int64(len(f.batches))
		var evictNum int64
		if toEvict > lfb {
			// Potentially, batches can be allocated but an error occurs before the batch is added to the cache, this
			// could result in an evictCount higher than number of batches, so we adjust accordingly
			evictNum = lfb
		} else {
			evictNum = toEvict
		}
		f.batches = f.batches[evictNum:]
		if len(f.batches) > 0 {
			f.firstCachedOffset = f.batches[0].startOffset
		} else {
			f.firstCachedOffset = -1
			f.lastCachedOffset = -1
		}
		atomic.AddInt64(&f.evictCount, -toEvict)
	}
}

func (f *PartitionFetcher) AddBatch(startOffset int64, endOffset int64, batch []byte) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if !f.topicInfo.CanCache {
		panic("AddBatch called for no cache fetcher")
	}
	if f.lastCachedOffset != -1 && startOffset <= f.lastCachedOffset {
		panic(fmt.Sprintf("batch added with overlapping offset, startOffset:%d lastcached:%d", startOffset, f.lastCachedOffset))
	}
	f.checkEvictBatches()
	binary.BigEndian.PutUint64(batch, uint64(startOffset)) // fill in baseOffset
	f.batches = append(f.batches, entry{
		startOffset: startOffset,
		endOffset:   endOffset,
		recordBatch: batch,
	})
	f.lastCachedOffset = endOffset
	f.highWaterMark = endOffset
	f.loadedHwm = true
	if f.firstCachedOffset == -1 {
		f.firstCachedOffset = startOffset
	}
	if f.waiter != nil {
		f.waiter.records = append(f.waiter.records, batch)
		lb := len(batch)
		if lb >= f.waiter.bytesRequired {
			f.waiter.timer.Stop()
			f.waiter.complFunc(f.waiter.records, f.lastCachedOffset, nil)
			f.waiter = nil
		} else {
			f.waiter.bytesRequired -= lb
		}
	}
}

// getHighWaterMark returns the offset of the last committed and available message in the partition.
func (f *PartitionFetcher) getHighWaterMark() (int64, error) {
	if f.loadedHwm {
		return f.highWaterMark, nil
	}
	latest, _, ok, err := f.topicInfo.ConsumerInfoProvider.LatestOffset(int(f.partitionID))
	if !ok {
		panic("cannot get last offset")
	}
	if err != nil {
		return 0, err
	}
	f.highWaterMark = latest - 1
	f.loadedHwm = true
	return latest, nil
}

func (f *PartitionFetcher) Fetch(fetchOffset int64, minBytes int, maxBytes int, maxWait time.Duration,
	complFunc func([][]byte, int64, error)) *Waiter {
	log.Debugf("topic:%s partition:%d fetching with offset:%d", f.topicInfo.Name, f.partitionID, fetchOffset)
	f.lock.Lock()
	defer f.lock.Unlock()
	f.checkEvictBatches()
	hwm, err := f.getHighWaterMark()
	if err != nil {
		complFunc(nil, 0, err)
	}
	if f.firstCachedOffset == -1 || fetchOffset < f.firstCachedOffset {
		// Fetch from offset smaller than anything we have cached, or nothing in cache - maybe we've just started
		// so look in store
		records, err := f.fetchFromStore(fetchOffset, maxBytes)
		log.Debugf("topic:%s partition:%d fetched %d bytes from store", f.topicInfo.Name, f.partitionID, len(records))

		if err != nil {
			complFunc(nil, 0, err)
			return nil
		}
		// We ignore minBytes here
		if records != nil {
			complFunc([][]byte{records}, hwm, nil)
		} else {
			complFunc(nil, hwm, nil)
		}
		return nil
	}
	// Fetch from cache
	var size int
	var records [][]byte
	if fetchOffset <= f.lastCachedOffset {
		records, size = f.fetchFromCache(fetchOffset, maxBytes)
		if records != nil && size >= minBytes {
			complFunc(records, hwm, nil)
			return nil
		}
	}

	// If we get here then either 1) there are no new records in log to return or < minBytes
	if maxWait == 0 {
		complFunc(nil, hwm, nil)
		return nil
	}
	// We will wait.
	if f.waiter != nil {
		// Already have a waiter - send an error on it
		f.waiter.complFunc(nil, 0, KafkaProtocolError{ErrorCode: ErrorCodeUnknownServerError})
		f.waiter = nil
	}
	return &Waiter{
		pf:            f,
		maxWait:       maxWait,
		complFunc:     complFunc,
		bytesRequired: minBytes - size,
		records:       records,
	}
}

func (f *PartitionFetcher) scheduleWaiter(w *Waiter) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.waiter = w
	f.waiter.timer = common.ScheduleTimer(w.maxWait, false, func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		if f.waiter == nil {
			return
		}
		f.waiter.complFunc(f.waiter.records, f.lastCachedOffset, nil)
		f.waiter = nil
	})
}

func (f *PartitionFetcher) completeWaiter(w *Waiter) {
	f.lock.Lock()
	defer f.lock.Unlock()
	w.complFunc(w.records, f.lastCachedOffset, nil)
}

type KafkaProtocolError struct {
	ErrorCode int
	Message   string
}

func (kpe KafkaProtocolError) Error() string {
	return fmt.Sprintf("KafkaProtocolError ErrorCode:%d %s", kpe.ErrorCode, kpe.Message)
}

func (f *PartitionFetcher) fetchFromStore(fetchOffset int64, maxBytes int) ([]byte, error) {
	slabId := uint64(f.topicInfo.ConsumerInfoProvider.SlabID())
	iterStart := encoding.EncodeEntryPrefix(f.partitionHash, slabId, 33)
	iterStart = append(iterStart, 1) // not null
	iterStart = encoding.KeyEncodeInt(iterStart, fetchOffset)
	iterEnd := encoding.EncodeEntryPrefix(f.partitionHash, slabId+1, 24)
	iter, err := f.processor.NewIterator(iterStart, iterEnd, math.MaxUint64, false)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	batchBytes := make([]byte, 61)
	first := true
	var firstOffset, lastOffset int64
	var firstTimestamp, lastTimestamp types.Timestamp
	var numRecords int
	for {
		ok, kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		offset, _ := encoding.KeyDecodeInt(kv.Key, 25)
		if firstOffset == -1 {
			firstOffset = offset
		}
		lastOffset = offset

		off := 1

		var ts types.Timestamp
		var u uint64
		u, off = encoding.ReadUint64FromBufferLE(kv.Value, off)
		ts = types.NewTimestamp(int64(u))

		isNull := kv.Value[off] == 0
		off++
		var key []byte
		if !isNull {
			key, off = encoding.ReadBytesFromBufferLE(kv.Value, off)
		}

		if first {
			firstOffset = offset
			firstTimestamp = ts
		}
		lastTimestamp = ts

		isNull = kv.Value[off] == 0
		off++
		var hdrs []byte
		if !isNull {
			hdrs, off = encoding.ReadBytesFromBufferLE(kv.Value, off)
		}

		isNull = kv.Value[off] == 0
		off++
		var val []byte
		if !isNull {
			val, off = encoding.ReadBytesFromBufferLE(kv.Value, off)
		}

		batchBytes, ok = kafkaencoding.AppendToBatch(batchBytes, offset, key, hdrs, val, ts, firstTimestamp, firstOffset, maxBytes, first)
		if !ok {
			// would exceed maxBytes
			break
		}
		numRecords++
		first = false
	}

	if first {
		// No rows read
		return nil, nil
	}
	kafkaencoding.SetBatchHeader(batchBytes, firstOffset, lastOffset, firstTimestamp, lastTimestamp, numRecords, f.crc32)

	log.Debugf("topic:%s partition:%d loaded batch from store firstoffset:%d lastoffset:%d",
		f.topicInfo.Name, f.partitionID, firstOffset, lastOffset)

	return batchBytes, nil
}

func (f *PartitionFetcher) fetchFromCache(fetchOffset int64, maxBytes int) ([][]byte, int) {
	var batches [][]byte
	var start int
	for i := len(f.batches) - 1; i >= 0; i-- {
		if f.batches[i].endOffset < fetchOffset {
			break
		}
		start = i
	}

	size := 0
	for j := start; j < len(f.batches); j++ {
		entry := f.batches[j]
		batch := entry.recordBatch
		lb := len(batch)
		wouldExceedMaxBytes := size+lb > maxBytes
		if size == 0 || !wouldExceedMaxBytes {
			batches = append(batches, batch)
			size += lb
		}
		log.Debugf("topic:%s partition:%d loading batch startoffset:%d endoffset:%d",
			f.topicInfo.Name, f.partitionID, entry.startOffset, entry.endOffset)
		if wouldExceedMaxBytes {
			break
		}
	}
	return batches, size
}

func (f *PartitionFetcher) cachedBatches() []entry {
	f.lock.Lock()
	defer f.lock.Unlock()
	fcp := make([]entry, len(f.batches))
	copy(fcp, f.batches)
	return fcp
}

type lockWrapper struct {
	mut          *sync.RWMutex
	hasReadLock  bool
	hasWriteLock bool
}

func (u *lockWrapper) Lock() {
	if u.hasWriteLock {
		panic("already has write lock")
	}
	if u.hasReadLock {
		u.mut.RUnlock()
		u.hasReadLock = false
	}
	u.mut.Lock()
	u.hasWriteLock = true
}

func (u *lockWrapper) RLock() {
	if u.hasWriteLock {
		panic("already has write lock")
	}
	if u.hasReadLock {
		panic("already has write lock")
	}
	u.mut.RLock()
	u.hasReadLock = true
}

func (u *lockWrapper) Unlock() {
	if u.hasReadLock {
		u.mut.RUnlock()
		u.hasReadLock = false
	} else if u.hasWriteLock {
		u.mut.Unlock()
		u.hasWriteLock = false
	}
}
