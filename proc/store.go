package proc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/objstore"
	sst2 "github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"golang.org/x/sync/semaphore"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ProcessorStore struct {
	s                      *Store
	lock                   sync.RWMutex
	processorID            int
	mt                     *mem.Memtable
	pushChan               chan struct{}
	queueLock              sync.RWMutex
	queue                  []*flushQueueEntry
	sem                    *semaphore.Weighted
	stopping               atomic.Bool
	started                bool
	stopWG                 sync.WaitGroup
	lastMTReplaceTime      uint64
	periodicReplaceInQueue atomic.Bool
	writeSeq               uint64
	firstGoodSequence      uint64
	iterators              sync.Map
}

func NewProcessorStore(s *Store, processorID int) *ProcessorStore {
	return &ProcessorStore{
		s:           s,
		processorID: processorID,
		pushChan:    make(chan struct{}),
		sem:         semaphore.NewWeighted(10), // TODO make configurable
	}
}

type flushQueueEntry struct {
	mt                   *mem.Memtable
	lastCompletedVersion int64
	periodic             bool
	seq                  uint64
	firstGoodSeq         uint64
}

func (ps *ProcessorStore) start() {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	ps.stopWG.Add(1)
	go ps.pushLoop()
	ps.started = true
	ps.createNewMemtable()
}

func (ps *ProcessorStore) isStarted() bool {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.started
}

func (ps *ProcessorStore) stop() {
	// Set stopping outside the lock - this unblocks the pushLoop if it is in a retry loop
	// it then continues consuming any remaining entries from the push queue and discards them
	// allowing any blocked push queue writer to unblock
	ps.stopping.Store(true)
	// Now we can get the lock
	ps.lock.Lock()
	defer ps.lock.Unlock()
	// Now we know there will be no more writers now
	ps.started = false
	// And we can close the push channel - this will cause the push Loop to exit
	close(ps.pushChan)
	// And wait for it to exit
	ps.stopWG.Wait()
}

func (ps *ProcessorStore) createNewMemtable() {
	arena := arenaskl.NewArena(uint32(ps.s.pm.cfg.MemtableMaxSizeBytes))
	ps.mt = mem.NewMemtable(arena, ps.s.pm.cfg.NodeID, int(ps.s.pm.cfg.MemtableMaxSizeBytes))
}

func (ps *ProcessorStore) Write(batch *mem.Batch) error {
	ps.lock.Lock() // Note, largely uncontended so minimal overhead
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	ok, err := ps.mt.Write(batch)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Memtable is full - add it to the push queue
	// This will block if queue is full, so provides throttling if writing faster than level manager can accept new
	// tables
	return ps.replaceMemtable(false)
}

func (ps *ProcessorStore) maybeFlushMemtable() error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	if ps.periodicReplaceInQueue.Load() {
		// We already have a periodic-replace queued, don't queue another one
		return nil
	}
	now := common.NanoTime()
	if ps.lastMTReplaceTime == 0 {
		ps.lastMTReplaceTime = now
		return nil
	}
	if now-ps.lastMTReplaceTime < uint64(ps.s.pm.cfg.MemtableMaxReplaceInterval) {
		return nil
	}
	if err := ps.replaceMemtable(true); err != nil {
		return err
	}
	ps.periodicReplaceInQueue.Store(true)
	ps.lastMTReplaceTime = now
	return nil
}

func (ps *ProcessorStore) flush(cb func(error)) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	ps.mt.AddFlushedCallback(cb)
	return ps.replaceMemtable(false)
}

func (ps *ProcessorStore) replaceMemtable(periodic bool) error {
	if err := ps.sem.Acquire(context.Background(), 1); err != nil {
		return err
	}
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	lcv := ps.s.getLastCompletedVersion()
	ps.queue = append(ps.queue, &flushQueueEntry{
		mt:                   ps.mt,
		lastCompletedVersion: lcv,
		seq:                  atomic.AddUint64(&ps.writeSeq, 1),
		periodic:             periodic,
	})
	ps.pushChan <- struct{}{}
	ps.createNewMemtable()
	return ps.updateIterators(ps.mt)
}

func (ps *ProcessorStore) clear() error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	// We set the first good sequence to be the next write sequence
	// Any entries with seq < first good sequence will be ignored when read by the push loop
	// so this means they are logically "cleared"
	atomic.StoreUint64(&ps.firstGoodSequence, atomic.LoadUint64(&ps.writeSeq)+1)
	return nil
}

func (ps *ProcessorStore) pushLoop() {
	defer ps.stopWG.Done()
	for range ps.pushChan {
		ps.queueLock.Lock()
		entry := ps.queue[0]
		ps.queueLock.Unlock()
		// We don't hold the lock while we are pushing as this can take time, and retry
		// Push the table if we're not clearing or stopping
		// Note that if we're stopping we will still consume entries, but we won't send them, this unblocks
		// writers as we release the semaphore
		if entry.seq >= atomic.LoadUint64(&ps.firstGoodSequence) && !ps.stopping.Load() {
			if err := ps.buildAndPushTable(entry); err != nil {
				log.Error(err)
				return
			}
		}
		ps.queueLock.Lock()
		// Remove the pushed entry, note: we copy, so we don't retain references to popped entries in buffer
		newEntries := make([]*flushQueueEntry, len(ps.queue)-1, cap(ps.queue))
		copy(newEntries, ps.queue[1:])
		ps.queueLock.Unlock()
		ps.sem.Release(1)
	}
}

func (ps *ProcessorStore) buildAndPushTable(entry *flushQueueEntry) error {
	// build sstable and push to cloud and register them with the level-manager
	if entry.mt != nil { // nil memtables are used for flush
		iter := entry.mt.NewIterator(nil, nil)
		ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst2.BuildSSTable(ps.s.pm.cfg.TableFormat,
			int(ps.s.pm.cfg.MemtableMaxSizeBytes), 8*1024, iter)
		if err != nil {
			return err
		}
		// Push and register the SSTable
		id := []byte(fmt.Sprintf("sst-%s", uuid.New().String()))
		tableBytes := ssTable.Serialize()
		for {
			if ps.stopping.Load() {
				return nil
			}
			if err := ps.s.cloudStoreClient.Put(id, tableBytes); err != nil {
				if common.IsUnavailableError(err) {
					// Transient availability error - retry
					log.Warnf("cloud store is unavailable, will retry: %v", err)
					time.Sleep(ps.s.pm.cfg.SSTablePushRetryDelay)
					continue
				}
				return err
			}
			log.Debugf("store %d added sstable with id %v for memtable %s to cloud store", ps.s.pm.cfg.NodeID, id,
				entry.mt.Uuid)
			break
		}
		if err := ps.s.tableCache.AddSSTable(id, ssTable); err != nil {
			return err
		}
		log.Debugf("store %d added sstable with id %v for memtable %s to table cache", ps.s.pm.cfg.NodeID, id,
			entry.mt.Uuid)
		for {
			if ps.stopping.Load() {
				return nil
			}
			clusterVersion := ps.s.getClusterVersion()
			// register with level-manager
			if err := ps.s.levelManagerClient.RegisterL0Tables(levels.RegistrationBatch{
				ClusterName:    ps.s.pm.cfg.ClusterName,
				ClusterVersion: clusterVersion,
				Registrations: []levels.RegistrationEntry{{
					Level:            0,
					TableID:          id,
					MinVersion:       minVersion,
					MaxVersion:       maxVersion,
					KeyStart:         smallestKey,
					KeyEnd:           largestKey,
					DeleteRatio:      ssTable.DeleteRatio(),
					AddedTime:        ssTable.CreationTime(),
					NumEntries:       uint64(ssTable.NumEntries()),
					TableSize:        uint64(ssTable.SizeBytes()),
					NumPrefixDeletes: uint32(ssTable.NumPrefixDeletes()),
				}},
				DeRegistrations: nil,
			}); err != nil {
				var tektiteErr errors.TektiteError
				if errors.As(err, &tektiteErr) {
					if tektiteErr.Code == errors.Unavailable || tektiteErr.Code == errors.LevelManagerNotLeaderNode {
						if ps.stopping.Load() {
							// Allow to break out of the loop if stopped or clearing
							return nil
						}
						// Transient availability error - retry
						log.Warnf("store failed to register new ss-table with level manager, will retry: %v", err)
						time.Sleep(ps.s.pm.cfg.SSTableRegisterRetryDelay)
						continue
					}
				}
				return err
			}
			log.Debugf("node %d registered memtable %s with levelManager sstableid %v- max version %d",
				ps.s.pm.cfg.NodeID, entry.mt.Uuid, id, maxVersion)
			break
		}
	} else {
		log.Debugf("node %d entry to flush has no writes", ps.s.pm.cfg.NodeID)
	}
	ps.s.versionFlushed(ps.processorID, entry.lastCompletedVersion)
	if entry.periodic {
		ps.periodicReplaceInQueue.Store(false)
	}
	log.Debugf("store %d calling flushed callback for memtable %s", ps.s.pm.cfg.NodeID, entry.mt.Uuid)
	return entry.mt.Flushed(nil)
}

// Get the value for the key, or nil if it doesn't exist.
// Note the key argument must not contain the version.
// Returns the highest version for the key
func (ps *ProcessorStore) Get(key []byte) ([]byte, error) {
	return ps.getWithMaxVersion(key, math.MaxUint64)
}

func (ps *ProcessorStore) getWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	ps.lock.RLock() // Note, largely uncontended so minimal overhead
	unlocked := false
	defer func() {
		if !unlocked {
			ps.lock.RUnlock()
		}
	}()
	if !ps.started {
		return nil, errors.New("processor store not started")
	}

	keyEnd := common.IncrementBytesBigEndian(key)

	log.Debugf("seq:%d node: %d store GetWithMaxVersion key start:%v key end:%v maxVersion:%d", ps.s.pm.cfg.NodeID,
		key, keyEnd, maxVersion)

	// First look in live memTable
	val, err := findInMemtable(ps.mt, key, keyEnd, maxVersion)
	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	val, err = ps.findInQueuedEntries(key, keyEnd, maxVersion)
	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	// Now we can unlock and look in the SSTables - we don't want to hold the lock while we're doing the remote call
	ps.lock.RUnlock()
	unlocked = true

	iters, err := ps.createSSTableIterators(key, keyEnd)
	if err != nil {
		return nil, err
	}
	iter, err := iteration.NewMergingIterator(iters, false, maxVersion)
	if err != nil {
		return nil, err
	}
	log.Debugf("created merging iter %p", iter)
	val, err = getWithIteratorNoVersionCheck(iter, key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (ps *ProcessorStore) findInQueuedEntries(keyStart []byte, keyEnd []byte, maxVersion uint64) ([]byte, error) {
	// We look in the memTables in the flush queue from newest to oldest
	ps.queueLock.RLock()
	defer ps.queueLock.RUnlock()
	for i := len(ps.queue) - 1; i >= 0; i-- {
		entry := ps.queue[i]
		val, err := findInMemtable(entry.mt, keyStart, keyEnd, maxVersion)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

func findInMemtable(mt *mem.Memtable, key []byte, keyEnd []byte, maxVersion uint64) ([]byte, error) {
	return getWithIterator(mt.NewIterator(key, keyEnd), key, maxVersion)
}

func getWithIterator(iter iteration.Iterator, key []byte, maxVersion uint64) ([]byte, error) {
	for {
		valid, err := iter.IsValid()
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, nil
		}
		kv := iter.Current()
		ver := math.MaxUint64 - binary.BigEndian.Uint64(kv.Key[len(kv.Key)-8:])
		// We skip past any versions which are too high. Note that iterator will always return highest versions first.
		if ver <= maxVersion {
			if bytes.Equal(key, kv.Key[:len(kv.Key)-8]) {
				return kv.Value, nil
			}
			return nil, nil
		}
		err = iter.Next()
		if err != nil {
			return nil, err
		}
	}
}

func getWithIteratorNoVersionCheck(iter iteration.Iterator, key []byte) ([]byte, error) {
	valid, err := iter.IsValid()
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, nil
	}
	kv := iter.Current()
	if bytes.Equal(key, kv.Key[:len(kv.Key)-8]) {
		return kv.Value, nil
	}
	return nil, nil
}

func (ps *ProcessorStore) createSSTableIterators(keyStart []byte, keyEnd []byte) ([]iteration.Iterator, error) {
	ids, deadVersions, err := ps.s.levelManagerClient.GetTableIDsForRange(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	log.Debugf("creating sstable iters for keystart %v keyend %v", keyStart, keyEnd)
	// Then we add each flushed SSTable with overlapping keys from the levelManagerClient. It's possible we might have the included
	// the same keys twice in a memtable from the flush queue which has been already flushed and one from the LSM
	// This is ok as he later one (the sstable) will just be ignored in the iterator.
	var iters []iteration.Iterator
	for i, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			log.Debugf("using sstable %v in iterator [%d, 0] for key start %v", nonOverLapIDs[0], i, keyStart)
			lazy, err := sst2.NewLazySSTableIterator(nonOverLapIDs[0], ps.s.tableCache, keyStart, keyEnd, ps.iterFactoryFunc())
			if err != nil {
				return nil, err
			}
			iters = append(iters, lazy)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				log.Debugf("using sstable %v in iterator [%d, %d] for key start %v", nonOverlapID, i, j, keyStart)
				lazy, err := sst2.NewLazySSTableIterator(nonOverlapID, ps.s.tableCache, keyStart, keyEnd, ps.iterFactoryFunc())
				if err != nil {
					return nil, err
				}
				itersInChain[j] = lazy
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	if len(deadVersions) > 0 {
		log.Debugf("dead versions are: %v", deadVersions)
		// We have dead versions that we need to remove on this side. This occurs after failure when we rollback to
		// the last good snapshot and we need to filter out any versions between that and when recovery completed.
		for i, iter := range iters {
			iters[i] = levels.NewRemoveDeadVersionsIterator(iter, deadVersions)
		}
	}
	return iters, nil
}

func (ps *ProcessorStore) iterFactoryFunc() func(sst *sst2.SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	return func(sst *sst2.SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
		sstIter, err := sst.NewIterator(keyStart, keyEnd)
		if err != nil {
			return nil, err
		}
		return sstIter, nil
	}
}

type Iterator struct {
	s          *ProcessorStore
	lock       *sync.RWMutex
	rangeStart []byte
	rangeEnd   []byte
	lastKey    []byte
	iter       *iteration.MergingIterator
}

func (ps *ProcessorStore) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	log.Debugf("creating store iterator from keystart %v to keyend %v", keyStart, keyEnd)

	ps.lock.RLock() // Note, largely uncontended so minimal overhead
	unlocked := false
	defer func() {
		if !unlocked {
			ps.lock.RUnlock()
		}
	}()
	if !ps.started {
		return nil, errors.New("processor store not started")
	}

	// First we add the current memtable
	iters := []iteration.Iterator{ps.mt.NewIterator(keyStart, keyEnd)}

	ps.queueLock.RLock()
	for i := len(ps.queue) - 1; i >= 0; i-- {
		entry := ps.queue[i]
		iters = append(iters, entry.mt.NewIterator(keyStart, keyEnd))
	}

	// We unlock before creating sstable iterator
	ps.queueLock.RUnlock()
	ps.lock.RUnlock()
	unlocked = true
	ssTableIters, err := ps.createSSTableIterators(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	iters = append(iters, ssTableIters...)
	si, err := ps.newStoreIterator(keyStart, keyEnd, iters, &ps.lock, highestVersion, preserveTombstones)
	if err != nil {
		return nil, err
	}
	ps.iterators.Store(si, nil)
	return si, nil
}

func (ps *ProcessorStore) newStoreIterator(rangeStart []byte, rangeEnd []byte, iters []iteration.Iterator, lock *sync.RWMutex,
	highestVersion uint64, preserveTombstones bool) (*Iterator, error) {
	mi, err := iteration.NewMergingIterator(iters, preserveTombstones, highestVersion)
	if err != nil {
		return nil, err
	}
	si := &Iterator{
		s:          ps,
		lock:       lock,
		rangeStart: rangeStart,
		rangeEnd:   rangeEnd,
		iter:       mi,
	}
	return si, nil
}

func (ps *ProcessorStore) removeIterator(iter *Iterator) {
	ps.iterators.Delete(iter)
}

func (ps *ProcessorStore) updateIterators(mt *mem.Memtable) error {
	var err error
	ps.iterators.Range(func(key, value any) bool {
		iter := key.(*Iterator) //nolint:forcetypeassert
		rs, re, lastKey := iter.getRange()
		if lastKey != nil {
			// lastKey includes the version
			lk := lastKey[:len(lastKey)-8]
			rs = common.IncrementBytesBigEndian(lk)
			rs = append(rs, lastKey[len(lastKey)-8:]...) // put version back on
		}
		mtIter := mt.NewIterator(rs, re)
		if err = iter.addNewMemtableIterator(mtIter); err != nil {
			return false
		}
		return true
	})
	return err
}

func (it *Iterator) getRange() ([]byte, []byte, []byte) {
	return it.rangeStart, it.rangeEnd, it.lastKey
}

func (it *Iterator) addNewMemtableIterator(iter iteration.Iterator) error {
	return it.iter.PrependIterator(iter)
}

func (it *Iterator) Close() {
	it.s.removeIterator(it)
}

func (it *Iterator) Current() common.KV {
	it.lock.RLock()
	defer it.lock.RUnlock()
	curr := it.iter.Current()
	it.lastKey = curr.Key
	return curr
}

func (it *Iterator) Next() error {
	it.lock.RLock()
	defer it.lock.RUnlock()
	return it.iter.Next()
}

func (it *Iterator) IsValid() (bool, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()
	return it.iter.IsValid()
}

type Store struct {
	pm                     *ProcessorManager
	cloudStoreClient       objstore.Client
	levelManagerClient     levels.Client
	tableCache             *tabcache.Cache
	iterators              sync.Map
	mtReplaceTimerLock     sync.Mutex
	mtReplaceTimer         *common.TimerHandle
	clusterVersion         uint64
	versionFlushedHandler  func(version int)
	shutdownFlushImmediate atomic.Bool
}

func newStore(pm *ProcessorManager, cloudStoreClient objstore.Client, levelManagerClient levels.Client,
	tableCache *tabcache.Cache) *Store {
	return &Store{
		pm:                 pm,
		cloudStoreClient:   cloudStoreClient,
		levelManagerClient: levelManagerClient,
		tableCache:         tableCache,
	}
}

func (s *Store) start() {
	s.scheduleMtReplace()
}

func (s *Store) stop() {
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	if s.mtReplaceTimer != nil {
		s.mtReplaceTimer.Stop()
		s.mtReplaceTimer = nil
	}
}

func (s *Store) Flush(shutdown bool) error {
	var procStores []*ProcessorStore
	s.pm.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if proc.IsLeader() {
			procStores = append(procStores, proc.store)
		}
		return true
	})

	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(len(procStores), func(err error) {
		ch <- err
	})
	for _, ps := range procStores {
		if err := ps.flush(cf.CountDown); err != nil {
			return err
		}
	}
	if shutdown {
		s.shutdownFlushImmediate.Store(true)
	}
	return <-ch
}

func (s *Store) scheduleMtReplace() {
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	s.mtReplaceTimer = common.ScheduleTimer(s.pm.cfg.MemtableMaxReplaceInterval, false, func() {
		if err := s.maybeReplaceMemtables(); err != nil {
			log.Errorf("failed to replace memtable %+v", err)
		}
		s.scheduleMtReplace()
	})
}

func (s *Store) maybeReplaceMemtables() error {
	var err error
	s.pm.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if proc.IsLeader() {
			if err = proc.store.maybeFlushMemtable(); err != nil {
				return false
			}
		}
		return true
	})
	return err
}

func (s *Store) versionFlushed(processorID int, flushedVersion int64) {
	// TODO
}

func (s *Store) getClusterVersion() int {
	return s.pm.ClusterVersion()
}

func (s *Store) lastCompletedUpdated() {
	if s.shutdownFlushImmediate.Load() {
		// After flush at shutdown we respond to each completed version by flushing each processor store,
		// this enables versions to be flushed from each processor, without waiting for memtable timeout
		s.pm.processors.Range(func(key, value any) bool {
			proc := value.(*processor)
			if proc.IsLeader() {
				if err := proc.store.flush(func(err error) {}); err != nil {
					log.Errorf("failed to flush %v", err)
				}
			}
			return true
		})
	}
}

func (s *Store) getLastCompletedVersion() int64 {
	return int64(s.pm.getLastCompletedVersion())
}
