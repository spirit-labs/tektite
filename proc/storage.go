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
	sst2 "github.com/spirit-labs/tektite/sst"
	"golang.org/x/sync/semaphore"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Store interface {
	Start()
	Stop()
	Get(key []byte) ([]byte, error)
	Write(batch *mem.Batch) error
	GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error)
	NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error)
}

type ProcessorStore struct {
	pm                     *ProcessorManager
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
}

func NewProcessorStore(pm *ProcessorManager, processorID int) *ProcessorStore {
	// Note that we limit max queue size with a semaphore, not on pushChan size. This is because we remove
	// from pushChan before push the sstable and remove the entry from the queue.
	maxQueueSize := pm.cfg.MemtableFlushQueueMaxSize
	return &ProcessorStore{
		pm:          pm,
		processorID: processorID,
		pushChan:    make(chan struct{}, maxQueueSize),
		sem:         semaphore.NewWeighted(int64(maxQueueSize)),
	}
}

type flushQueueEntry struct {
	mt                   *mem.Memtable
	lastCompletedVersion int64
	periodic             bool
	seq                  uint64
	firstGoodSeq         uint64
	cb                   func(error)
}

func (ps *ProcessorStore) Start() {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	ps.stopWG.Add(1)
	go ps.pushLoop()
	ps.started = true
}

func (ps *ProcessorStore) isStarted() bool {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return ps.started
}

func (ps *ProcessorStore) Stop() {
	// Set stopping outside the lock - this unblocks the pushLoop if it is in a retry loop
	// it then continues consuming any remaining entries from the push queue and discards them
	// allowing any blocked push queue writer to unblock
	ps.stopping.Store(true)
	// Now we can get the lock
	ps.lock.Lock()
	// Now we know there will be no more writers now
	ps.started = false
	ps.lock.Unlock()
	// And we can close the push channel - this will cause the push Loop to exit - must be done outside lock
	close(ps.pushChan)
	// And wait for it to exit
	ps.stopWG.Wait()
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if ps.mt != nil {
		ps.mt.Close()
		ps.mt = nil
	}
}

func (ps *ProcessorStore) createNewMemtable() {
	arena := arenaskl.NewArena(uint32(ps.pm.cfg.MemtableMaxSizeBytes))
	ps.mt = mem.NewMemtable(arena, ps.pm.cfg.NodeID, int(ps.pm.cfg.MemtableMaxSizeBytes))
}

func (p *processor) ValidateKeyRange(key []byte) {
	inRange := bytes.Compare(key, p.keyRangeStart) >= 0 && (p.keyRangeEnd == nil || bytes.Compare(key, p.keyRangeEnd) < 0)
	if !inRange {
		panic(fmt.Sprintf("processor %d writing key %v, but range start %v range end %v", p.id, key, p.keyRangeStart, p.keyRangeEnd))
	}
}

func (ps *ProcessorStore) Write(batch *mem.Batch) error {
	ps.lock.Lock() // Note, largely uncontended so minimal overhead
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	if ps.mt == nil {
		ps.createNewMemtable()
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
	if err := ps.replaceMemtable(false, nil); err != nil {
		return err
	}
	ps.createNewMemtable()
	ok, err = ps.mt.Write(batch)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("batch is too large to be written in memtable")
	}
	return nil
}

func (ps *ProcessorStore) maybeReplaceMemtable() error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if !ps.started {
		return nil
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
	if now-ps.lastMTReplaceTime < uint64(ps.pm.cfg.MemtableMaxReplaceInterval) {
		return nil
	}
	if err := ps.replaceMemtable(true, nil); err != nil {
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
	return ps.replaceMemtable(false, cb)
}

func (ps *ProcessorStore) replaceMemtable(periodic bool, cb func(error)) error {
	if err := ps.sem.Acquire(context.Background(), 1); err != nil {
		return err
	}
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	lcv := ps.pm.GetLastCompletedVersion()
	// Note, we add a callback on the entry, not on the memtable - the memtable callback is only called when the memtable
	// is successfully pushed, but we need the callback to always be called when the entry is processed (even in case
	// of error), so we use a different callback
	ps.queue = append(ps.queue, &flushQueueEntry{
		mt:                   ps.mt,
		lastCompletedVersion: lcv,
		seq:                  atomic.AddUint64(&ps.writeSeq, 1),
		periodic:             periodic,
		cb:                   cb,
	})
	ps.mt = nil
	ps.pushChan <- struct{}{}
	return nil
}

func (ps *ProcessorStore) clear() error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if !ps.started {
		return errors.New("processor store not started")
	}
	// We lock queueLock too to make sure no pushes in progress
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
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
		var err error
		if entry.mt != nil {
			// Memtable can be nil if we're flushing and no writes occurred
			if entry.seq >= atomic.LoadUint64(&ps.firstGoodSequence) && !ps.stopping.Load() {
				err = ps.buildAndPushTable(entry)
			} else {
				log.Debugf("didn't push memtable %s as clearing/stopping", entry.mt.Uuid)
			}
		}
		ps.queueLock.Lock()
		// Remove the pushed entry, note: we copy, so we don't retain references to popped entries in buffer
		newEntries := make([]*flushQueueEntry, len(ps.queue)-1, cap(ps.queue))
		//sanity check
		if ps.queue[0].mt != entry.mt {
			panic("removing wrong memtable")
		}

		copy(newEntries, ps.queue[1:])
		ps.queue = newEntries
		ps.queueLock.Unlock()
		ps.sem.Release(1)

		ps.pm.storeFlushed(ps.processorID, int(entry.lastCompletedVersion))
		if entry.periodic {
			ps.periodicReplaceInQueue.Store(false)
		}

		if entry.mt != nil {
			// Call the flushed callbacks
			entry.mt.Flushed(err)
			entry.mt.Close()
		}
		if entry.cb != nil {
			// Note, we do not use the memtable flush callbacks here as memtable can be nil when flushing
			entry.cb(err)
		}
		if err != nil {
			log.Errorf("failed to push entry %v", err)
			return
		}
	}
}

func (ps *ProcessorStore) buildAndPushTable(entry *flushQueueEntry) error {
	// build sstable and push to cloud and register them with the level-manager
	if entry.mt.HasWrites() {
		iter := entry.mt.NewIterator(nil, nil)
		ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst2.BuildSSTable(ps.pm.cfg.TableFormat,
			int(ps.pm.cfg.MemtableMaxSizeBytes), 8*1024, iter)
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
			if err := ps.pm.cloudStoreClient.Put(id, tableBytes); err != nil {
				if common.IsUnavailableError(err) {
					// Transient availability error - retry
					log.Warnf("cloud store is unavailable, will retry: %v", err)
					time.Sleep(ps.pm.cfg.SSTablePushRetryDelay)
					continue
				}
				return err
			}
			log.Debugf("store %d added sstable with id %v for memtable %s to cloud store", ps.pm.cfg.NodeID, id,
				entry.mt.Uuid)
			break
		}
		if err := ps.pm.tableCache.AddSSTableWithMaxAge(id, ssTable); err != nil {
			return err
		}
		log.Debugf("store %d added sstable with id %v for memtable %s to table cache", ps.pm.cfg.NodeID, id,
			entry.mt.Uuid)
		for {
			if ps.stopping.Load() {
				return nil
			}
			clusterVersion := ps.pm.ClusterVersion()
			if clusterVersion < 0 {
				panic("cluster version not set")
			}
			// register with level-manager
			log.Debugf("node %d processor %d registering memtable %s", ps.pm.cfg.NodeID, ps.processorID, entry.mt.Uuid)
			if err := ps.pm.levelManagerClient.RegisterL0Tables(levels.RegistrationBatch{
				ClusterName:    ps.pm.cfg.ClusterName,
				ClusterVersion: clusterVersion,
				ProcessorID:    ps.processorID,
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
						time.Sleep(ps.pm.cfg.SSTableRegisterRetryDelay)
						continue
					}
				}
				return err
			}
			log.Debugf("node %d processor %d registered memtable %s with levelManager sstableid %v- max version %d %p",
				ps.pm.cfg.NodeID, ps.processorID, entry.mt.Uuid, id, maxVersion, entry.mt)
			break
		}
	} else {
		log.Debugf("node %d entry to flush has no writes", ps.pm.cfg.NodeID)
	}
	log.Debugf("store %d calling flushed callback for memtable %s", ps.pm.cfg.NodeID, entry.mt.Uuid)
	return nil
}

// Get the value for the key, or nil if it doesn't exist.
// Note the key argument must not contain the version.
// Returns the highest version for the key
func (ps *ProcessorStore) Get(key []byte) ([]byte, error) {
	return ps.GetWithMaxVersion(key, math.MaxUint64)
}

func (ps *ProcessorStore) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
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
	log.Debugf("seq:%d node: %d store GetWithMaxVersion key start:%v key end:%v maxVersion:%d", ps.pm.cfg.NodeID,
		key, keyEnd, maxVersion)
	if ps.mt != nil {
		// First look in live memTable
		val, err := findInMemtable(ps.mt, key, keyEnd, maxVersion)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	val, err := ps.findInQueuedEntries(key, keyEnd, maxVersion)
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
		if entry.mt != nil {
			val, err := findInMemtable(entry.mt, keyStart, keyEnd, maxVersion)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
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
	ids, deadVersions, err := ps.pm.levelManagerClient.GetTableIDsForRange(keyStart, keyEnd)
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
			lazy, err := sst2.NewLazySSTableIterator(nonOverLapIDs[0], ps.pm.tableCache, keyStart, keyEnd, ps.iterFactoryFunc())
			if err != nil {
				return nil, err
			}
			iters = append(iters, lazy)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				log.Debugf("using sstable %v in iterator [%d, %d] for key start %v", nonOverlapID, i, j, keyStart)
				lazy, err := sst2.NewLazySSTableIterator(nonOverlapID, ps.pm.tableCache, keyStart, keyEnd, ps.iterFactoryFunc())
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
	iters := make([]iteration.Iterator, 0, 4)
	// First we add the current memtable
	if ps.mt != nil {
		iters = append(iters, ps.mt.NewIterator(keyStart, keyEnd))
	}
	// Then the tables in the queue
	ps.queueLock.RLock()
	for i := len(ps.queue) - 1; i >= 0; i-- {
		entry := ps.queue[i]
		if entry.mt != nil {
			iters = append(iters, entry.mt.NewIterator(keyStart, keyEnd))
		}
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
	return iteration.NewMergingIterator(iters, preserveTombstones, highestVersion)
}
