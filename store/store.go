package store

import (
	"bytes"
	"encoding/binary"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/tabcache"
	"math"
	"sync"
	"sync/atomic"
	"time"

	_ "net/http/pprof" //nolint:gosec
)

type Store struct {
	started            atomic.Bool
	stopped            bool
	conf               conf.Config
	memTable           *mem.Memtable
	cloudStoreClient   objstore.Client
	levelManagerClient levels.Client
	tableCache         *tabcache.Cache
	lock               sync.RWMutex
	flushChan          chan struct{}
	queueFull          atomic.Bool
	// We use a separate lock to protect the flush queue as we don't want the removal from queue to block
	// writes to the memtable
	mtQueue                     []*flushQueueEntry
	mtFlushQueueLock            sync.Mutex
	lastCompletedVersion        int64
	lastLocalFlushedVersion     int64
	minCurrentMTVersion         int64
	iterators                   sync.Map
	mtReplaceTimerLock          sync.Mutex
	mtReplaceTimer              *common.TimerHandle
	mtLastReplace               uint64
	mtMaxReplaceTime            uint64
	lastCommittedBatchSequences sync.Map
	stopWg                      sync.WaitGroup
	clusterVersion              uint64
	versionFlushedHandler       func(version int)
	clearFlushedLock            sync.Mutex
	periodicMtInQueue           atomic.Bool // we limit to one empty memtable for periodic flush in queue at any one time
	shutdownFlushImmediate      bool
	clearing                    atomic.Bool
	//flushing                    atomic.Bool
}

func NewStore(cloudStoreClient objstore.Client, levelManagerClient levels.Client, tableCache *tabcache.Cache, conf conf.Config) *Store {
	return &Store{
		conf:                    conf,
		cloudStoreClient:        cloudStoreClient,
		levelManagerClient:      levelManagerClient,
		tableCache:              tableCache,
		mtMaxReplaceTime:        uint64(conf.MemtableMaxReplaceInterval),
		lastCompletedVersion:    -2, // -2 as -1 is a valid value
		lastLocalFlushedVersion: -1,
	}
}

func (s *Store) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		panic("store cannot be restarted")
	}
	if s.started.Load() {
		return nil
	}
	s.createNewMemtable()
	s.stopWg = sync.WaitGroup{}
	s.stopWg.Add(1)
	s.flushChan = make(chan struct{}, 10)
	s.mtQueue = nil
	s.queueFull.Store(false)
	s.lastCompletedVersion = -1
	s.iterators = sync.Map{}
	s.mtLastReplace = 0
	s.lastCommittedBatchSequences = sync.Map{}
	s.clusterVersion = 0
	s.started.Store(true)
	common.Go(s.flushLoop)
	s.scheduleMtReplace()
	return nil
}

func (s *Store) Stop() error {
	s.lock.Lock()
	if !s.started.Load() {
		s.lock.Unlock()
		return nil
	}
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	if s.mtReplaceTimer != nil {
		s.mtReplaceTimer.Stop()
		s.mtReplaceTimer = nil
	}
	s.started.Store(false)
	close(s.flushChan)
	s.stopped = true
	s.lock.Unlock()
	s.stopWg.Wait()
	return nil
}

func (s *Store) CloudStore() objstore.Client {
	return s.cloudStoreClient
}

func (s *Store) maybeBlockWrite() error {
	for s.queueFull.Load() {
		if !s.started.Load() {
			return errors.New("store is not started")
		}
		time.Sleep(s.conf.StoreWriteBlockedRetryInterval)
	}
	return nil
}

func (s *Store) Write(batch *mem.Batch) error {
	if err := s.maybeBlockWrite(); err != nil {
		return err
	}
	attempts := 0
	for {
		memtable, ok, err := s.doWrite(batch)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// No more space left in memtable - swap writeIter out and replace writeIter with a new one and flush writeIter async
		log.Debug("no more space in memtable - replacing it")
		if err := s.replaceMemtable(memtable, true); err != nil {
			return err
		}
		attempts++
		if attempts > 1 {
			// To prevent a busy loop when many writes occurring concurrently and memtable is being replaced, we
			// introduce a delay
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (s *Store) Flush(sync bool, shutdown bool) error {
	if !s.started.Load() {
		return nil
	}
	log.Debugf("node %d calling store.Flush()", s.conf.NodeID)
	ch, err := s.flush(shutdown)
	if err != nil {
		return err
	}
	if sync {
		return <-ch
	}
	log.Debugf("node %d called store.Flush()", s.conf.NodeID)
	return nil
}

func (s *Store) SetClusterVersion(version int) {
	log.Debugf("node %d store receiving cluster version %d", s.conf.NodeID, version)
	atomic.StoreUint64(&s.clusterVersion, uint64(version))
}

func (s *Store) createNewMemtable() {
	arena := arenaskl.NewArena(uint32(s.conf.MemtableMaxSizeBytes))
	s.memTable = mem.NewMemtable(arena, s.conf.NodeID, int(s.conf.MemtableMaxSizeBytes))
}

func (s *Store) getClusterVersion() int {
	return int(atomic.LoadUint64(&s.clusterVersion))
}

func (s *Store) flush(shutdown bool) (chan error, error) {
	// We flush the table, whether or not it has writes. If it's empty it will go through the whole flush process
	// but not actually result in an SSTable being pushed or registered, but it will ensure everything before it has
	// been flushed
	s.lock.Lock()
	defer s.lock.Unlock()
	//if s.flushing.Get() {
	//	// Don't flush again if already flushing
	//	return nil, nil
	//}
	//s.flushing.Set(true)

	if shutdown {
		// During shutdown any completed version will trigger a store flush in order to speed up the shutdown process.
		// Otherwise, can wait for a periodic flush before all data is flushed.
		s.shutdownFlushImmediate = true
	}

	// We flush even if there is no data to flush as this prompts last completed version to get flushed to version
	// manager from this node.
	ch := make(chan error, 1)
	log.Debugf("node %d adding flushed callback on memtable %s", s.conf.NodeID, s.memTable.Uuid)
	s.memTable.AddFlushedCallback(func(err error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		//s.flushing.Set(false)
		ch <- err
	})
	log.Debugf("replacing memtable %s on node %d because of flush", s.memTable.Uuid, s.conf.NodeID)
	if err := s.replaceMemtable0(s.memTable, true, true); err != nil {
		//s.flushing.Set(false)
		return nil, err
	}
	return ch, nil
}

func (s *Store) SetVersionFlushedHandler(handler func(version int)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.versionFlushedHandler = handler
}

func (s *Store) SetClusterMessageHandlers(vbHandler *remoting.TeeBlockingClusterMessageHandler) {
	vbHandler.Handlers = append(vbHandler.Handlers, s.CompletedVersionHandler())
}

// Used in testing only
func (s *Store) forceReplaceMemtable() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.replaceMemtable0(s.memTable, true, false)
}

func (s *Store) replaceMemtable(memtable *mem.Memtable, triggerFlushLoop bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		return nil
	}
	return s.replaceMemtable0(memtable, true, triggerFlushLoop)
}

func (s *Store) replaceMemtable0(memtable *mem.Memtable, allowFlushEmpty bool, triggerFlushLoop bool) error {
	// We do a check that it's the same memtable here under lock as writes are concurrent and two writes could
	// concurrently return full - we don't want to replace the mt more than once!
	if memtable == s.memTable {
		if !allowFlushEmpty && !s.memTable.HasWrites() {
			log.Debugf("node %d no writes so doing nothing", s.conf.NodeID)
			return nil
		}
		s.createNewMemtable()
		if err := s.updateIterators(s.memTable); err != nil {
			return err
		}
		s.mtFlushQueueLock.Lock()
		lcv := s.GetLastCompletedVersion()
		if lcv == -2 {
			// We must have received at least one version broadcast
			panic("last completed version not received yet")
		}
		s.mtQueue = append(s.mtQueue, &flushQueueEntry{
			memtable:             memtable,
			lastCompletedVersion: lcv,
		})
		if len(s.mtQueue) == s.conf.MemtableFlushQueueMaxSize {
			s.queueFull.Store(true)
		}
		//s.mtQueueChan <- struct{}{} // will block if queue is full
		log.Debugf("store %d added memtable %s to flush queue with lcv: %d", s.conf.NodeID, memtable.Uuid, lcv)
		s.mtFlushQueueLock.Unlock()
		if triggerFlushLoop {
			log.Debugf("node %d triggering flush loop", s.conf.NodeID)
			s.triggerFlushLoop()
		}
		s.mtLastReplace = common.NanoTime()
	} else {
		log.Debugf("node %d didn't replace memtable as it changed", s.conf.NodeID)
	}
	return nil
}

func (s *Store) doWrite(batch *mem.Batch) (*mem.Memtable, bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if !s.started.Load() {
		// We return an unavailable error here. This can occur on shutdown as the store is stopped before processors
		// The sender can retry.
		return nil, false, errors.NewTektiteErrorf(errors.Unavailable, "store not started")
	}

	mt := s.memTable
	ok, err := mt.Write(batch)
	if err != nil && errors.Is(err, arenaskl.ErrArenaFull) {
		// Should never happen
		panic("memtable arena full")
	}
	return mt, ok, err
}

func (s *Store) updateLastCompletedVersion(version int64) {
	s.lock.Lock() // We have to lock to protect the flush channel - it gets closed from another goroutine
	defer s.lock.Unlock()
	if !s.started.Load() {
		return
	}
	atomic.StoreInt64(&s.lastCompletedVersion, version)
	log.Debugf("store %d received last completed version %d", s.conf.NodeID, version)
	if s.shutdownFlushImmediate {
		log.Debugf("store %d flushing immediately as shutting down", s.conf.NodeID)
		if err := s.replaceMemtable0(s.memTable, true, true); err != nil {
			log.Errorf("failed to replace memtable: %v", err)
		}
	}
}

// We periodically replace the memtable if it hasn't already been replaced within a max period
func (s *Store) maybeReplaceMemtable() error {
	// Note we don't use time.Now() as it is *not* monotonic - it uses system time so any adjustments to system time
	// would make this go wrong
	s.lock.RLock()
	now := common.NanoTime()
	if s.mtLastReplace != 0 {
		if s.mtLastReplace-now >= s.mtMaxReplaceTime && !s.periodicMtInQueue.Load() {
			mt := s.memTable
			mt.AddFlushedCallback(func(err error) {
				s.periodicMtInQueue.Store(false)
			})
			s.lock.RUnlock()
			s.periodicMtInQueue.Store(true)
			return s.replaceMemtable(mt, true)
		}
	} else {
		s.mtLastReplace = now
	}
	s.lock.RUnlock()
	return nil
}

func (s *Store) scheduleMtReplace() {
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	s.mtReplaceTimer = common.ScheduleTimer(s.conf.MemtableMaxReplaceInterval, false, func() {
		if err := s.maybeReplaceMemtable(); err != nil {
			log.Errorf("failed to replace memtable %+v", err)
		}
		s.scheduleMtReplace()
	})
}

// Get the value for the key, or nil if it doesn't exist.
// Note the key argument must not contain the version.
// Returns the highest version for the key
func (s *Store) Get(key []byte) ([]byte, error) {
	return s.GetWithMaxVersion(key, math.MaxUint64)
}

func (s *Store) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	if !s.started.Load() {
		return nil, errors.NewTektiteErrorf(errors.Unavailable, "store not started")
	}
	unlocked := false
	s.lock.RLock()
	s.mtFlushQueueLock.Lock()
	defer func() {
		if !unlocked {
			s.mtFlushQueueLock.Unlock()
			s.lock.RUnlock()
		}
	}()

	keyEnd := common.IncrementBytesBigEndian(key)

	log.Debugf("seq:%d node: %d store GetWithMaxVersion key start:%v key end:%v maxVersion:%d", s.conf.NodeID,
		key, keyEnd, maxVersion)

	// First look in live memTable
	val, err := findInMemtable(s.memTable, key, keyEnd, maxVersion)
	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	// Then in the memTables in the flush queue from newest to oldest
	for i := len(s.mtQueue) - 1; i >= 0; i-- {
		entry := s.mtQueue[i]
		val, err := findInMemtable(entry.memtable, key, keyEnd, maxVersion)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	// Now we can unlock and look in the SSTables - we don't want to hold the lock while we're doing the remote call
	s.mtFlushQueueLock.Unlock()
	s.lock.RUnlock()
	unlocked = true
	iters, err := s.createSSTableIterators(key, keyEnd)
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

func (s *Store) Dump() error {
	log.Debugf("*********** DUMPING STORE ON NODE %d", s.conf.NodeID)
	iter, err := s.NewIterator(nil, nil, math.MaxUint64, false)
	if err != nil {
		return err
	}
	defer iter.Close()
	for {
		valid, err := iter.IsValid()
		if err != nil {
			return err
		}
		if !valid {
			break
		}
		curr := iter.Current()
		log.Debugf("key:%v value:%v", curr.Key, curr.Value)
		if err := iter.Next(); err != nil {
			return err
		}
	}
	log.Debug("*********** END DUMPING STORE")
	return nil
}

type completedVersionHandler struct {
	s *Store
}

func (c *completedVersionHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	vbm := messageHolder.Message.(*clustermsgs.VersionsMessage)
	c.s.updateLastCompletedVersion(vbm.CompletedVersion)
	return nil, nil
}

func (s *Store) CompletedVersionHandler() remoting.BlockingClusterMessageHandler {
	return &completedVersionHandler{s: s}
}

func (s *Store) mtQueueInfo() (queueSize int) {
	s.mtFlushQueueLock.Lock()
	defer s.mtFlushQueueLock.Unlock()
	return len(s.mtQueue)
}

func (s *Store) GetLastCompletedVersion() int64 {
	return atomic.LoadInt64(&s.lastCompletedVersion)
}
