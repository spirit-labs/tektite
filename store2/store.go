package store2

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	sst2 "github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"sync"
	"sync/atomic"
	"time"
)

type ProcessorStore struct {
	s                      *Store
	lock                   sync.Mutex
	processorID            int
	mt                     *mem.Memtable
	queue                  *PushQueue
	stopping               atomic.Bool
	started                bool
	stopWG                 sync.WaitGroup
	lastMTReplaceTime      uint64
	periodicReplaceInQueue atomic.Bool
	writeSeq               uint64
	firstGoodSequence      uint64
}

func NewProcessorStore(s *Store, processorID int) *ProcessorStore {
	return &ProcessorStore{
		s:           s,
		processorID: processorID,
		queue:       &PushQueue{maxEntries: 10}, // make configurable
	}
}

type flushQueueEntry struct {
	mt                   *mem.Memtable
	lastCompletedVersion int64
	periodic             bool
	seq                  uint64
	firstGoodSeq         uint64
}

func (p *ProcessorStore) start() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stopWG.Add(1)
	go p.pushLoop()
	p.started = true
	p.createNewMemtable()
}

func (p *ProcessorStore) isStarted() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.started
}

func (p *ProcessorStore) stop() {
	// Set stopping outside the lock - this unblocks the pushLoop if it is in a retry loop
	// it then continues consuming any remaining entries from the push queue and discards them
	// allowing any blocked push queue writer to unblock
	p.stopping.Store(true)
	// Now we can get the lock
	p.lock.Lock()
	defer p.lock.Unlock()
	// Now we know there will be no more writers now
	p.started = false
	// And we can close the push channel - this will cause the push Loop to exit
	p.queue.close()
	// And wait for it to exit
	p.stopWG.Wait()
}

func (p *ProcessorStore) createNewMemtable() {
	arena := arenaskl.NewArena(uint32(p.s.cfg.MemtableMaxSizeBytes))
	p.mt = mem.NewMemtable(arena, p.s.cfg.NodeID, int(p.s.cfg.MemtableMaxSizeBytes))
}

func (p *ProcessorStore) Write(batch *mem.Batch, lastCompletedVersion int64) error {
	p.lock.Lock() // Note, largely uncontended so minimal overhead
	defer p.lock.Unlock()
	if !p.started {
		return errors.New("processor store not started")
	}
	ok, err := p.mt.Write(batch)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Memtable is full - add it to the push queue
	// This will block if queue is full, so provides throttling if writing faster than level manager can accept new
	// tables
	p.queue.addEntry(&flushQueueEntry{
		mt:                   p.mt,
		lastCompletedVersion: lastCompletedVersion,
		seq:                  atomic.AddUint64(&p.writeSeq, 1),
	})
	p.createNewMemtable()
	return nil
}

func (p *ProcessorStore) maybeFlushMemtable(lastCompletedVersion int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return errors.New("processor store not started")
	}
	if p.periodicReplaceInQueue.Load() {
		// We already have a periodic-replace queued, don't queue another one
		return nil
	}
	now := common.NanoTime()
	if p.lastMTReplaceTime == 0 {
		p.lastMTReplaceTime = now
		return nil
	}
	if now-p.lastMTReplaceTime < uint64(p.s.cfg.MemtableMaxReplaceInterval) {
		return nil
	}
	p.queue.addEntry(&flushQueueEntry{
		mt:                   p.mt,
		lastCompletedVersion: lastCompletedVersion,
		periodic:             true,
		seq:                  atomic.AddUint64(&p.writeSeq, 1),
	})
	p.createNewMemtable()
	p.periodicReplaceInQueue.Store(true)
	p.lastMTReplaceTime = now
	return nil
}

func (p *ProcessorStore) flush(lastCompletedVersion int64, cb func(error)) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return errors.New("processor store not started")
	}
	p.mt.AddFlushedCallback(cb)
	p.queue.addEntry(&flushQueueEntry{
		mt:                   p.mt,
		lastCompletedVersion: lastCompletedVersion,
		seq:                  atomic.AddUint64(&p.writeSeq, 1),
	})
	p.createNewMemtable()
	return nil
}

func (p *ProcessorStore) clear() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return errors.New("processor store not started")
	}
	// We set the first good sequence to be the next write sequence
	// Any entries with seq < first good sequence will be ignored when read by the push loop
	// so this means they are logically "cleared"
	atomic.StoreUint64(&p.firstGoodSequence, atomic.LoadUint64(&p.writeSeq)+1)
	return nil
}

func (p *ProcessorStore) pushLoop() {
	defer p.stopWG.Done()
	for {
		entry := p.queue.popEntry()
		if entry == nil {
			// closed
			return
		}
		if p.stopping.Load() {
			// Continue consuming any entries - this unblocks any writer of the push queue
			continue
		}
		if entry.seq < atomic.LoadUint64(&p.firstGoodSequence) {
			// cleared entry
			continue
		}
		if err := p.buildAndPushTable(entry); err != nil {
			log.Error(err)
			return
		}
	}
}

func (p *ProcessorStore) buildAndPushTable(entry *flushQueueEntry) error {
	// build sstable and push to cloud and register them with the level-manager
	if entry.mt != nil { // nil memtables are used for flush
		iter := entry.mt.NewIterator(nil, nil)
		ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst2.BuildSSTable(p.s.cfg.TableFormat,
			int(p.s.cfg.MemtableMaxSizeBytes), 8*1024, iter)
		if err != nil {
			return err
		}
		// Push and register the SSTable
		id := []byte(fmt.Sprintf("sst-%s", uuid.New().String()))
		tableBytes := ssTable.Serialize()
		for {
			if p.stopping.Load() {
				return nil
			}
			if err := p.s.cloudStoreClient.Put(id, tableBytes); err != nil {
				if common.IsUnavailableError(err) {
					// Transient availability error - retry
					log.Warnf("cloud store is unavailable, will retry: %v", err)
					time.Sleep(p.s.cfg.SSTablePushRetryDelay)
					continue
				}
				return err
			}
			log.Debugf("store %d added sstable with id %v for memtable %s to cloud store", p.s.cfg.NodeID, id,
				entry.mt.Uuid)
			break
		}
		if err := p.s.tableCache.AddSSTable(id, ssTable); err != nil {
			return err
		}
		log.Debugf("store %d added sstable with id %v for memtable %s to table cache", p.s.cfg.NodeID, id,
			entry.mt.Uuid)
		for {
			if p.stopping.Load() {
				return nil
			}
			clusterVersion := p.s.getClusterVersion()
			// register with level-manager
			if err := p.s.levelManagerClient.RegisterL0Tables(levels.RegistrationBatch{
				ClusterName:    p.s.cfg.ClusterName,
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
						if p.stopping.Load() {
							// Allow to break out of the loop if stopped or clearing
							return nil
						}
						// Transient availability error - retry
						log.Warnf("store failed to register new ss-table with level manager, will retry: %v", err)
						time.Sleep(p.s.cfg.SSTableRegisterRetryDelay)
						continue
					}
				}
				return err
			}
			log.Debugf("node %d registered memtable %s with levelManager sstableid %v- max version %d",
				p.s.cfg.NodeID, entry.mt.Uuid, id, maxVersion)
			break
		}
	} else {
		log.Debugf("node %d entry to flush has no writes", p.s.cfg.NodeID)
	}
	p.s.versionFlushed(p.processorID, entry.lastCompletedVersion)
	if entry.periodic {
		p.periodicReplaceInQueue.Store(false)
	}
	log.Debugf("store %d calling flushed callback for memtable %s", p.s.cfg.NodeID, entry.mt.Uuid)
	return entry.mt.Flushed(nil)
}

type PushQueue struct {
	lock       sync.RWMutex
	ch         chan *flushQueueEntry
	entries    []*flushQueueEntry
	maxEntries int
}

func (pq *PushQueue) addEntry(entry *flushQueueEntry) {
	pq.ch <- entry
	pq.lock.Lock()
	defer pq.lock.Unlock()
	pq.entries = append(pq.entries, entry)
}

func (pq *PushQueue) popEntry() *flushQueueEntry {
	entry, ok := <-pq.ch
	if !ok {
		return nil
	}
	pq.lock.Lock()
	defer pq.lock.Unlock()
	newEntries := make([]*flushQueueEntry, len(pq.entries)-1, pq.maxEntries)
	// we copy, so we don't retain references to popped entries in buffer
	copy(newEntries, pq.entries[1:])
	return entry
}

func (pq *PushQueue) close() {
	close(pq.ch)
}

type Store struct {
	cfg                    conf.Config
	cloudStoreClient       objstore.Client
	levelManagerClient     levels.Client
	tableCache             *tabcache.Cache
	iterators              sync.Map
	mtReplaceTimerLock     sync.Mutex
	mtReplaceTimer         *common.TimerHandle
	clusterVersion         uint64
	versionFlushedHandler  func(version int)
	shutdownFlushImmediate bool
	procStores             []*ProcessorStore
	lastCompletedVersion   int64
}

func NewStore(cfg conf.Config, cloudStoreClient objstore.Client, levelManagerClient levels.Client,
	tableCache *tabcache.Cache) *Store {
	s := &Store{
		cfg:                cfg,
		cloudStoreClient:   cloudStoreClient,
		levelManagerClient: levelManagerClient,
		tableCache:         tableCache,
	}
	s.procStores = make([]*ProcessorStore, cfg.ProcessorCount)
	for i := 0; i < cfg.ProcessorCount; i++ {
		s.procStores[i] = NewProcessorStore(s, i)
	}
	return s
}

// FIXME - call processorStore.Write direct from processor
func (s *Store) Write(batch *mem.Batch, processorID int) error {
	return s.procStores[processorID].Write(batch, s.getLastCompletedVersion())
}

// FIXME - this can be coordinated from processor manager
func (s *Store) Flush() error {
	lcv := s.getLastCompletedVersion()
	var startedPs []*ProcessorStore
	for _, ps := range s.procStores {
		if ps.isStarted() {
			startedPs = append(startedPs, ps)
		}
	}
	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(len(startedPs), func(err error) {
		ch <- err
	})
	for _, ps := range startedPs {
		if err := ps.flush(lcv, cf.CountDown); err != nil {
			return err
		}
	}
	return <-ch
}

func (s *Store) Start() error {
	s.scheduleMtReplace()
	return nil
}

// FIXME - this can be coordinated from processor manager and we can call each processor directly.
func (s *Store) scheduleMtReplace() {
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	s.mtReplaceTimer = common.ScheduleTimer(s.cfg.MemtableMaxReplaceInterval, false, func() {
		if err := s.maybeReplaceMemtables(); err != nil {
			log.Errorf("failed to replace memtable %+v", err)
		}
		s.scheduleMtReplace()
	})
}

func (s *Store) maybeReplaceMemtables() error {
	lcv := s.getLastCompletedVersion()
	for _, ps := range s.procStores {
		if ps.isStarted() {
			if err := ps.maybeFlushMemtable(lcv); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) Stop() error {
	s.mtReplaceTimerLock.Lock()
	defer s.mtReplaceTimerLock.Unlock()
	if s.mtReplaceTimer != nil {
		s.mtReplaceTimer.Stop()
		s.mtReplaceTimer = nil
	}
	for _, ps := range s.procStores {
		if ps.isStarted() {
			ps.stop()
		}
	}
	return nil
}

func (s *Store) versionFlushed(processorID int, flushedVersion int64) {
	// TODO
}

func (s *Store) getClusterVersion() int {
	return int(atomic.LoadUint64(&s.clusterVersion))
}

// FIXME - if we plug the processor store direct into the processor, then we
// can get last completed version and cluster version from the processor manager directly, and
// don't need this here.
func (s *Store) SetClusterVersion(version int) {
	log.Debugf("node %d store receiving cluster version %d", s.cfg.NodeID, version)
	atomic.StoreUint64(&s.clusterVersion, uint64(version))
}

func (s *Store) updateLastCompletedVersion(version int64) {
	atomic.StoreInt64(&s.lastCompletedVersion, version)
	log.Debugf("store %d received last completed version %d", s.cfg.NodeID, version)
	if s.shutdownFlushImmediate {
		for _, ps := range s.procStores {
			if ps.isStarted() {
				if err := ps.flush(version, func(error) {}); err != nil {
					log.Errorf("failed to flush %v", err)
				}
			}
		}
	}
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

func (s *Store) getLastCompletedVersion() int64 {
	return atomic.LoadInt64(&s.lastCompletedVersion)
}
