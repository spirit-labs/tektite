package proc

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/debug"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/store"
	"github.com/timandy/routine"
	"sync"
	"sync/atomic"
	"time"
)

/*
How barriers and snapshots work:

* The processor maintains a currentVersion - this is used to assign a version to a ProcessBatch when it is first ingested.
* When a ProcessBatch is processed and causes new ProcessBatch(es) to be created and forwarded to other processors. The
forwarded batches are assigned the same version as the original batch.
* A barrier can be injected at a processor for a particular receiver id. If the processing of a batch on the receiver
can forward batches to other processors, then the processing of a barrier on a processor causes a barrier to be
forwarded to every receiver the receiver can forward to.
* Forwarded barriers are not forwarded to other processors until that processor has received a barrier from every other processor
for that receiver id. This ensures that once a barrier is forwarded to another processor no more batches for version <=
barrier version will be forwarded.
* When a barrier is injected at a processor, current version is set to the barrier version + 1
* When all barriers have made their way to all terminating receivers (those that don't forward), we know that the version
is complete for that processor. I.e. that there are no more batches currently or in the future being processed for
that version.
* When the version is complete for all processors in the system, we know we have a consistent snapshot. I.e. the snapshot reflects
the set of updates caused by processing ingests from all receivers up to specific value of offset for each receiver, in the stream
of batches being ingested.

*/

type Processor interface {
	ID() int

	IngestBatch(processBatch *ProcessBatch, completionFunc func(error))

	IngestBatchSync(processBatch *ProcessBatch) error

	ProcessBatch(batch *ProcessBatch, completionFunc func(error))

	ReprocessBatch(batch *ProcessBatch, completionFunc func(error))

	SetLeader()

	IsLeader() bool

	CheckInProcessorLoop()

	Stop()

	InvalidateCachedReceiverInfo()

	SetVersionCompleteHandler(handler VersionCompleteHandler)

	SetNotIdleNotifier(func())

	IsIdle(completedVersion int) bool

	IsStopped() bool

	SetReplicator(replicator Replicator)

	GetReplicator() Replicator

	SubmitAction(action func() error) bool

	CloseVersion(version int, receiverIDs []int)

	WriteCache() *WriteCache

	LoadLastProcessedReplBatchSeq(version int) (int64, error)
}

type VersionCompleteHandler func(version int, requiredCompletions int, commandID int, doom bool, completionFunc func(error))

func NewProcessor(id int, cfg *conf.Config, store *store.Store, batchForwarder BatchForwarder,
	batchHandler BatchHandler, receiverInfoProvider ReceiverInfoProvider, dataKey []byte) Processor {
	// We choose cache max size to 25% of the available space in a newly created memtable
	cacheMaxSize := int64(0.25 * float64(int64(cfg.MemtableMaxSizeBytes)-mem.MemtableSizeOverhead))
	levelManagerProcessor := cfg.LevelManagerEnabled && id == cfg.ProcessorCount
	replSeqKey := encoding.EncodeEntryPrefix(dataKey, common.ReplSeqSlabID, 24)
	proc := &processor{
		id:                        id,
		cfg:                       cfg,
		batchForwarder:            batchForwarder,
		batchHandler:              batchHandler,
		receiverInfoProvider:      receiverInfoProvider,
		actions:                   make(chan func() error, cfg.MaxProcessorBatchesInProgress),
		closeCh:                   make(chan struct{}, 1),
		store:                     store,
		completedReceiverVersions: map[int]int{},
		storeCache:                NewWriteCache(store, cacheMaxSize, id),
		barrierInfos:              map[int]*barrierInfo{},
		requiredCompletions:       -1,
		currentVersion:            -1,
		levelManagerProcessor:     levelManagerProcessor,
		replSeqKey:                replSeqKey,
	}
	procCount := cfg.ProcessorCount
	if cfg.LevelManagerEnabled {
		procCount++
	}
	proc.forwardSequences = make([]int, procCount)
	proc.stopWg.Add(1)
	return proc
}

const versionCompleteRetryDelay = 250 * time.Millisecond

type processor struct {
	forwardQueue
	lock                      sync.Mutex
	id                        int
	cfg                       *conf.Config
	batchForwarder            BatchForwarder
	batchHandler              BatchHandler
	replicator                Replicator
	receiverInfoProvider      ReceiverInfoProvider
	leader                    atomic.Bool
	stopped                   bool
	shuttingDown              bool
	actions                   chan func() error
	closeCh                   chan struct{}
	internalActions           []func() error
	goID                      int64
	currentVersion            int
	storeCache                *WriteCache
	stopWg                    sync.WaitGroup
	store                     *store.Store
	barrierInfos              map[int]*barrierInfo
	verCompleteHandler        VersionCompleteHandler
	completedReceiverVersions map[int]int
	requiredCompletions       int
	idle                      bool
	lastProcessedVersion      int
	notIdleNotifier           func()
	levelManagerProcessor     bool
	replSeqKey                []byte
}

type barrierInfo struct {
	delaying                 bool
	maxVersion               int
	delayedBatches           []delayedBatch
	lastBarrierVersion       int
	barrier                  *ProcessBatch
	forwardingProcessorCount int
	receivedBarriers         map[int]struct{}
	lastCommandID            int
}

type delayedBatch struct {
	processBatch   *ProcessBatch
	completionFunc func(error)
}

func (p *processor) ID() int {
	return p.id
}

func (p *processor) WriteCache() *WriteCache {
	return p.storeCache
}

// Stop is called when individual nodes are stopped, e.g. to roll a cluster. It is not used when the whole cluster
// is being shutdown.
func (p *processor) Stop() {
	if !p.IsLeader() {
		return
	}
	p.lock.Lock()
	p.stopped = true
	p.closeCh <- struct{}{}
	p.lock.Unlock()
	p.stopWg.Wait()
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.replicator != nil {
		p.replicator.Stop()
	}
	if p.forwardResendTimer != nil {
		p.forwardResendTimer.Stop()
	}
}

func (p *processor) IsStopped() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.stopped
}

func (p *processor) IsLeader() bool {
	return p.leader.Load()
}

func (p *processor) SetReplicator(replicator Replicator) {
	p.replicator = replicator
}

func (p *processor) GetReplicator() Replicator {
	return p.replicator
}

func (p *processor) InvalidateCachedReceiverInfo() {
	p.SubmitAction(func() error {
		p.barrierInfos = map[int]*barrierInfo{}
		p.requiredCompletions = -1
		return nil
	})
}

func (p *processor) CloseVersion(version int, receiverIDs []int) {
	p.SubmitAction(func() error {
		return p.closeVersion(version, receiverIDs)
	})
}

func (p *processor) closeVersion(version int, receiverIDs []int) error {
	if version < p.currentVersion {
		// Ignore
		return nil
	}
	// We inject barriers for each receiver
	for _, receiverID := range receiverIDs {
		// Note, commandID will be set in the HandleProcessBatch implementation (typically the stream manager)
		barrier := NewBarrierProcessBatch(p.id, receiverID, version, -1, -1, -1)
		p.ProcessBatch(barrier, func(err error) {
			if err != nil {
				log.Errorf("failed to process injected barrier: %v", err)
			}
		})
	}
	p.currentVersion = version + 1
	if p.replicator != nil {
		if err := p.replicator.InitialiseLeader(false); err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) CheckInProcessorLoop() {
	if routine.Goid() != p.goID {
		panic("not on processor loop")
	}
}

func (p *processor) SetVersionCompleteHandler(handler VersionCompleteHandler) {
	p.verCompleteHandler = handler
}

func (p *processor) IngestBatchSync(processBatch *ProcessBatch) error {
	ch := make(chan error, 1)
	p.IngestBatch(processBatch, func(err error) {
		ch <- err
	})
	return <-ch
}

func (p *processor) IngestBatch(batch *ProcessBatch, completionFunc func(error)) {
	if !p.IsLeader() {
		// It's possible that leader has changed by the time this request is handled
		// we return an error and the caller should retry on the new leader
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "processor is not leader"))
		return
	}
	p.SubmitAction(func() error {
		p.ProcessBatch(batch, completionFunc)
		return nil
	})
}

func (p *processor) SetNotIdleNotifier(notifier func()) {
	p.notIdleNotifier = notifier
}

func (p *processor) notifyNotIdle() {
	if p.notIdleNotifier != nil {
		p.notIdleNotifier()
	}
}

func (p *processor) doCheckIdle(completedVersion int) bool {
	// Considered idle if no batches processed since the specified version
	if p.lastProcessedVersion <= completedVersion {
		p.idle = true
		return true
	}
	return false
}

func (p *processor) IsIdle(completedVersion int) bool {
	ch := make(chan bool, 1)
	p.SubmitAction(func() error {
		idle := p.doCheckIdle(completedVersion)
		ch <- idle
		return nil
	})
	return <-ch
}

func (p *processor) ProcessBatch(batch *ProcessBatch, completionFunc func(error)) {
	p.CheckInProcessorLoop()
	if p.shuttingDown && batch.ForwardingProcessorID == -1 && !batch.Barrier && batch.ReceiverID != common.LevelManagerReceiverID {
		// Note, we must let levelManager batches through as store flush is called during shutdown and this needs to apply
		// changes on levelManager
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "cluster is shutting down"))
		return
	}

	isBarrier := batch.Barrier
	p.doHandleBatch(batch, false, func(err error) {
		if !isBarrier && err == nil {
			p.lastProcessedVersion = p.currentVersion
			if p.idle {
				p.notifyNotIdle()
				p.idle = false
			}
		}
		completionFunc(err)
	})
}

func (p *processor) ReprocessBatch(batch *ProcessBatch, completionFunc func(error)) {
	p.CheckInProcessorLoop()
	p.doHandleBatch(batch, true, completionFunc)
}

func (p *processor) doHandleBatch(batch *ProcessBatch, reprocess bool, completionFunc func(error)) {

	version := batch.Version
	barrier := batch.Barrier
	receiverID := batch.ReceiverID
	info := p.getBarrierInfo(receiverID)

	if !barrier {
		if batch.ForwardingProcessorID == -1 {
			// Injected batch
			if p.currentVersion == -1 && !p.levelManagerProcessor {
				// for level manager processor we allow batches to be handled when initial version not set - the
				// level manager does not need versions, and if it waited for initial version to be set it would
				// never initialise
				completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "initial version not set"))
				return
			}
			// Assign a version
			batch.Version = p.currentVersion
			// Process it now
			completionFunc(p.processBatch(batch, reprocess))
			return
		}
		if info.delaying && batch.Version > info.lastBarrierVersion {
			// forwarded batch - we are waiting on getting all forwarded barriers, so delay the batch,
			// completion will be called after the delayed batch is released
			info.delayedBatches = append(info.delayedBatches, delayedBatch{
				processBatch:   batch,
				completionFunc: completionFunc,
			})
			return
		}
		if batch.Version < info.maxVersion {
			// When versions are doomed and advanced on stream changes and new barriers injected before preceding
			// versions have completed, it's possible we might see batch versions go backwards - receivers must not
			// see versions going backwards as it can cause incorrect processing (e.g. of aggregates), so we keep track
			// of the max version seen for the receiver and we adjust the batch version to be the max version if its
			// less than that.
			batch.Version = info.maxVersion
		} else {
			// update the max version
			info.maxVersion = batch.Version
		}
		completionFunc(p.processBatch(batch, reprocess))
		return
	}

	lastCommandID := info.lastCommandID
	info.lastCommandID = batch.CommandID
	if lastCommandID != -1 && lastCommandID != batch.CommandID {
		log.Debugf("command id has changed this %d last %d - dooming version %d", batch.CommandID, lastCommandID, version)
		// The command ID has changed, this means a stream was undeploy/deployed during the version. We will doom
		// this version, so it cannot complete, and the version manager will move to the next version.
		p.completedReceiverVersions[receiverID] = version
		if p.verCompleteHandler != nil {
			p.sendVersionComplete(receiverID, version, -1, -1, true)
		}
		completionFunc(nil)
		return
	}

	if batch.Version < info.lastBarrierVersion {
		// We received a barrier for an older version, this can happen if a forwarding processor resends forward batches
		// after transient failure - we ignore the barrier
		completionFunc(nil)
		return
	}
	if !info.delaying && batch.Version == info.lastBarrierVersion {
		// received barrier for version that's already been completed on this processor - can happen after retry
		completionFunc(nil)
		return
	}

	if batch.ForwardingProcessorID == -1 {
		// Externally injected barriers never get delayed
		completionFunc(p.processBarrier(batch))
		return
	}

	if info.forwardingProcessorCount == -1 {
		// Lazy get and cache the value
		pc, ok := p.receiverInfoProvider.GetForwardingProcessorCount(receiverID)
		if !ok {
			// Unknown receiver - probably being deployed still, or undeployed.
			log.Debugf("node %d received barrier for unknown receiver %d version %d", p.cfg.NodeID, receiverID, batch.Version)
			completionFunc(nil)
			return
		}
		info.forwardingProcessorCount = pc
	}

	if info.forwardingProcessorCount == 1 {
		// forwarded from a single processor - no need to delay
		completionFunc(p.processBarrier(batch))
		return
	}

	if info.delaying {

		// We are currently waiting for barriers

		if version > info.lastBarrierVersion {
			// We have received a higher barrier version while we were still delaying at an earlier version
			// this can happen when versions are doomed - we ignore the previous version and go on to the next.
			// We do this as otherwise we don't know which barriers we have already received in the recovered processor
			// before failure, so we just go to the next version

			// We release any batches delayed at earlier versions
			// This is important when dooming versions as the forward queue will only forward one batch at a time, so
			// if it's waiting on completion of a delayed batch before sending next barrier, it would wait forever.
			for _, delayed := range info.delayedBatches {
				delayed.completionFunc(p.processBatch(delayed.processBatch, false))
			}
			info.delayedBatches = nil

			info.lastBarrierVersion = version
			info.barrier = batch

			info.receivedBarriers = map[int]struct{}{}
			info.receivedBarriers[batch.ForwardingProcessorID] = struct{}{}

			completionFunc(nil)
			return
		}

		if version != info.lastBarrierVersion {
			panic(fmt.Sprintf("processor %d barrier version %v not same as last barrier version %d", p.id,
				version, info.lastBarrierVersion))
		}

		// We maintain the min watermark of all barriers - unless the watermark is -1 in which case we ignore it
		// if other barriers are not -1. if all barriers are -1, we forward -1.
		if info.barrier.Watermark == -1 {
			info.barrier.Watermark = batch.Watermark
		} else if batch.Watermark != -1 && batch.Watermark < info.barrier.Watermark {
			info.barrier.Watermark = batch.Watermark
		}

		// sanity check to make sure we don't receive more than one barrier per sending receiver
		prevCount := len(info.receivedBarriers)
		info.receivedBarriers[batch.ForwardingProcessorID] = struct{}{}
		receivedBarrierCount := len(info.receivedBarriers)
		if prevCount == receivedBarrierCount {
			// We received a duplicate barrier - this can occur when barriers are retried from remote processors -
			// we ignore it
			completionFunc(nil)
			return
		}
		if receivedBarrierCount > info.forwardingProcessorCount {
			panic(fmt.Sprintf("processor %d receiver %d received too many barriers received %d expecting %d",
				p.id, receiverID,
				receivedBarrierCount, info.forwardingProcessorCount))
		}

		if receivedBarrierCount == info.forwardingProcessorCount {
			// We have received a barrier from all processors - we can now forward a single barrier
			if err := p.processBarrier(info.barrier); err != nil {
				completionFunc(err)
				return
			}
			// And we can let any delayed batches through
			if len(info.delayedBatches) > 0 {
				for _, delayed := range info.delayedBatches {
					if delayed.processBatch.Version > info.maxVersion {
						// update the max version
						info.maxVersion = delayed.processBatch.Version
					} else {
						// When versions are doomed and advanced, we can end up with batches from older version,
						// set them to max (see earlier comment in this function)
						delayed.processBatch.Version = info.maxVersion
					}
					delayed.completionFunc(p.processBatch(delayed.processBatch, false))
				}
				info.delayedBatches = info.delayedBatches[:0]
			}

			info.barrier = nil
			info.receivedBarriers = map[int]struct{}{}
			info.delaying = false
		}
	} else {
		// Not delaying - first barrier for the version
		info.lastBarrierVersion = version
		info.delaying = true
		info.barrier = batch
		info.receivedBarriers[batch.ForwardingProcessorID] = struct{}{}
	}
	// call the barrier completion func
	completionFunc(nil)
}

func (p *processor) getBarrierInfo(receiverID int) *barrierInfo {
	info, ok := p.barrierInfos[receiverID]
	if !ok {
		info = &barrierInfo{
			lastBarrierVersion:       -1,
			forwardingProcessorCount: -1,
			lastCommandID:            -1,
		}
		if //goland:noinspection GoBoolExpressions
		debug.SanityChecks {
			info.receivedBarriers = map[int]struct{}{}
		}
		p.barrierInfos[receiverID] = info
	}
	return info
}

func (p *processor) getRequiredCompletions() int {
	if p.requiredCompletions == -1 {
		p.requiredCompletions = p.receiverInfoProvider.GetRequiredCompletions()
	}
	return p.requiredCompletions
}

func (p *processor) processBarrier(barrier *ProcessBatch) error {
	_, _, remoteBatches, err := p.batchHandler.HandleProcessBatch(p, barrier, false)
	if err != nil {
		return err
	}
	// We write the cache on every barrier. However, instead of writing cache on every barrier it would be better to
	// write it when we know there will be no more barriers on a processor for a version. This is not as simple as
	// writing it on VersionComplete as VersionComplete is only called on terminal processors
	if err := p.WriteCache().MaybeWriteToStore(); err != nil {
		return err
	}
	if len(remoteBatches) > 0 {
		p.enqueueForwardBatches(remoteBatches)
	} else {
		// We are at a terminal receiver.
		if err := p.versionComplete(barrier.ReceiverID, barrier.Version, barrier.CommandID); err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) processBatch(batch *ProcessBatch, reprocess bool) error {
	ok, localBatch, remoteBatches, err := p.batchHandler.HandleProcessBatch(p, batch, reprocess)
	if err != nil {
		return err
	}

	if !ok {
		// This can occur if the batch is dropped because the rc can't be found, or the batch handler simply doesn't
		// write anything (e.g. it's the levelManager processor)
		return nil
	}

	if !p.levelManagerProcessor && batch.ReplSeq != -1 {
		// The batch is from replication (it's from a producer_endpoint, command manager command or level manager command)
		// we need to persist the batch sequence as these tell us where to replay replication queues from when rolling
		// back to last flushed version after failure.
		// Note - we do not store repl batch seq for level manager processor - this is stored in the level manager
		// master record instead
		if localBatch == nil {
			localBatch = mem.NewBatch()
		}
		localBatch = p.persistReplBatchSeq(batch, localBatch)
	}

	if localBatch != nil {
		// Write the data to the store
		if err := p.store.Write(localBatch); err != nil {
			return err
		}
	}

	if len(remoteBatches) == 0 {
		return nil
	}

	for _, remoteBatch := range remoteBatches {
		remoteBatch.Version = batch.Version
	}

	p.enqueueForwardBatches(remoteBatches)
	return nil
}

func (p *processor) versionComplete(receiverID int, version int, commandID int) error {
	p.CheckInProcessorLoop()
	lastCompletedVersion, ok := p.completedReceiverVersions[receiverID]
	if ok && version <= lastCompletedVersion {
		// ignore
		return nil
	}
	p.completedReceiverVersions[receiverID] = version
	requiredCompletions := p.getRequiredCompletions()
	if p.verCompleteHandler != nil {
		p.sendVersionComplete(receiverID, version, requiredCompletions, commandID, false)
	}
	return nil
}

func (p *processor) sendVersionComplete(receiverID int, version int, requiredCompletions int, commandID int, doom bool) {
	// Note, this is sent async to avoid blocking the event loop
	p.verCompleteHandler(version, requiredCompletions, commandID, doom, func(err error) {
		if err == nil {
			return
		}
		if common.IsUnavailableError(err) {
			// The version manager might be unavailable, e.g. it's failing over, so we retry on error
			common.ScheduleTimer(versionCompleteRetryDelay, true, func() {
				p.SubmitAction(func() error {
					// Make sure the same version needs completing
					lastCompletedVersion, ok := p.completedReceiverVersions[receiverID]
					if ok && lastCompletedVersion == version {
						requiredCompletions := p.getRequiredCompletions()
						p.sendVersionComplete(receiverID, version, requiredCompletions, commandID, doom)
					}
					return nil
				})
			})
		} else {
			// Normal to get error when shutting down
			log.Debugf("failed to send version complete %v", err)
		}
	})
}

func (p *processor) getGoID() int64 {
	return atomic.LoadInt64(&p.goID)
}

func (p *processor) SubmitAction(action func() error) bool {
	if !p.IsLeader() {
		return false
	}
	if routine.Goid() == p.getGoID() {
		// If being submitted from event loop we queue on a simple slice, this is because if we used the same
		// channel as external actions then the send to channel would block when the channel is full, causing
		// the processor to deadlock.
		p.internalActions = append(p.internalActions, action)
		return true
	}
	defer func() {
		if r := recover(); r != nil {
			// There are multiple writers to the channel that we do not hqve simple control over, so channel can
			// still panic if gets written to after close
			if //goland:noinspection GoTypeAssertionOnErrors
			err, ok := r.(error); ok && err.Error() == "send on closed channel" {
				// Panic occurred due to closed channel, ignore it
				return
			}
			panic(r)
		}
	}()
	p.actions <- action
	return true
}

func (p *processor) SetLeader() {
	log.Debugf("node %d processor %d is becoming leader", p.cfg.NodeID, p.id)
	p.leader.Store(true)
	// Need to make sure runLoop has actually started and goID set before we continue, hence the channel
	ch := make(chan struct{}, 1)
	common.Go(func() {
		p.runLoop(ch)
	})
	<-ch
}

func (p *processor) runLoop(ch chan struct{}) {
	defer common.PanicHandler()
	goID := routine.Goid()
	atomic.StoreInt64(&p.goID, goID)
	ch <- struct{}{}
	defer func() {
		p.stopWg.Done()
	}()
	p.loop()
}

func (p *processor) loop() {
	for {
		select {
		case <-p.closeCh:
			return
		case act, ok := <-p.actions:
			if !ok {
				return
			}
			if err := act(); err != nil {
				log.Errorf("processor %d failed to process action: %v", p.id, err)
			}
			for len(p.internalActions) > 0 {
				internalAct := p.internalActions[0]
				if err := internalAct(); err != nil {
					log.Errorf("processor %d failed to process internal action: %v", p.id, err)
				}
				p.internalActions = p.internalActions[1:]
			}
		}
	}
}

func (p *processor) prepareForShutdown() {
	ch := make(chan struct{}, 1)
	// Execute on event loop - ensures no data gets ingested after this
	p.SubmitAction(func() error {
		p.shuttingDown = true
		ch <- struct{}{}
		return nil
	})
	<-ch
}

func (p *processor) persistReplBatchSeq(batch *ProcessBatch, memBatch *mem.Batch) *mem.Batch {
	key := make([]byte, 24, 32)
	copy(key, p.replSeqKey)
	key = encoding.EncodeVersion(key, uint64(batch.Version))
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(batch.ReplSeq))
	if memBatch == nil {
		memBatch = mem.NewBatch()
	}
	memBatch.AddEntry(common.KV{
		Key:   key,
		Value: val,
	})
	log.Debugf("node %d processor %d persisted batch seq %d at version %d -key %v", p.cfg.NodeID, p.id, batch.ReplSeq,
		batch.Version, key)
	return memBatch
}

func (p *processor) LoadLastProcessedReplBatchSeq(version int) (int64, error) {
	value, err := p.store.GetWithMaxVersion(p.replSeqKey, uint64(version))
	if err != nil {
		return 0, err
	}
	if value == nil {
		return -1, nil
	}
	seq, _ := encoding.ReadUint64FromBufferLE(value, 0)
	return int64(seq), nil
}
