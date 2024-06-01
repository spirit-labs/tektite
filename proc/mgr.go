package proc

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/debug"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/v1/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/vmgr"
	"sync"
	"sync/atomic"
	"time"
)

type Manager interface {
	clustmgr.ClusterStateNotifier
	PartitionMapper
	BatchForwarder
	Start() error
	Stop() error
	RegisterListener(listenerName string, listener ProcessorListener) []Processor
	UnregisterListener(listenerName string)
	SetClusterMessageHandlers(remotingServer remoting.Server, vbHandler *remoting.TeeBlockingClusterMessageHandler)
	MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error)
	SetVersionManagerClient(client vmgr.Client)
	HandleVersionBroadcast(currentVersion int, completedVersion int, flushedVersion int)
	GetGroupState(processorID int) (clustmgr.GroupState, bool)
	GetLeaderNode(processorID int) (int, error)
	GetProcessor(processorID int) (Processor, bool)
	ClusterVersion() int
	IsReadyAsOfVersion(clusterVersion int) bool
	HandleClusterState(cs clustmgr.ClusterState) error
	AfterReceiverChange()
	GetCurrentVersion() int
	PrepareForShutdown()
	AcquiesceLevelManagerProcessor() error
	WaitForProcessingToComplete()
	Freeze()
	FailoverOccurred() bool
	VersionManagerClient() vmgr.Client
	EnsureReplicatorsReady() error
	SetLevelMgrProcessorInitialisedCallback(callback func() error)
}

type ProcessorManager struct {
	partitionMapper
	lock                        sync.Mutex
	cfg                         *conf.Config
	store                       *store2.Store
	vMgrClient                  vmgr.Client
	replicatorFactory           ReplicatorFactory
	batchHandlerFactory         BatchHandlerFactory
	levelManagerFlushNotifier   FlushNotifier
	ingestNotifier              IngestNotifier
	stopped                     atomic.Bool
	frozen                      bool
	processors                  sync.Map
	processorNodeMap            sync.Map
	remotingClient              *remoting.Client
	clustStateMgr               clustStateManager
	receiverInfoProvider        ReceiverInfoProvider
	clusterVersion              int64
	stateHandlers               []clustmgr.ClusterStateHandler
	currWriteVersion            int
	lastCompletedVersion        int
	checkIdlePrevLastCompleted  int
	injectableReceiverIDs       map[int][]int
	lastClusterState            *clustmgr.ClusterState
	processorsIdle              bool
	lastInjectedBarrierVersion  int
	lastBarrierInjectionTime    int64
	barrierInjectTimer          *common.TimerHandle
	processorListeners          map[string]ProcessorListener
	procListenersLock           sync.Mutex
	idleCheckTimer              *common.TimerHandle
	syncCheckTimer              *common.TimerHandle
	batchFlushCheckTimer        *common.TimerHandle
	levelManagerFlushCheckTimer *common.TimerHandle
	failoverOccurred            bool
	shutdownVersion             int
	shutdownVersionChannel      chan struct{}
	latestCommandID             int64
	flushCallbacks              []flushCallbackEntry
	lastFlushedVersion          int
	prevFlushedVersion          int
	failure                     *failureHandler
	replBatchFlushDisabled      bool
	replBatchFlushLock          sync.Mutex
	storeFlushedLock            sync.Mutex
	lastMarkedVersion           int
	failureEnabled              bool
	levelMgrInitialisedCallback func() error
	wfpCalled                   bool
}

type BatchForwarder interface {
	ForwardBatch(batch *ProcessBatch, replicate bool, completionFunc func(error))
}

type ProcessorListener func(processor Processor, started bool, promoted bool)

type clustStateManager interface {
	SetClusterStateHandler(clustmgr.ClusterStateHandler)
	Start() error
	Halt() error
	Stop() error
	MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error)
}

type ReplicatorFactory func(id int, cfg *conf.Config, processor Processor, manager Manager,
	joinedClusterVersion int) Replicator

type Replicator interface {
	ReplicateBatch(processBatch *ProcessBatch, completionFunc func(error))
	InitialiseLeader(forceReprocess bool) error
	SetLeader(sufficientReplicas bool, clusterVersion int)
	MarkProcessedSequenceCheckpoint()
	ResetProcessedSequences()
	FlushBatchesFromCheckpoint()
	Acquiesce()
	Resume()
	MaybeReprocessQueue(lastFlushedVersion int) error
	Stop()
	GetReplicatedBatchCount() int
	SetInitialisedCallback(callback func() error)
	CheckSync()
}

type BatchHandlerFactory func(processorID int) BatchHandler

type FlushNotifier interface {
	AddFlushedCallback(func(err error))
}

type IngestNotifier interface {
	StopIngest() error
	StartIngest(version int) error
}

func NewProcessorManager(clustStateMgr clustStateManager, receiverInfoProvider ReceiverInfoProvider,
	store *store2.Store, cfg *conf.Config, replicatorFactory ReplicatorFactory, batchHandlerFactory BatchHandlerFactory,
	levelManagerFlushNotifier FlushNotifier, ingestNotififer IngestNotifier) Manager {
	return NewProcessorManagerWithFailure(clustStateMgr, receiverInfoProvider, store, cfg, replicatorFactory, batchHandlerFactory,
		levelManagerFlushNotifier, ingestNotififer, false)
}

func NewProcessorManagerWithFailure(clustStateMgr clustStateManager, receiverInfoProvider ReceiverInfoProvider,
	store *store2.Store, cfg *conf.Config, replicatorFactory ReplicatorFactory, batchHandlerFactory BatchHandlerFactory,
	levelManagerFlushNotifier FlushNotifier, ingestNotififer IngestNotifier, failureEnabled bool) Manager {
	vmgrClient := NewVmgrClient(cfg)
	pm := NewProcessorManagerWithVmgrClient(clustStateMgr, receiverInfoProvider, store, cfg, replicatorFactory,
		batchHandlerFactory, levelManagerFlushNotifier, ingestNotififer, failureEnabled, vmgrClient)
	vmgrClient.SetProcessorManager(pm)
	return pm
}

func NewProcessorManagerWithVmgrClient(clustStateMgr clustStateManager, receiverInfoProvider ReceiverInfoProvider,
	store *store2.Store, cfg *conf.Config, replicatorFactory ReplicatorFactory, batchHandlerFactory BatchHandlerFactory,
	levelManagerFlushNotifier FlushNotifier, ingestNotififer IngestNotifier, failureEnabled bool, vmgrClient vmgr.Client) *ProcessorManager {
	pm := &ProcessorManager{
		cfg:                        cfg,
		clustStateMgr:              clustStateMgr,
		store:                      store,
		replicatorFactory:          replicatorFactory,
		batchHandlerFactory:        batchHandlerFactory,
		levelManagerFlushNotifier:  levelManagerFlushNotifier,
		ingestNotifier:             ingestNotififer,
		receiverInfoProvider:       receiverInfoProvider,
		injectableReceiverIDs:      map[int][]int{},
		processorListeners:         map[string]ProcessorListener{},
		failureEnabled:             failureEnabled,
		vMgrClient:                 vmgrClient,
		clusterVersion:             -1,
		currWriteVersion:           -1,
		lastCompletedVersion:       -1,
		checkIdlePrevLastCompleted: -1,
		lastBarrierInjectionTime:   -1,
		lastInjectedBarrierVersion: -1,
		prevFlushedVersion:         -1,
		lastMarkedVersion:          -1,
		shutdownVersion:            -1,
	}
	pm.failure = newFailureHandler(pm)
	store.SetVersionFlushedHandler(pm.storeFlushed)
	return pm
}

func (m *ProcessorManager) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.isStopped() {
		panic("cannot be restarted")
	}
	m.invalidatePartitionMappings()
	remotingClient := remoting.NewClient(m.cfg.ClusterTlsConfig)
	m.remotingClient = remotingClient
	m.getInitialVersions()
	m.scheduleIdleCheck()
	m.scheduleSyncCheck()
	m.scheduleBatchFlushCheck(true)
	if *m.cfg.LevelManagerEnabled {
		m.scheduleLevelManagerFlushCheck(true)
	}
	m.failure.start()
	return nil
}

func (m *ProcessorManager) isStopped() bool {
	return m.stopped.Load()
}

func (m *ProcessorManager) scheduleIdleCheck() {
	//m.idleCheckTimer = common.ScheduleTimer(m.cfg.IdleProcessorCheckInterval, false, func() {
	//	m.checkIdleProcessors()
	//	m.lock.Lock()
	//	defer m.lock.Unlock()
	//	m.scheduleIdleCheck()
	//})
}

func (m *ProcessorManager) scheduleSyncCheck() {
	m.syncCheckTimer = common.ScheduleTimer(2*time.Second, false, func() {
		m.checkSyncProcessors()
		m.lock.Lock()
		defer m.lock.Unlock()
		m.scheduleSyncCheck()
	})
}

func (m *ProcessorManager) checkSyncProcessors() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.isStopped() {
		return
	}
	m.processors.Range(func(_, value any) bool {
		processor := value.(Processor)
		if processor.IsLeader() {
			repli := processor.GetReplicator()
			if repli != nil {
				repli.CheckSync()
			}
		}
		return true
	})
}

// EnsureReplicatorsReady ensures that all replicators have completed initialising and syncing and are available
// to replicate.
func (m *ProcessorManager) EnsureReplicatorsReady() error {
	for i := 0; i < *m.cfg.ProcessorCount; i++ {
		start := time.Now()
		for {
			batch := NewProcessBatch(i, nil, common.DummyReceiverID, 0, -1)
			ch := make(chan error, 1)
			m.ForwardBatch(batch, true, func(err error) {
				ch <- err
			})
			err := <-ch
			if err == nil {
				break
			}
			if !common.IsUnavailableError(err) {
				return err
			}
			time.Sleep(10 * time.Millisecond)
			dur := time.Now().Sub(start)
			if dur >= 5*time.Second {
				log.Warnf("waiting for replicators to be ready, returned err: %v", err)
			}
			if dur >= 5*time.Minute {
				return errors.New("timed out waiting for replicators to be ready")
			}
		}
	}
	return nil
}

func (m *ProcessorManager) SetLevelMgrProcessorInitialisedCallback(callback func() error) {
	m.levelMgrInitialisedCallback = callback
}

func (m *ProcessorManager) scheduleBatchFlushCheck(first bool) {
	if m.batchFlushCheckTimer != nil {
		panic("batch flush check timer already scheduled")
	}
	m.batchFlushCheckTimer = common.ScheduleTimer(*m.cfg.BatchFlushCheckInterval, first, m.checkFlushBatches)
}

func (m *ProcessorManager) scheduleLevelManagerFlushCheck(first bool) {
	if m.levelManagerFlushCheckTimer != nil {
		panic("level mgr batch flush check timer already scheduled")
	}
	m.levelManagerFlushCheckTimer = common.ScheduleTimer(*m.cfg.BatchFlushCheckInterval, first, m.checkFlushLevelManagerBatches)
}

func (m *ProcessorManager) Stop() error {
	return m.stop(false)
}

func (m *ProcessorManager) Halt() error {
	return m.stop(true)
}

func (m *ProcessorManager) stop(halt bool) error {
	if m.isStopped() {
		return nil
	}
	m.stopped.Store(true)
	m.lock.Lock()
	m.failure.stop()
	// We stop the timers without waiting for them to complete to avoid deadlock as some of the timers can hold the
	// manager lock
	if m.barrierInjectTimer != nil {
		m.barrierInjectTimer.Stop()
	}
	if m.idleCheckTimer != nil {
		m.idleCheckTimer.Stop()
	}
	if m.batchFlushCheckTimer != nil {
		m.batchFlushCheckTimer.Stop()
	}
	if m.levelManagerFlushCheckTimer != nil {
		m.levelManagerFlushCheckTimer.Stop()
	}
	if m.syncCheckTimer != nil {
		m.syncCheckTimer.Stop()
	}
	bit := m.barrierInjectTimer
	ict := m.idleCheckTimer
	bft := m.batchFlushCheckTimer
	lft := m.levelManagerFlushCheckTimer
	sct := m.syncCheckTimer
	m.lock.Unlock()
	// And we wait for the timers to complete without the lock held
	if bit != nil {
		bit.WaitComplete()
	}
	if ict != nil {
		ict.WaitComplete()
	}
	if bft != nil {
		bft.WaitComplete()
	}
	if lft != nil {
		lft.WaitComplete()
	}
	if sct != nil {
		sct.WaitComplete()
	}
	m.remotingClient.Stop()
	//goland:noinspection GoUnhandledErrorResult
	m.vMgrClient.Stop()
	if halt {
		if err := m.clustStateMgr.Halt(); err != nil {
			log.Warnf("failed to stop cluster state mgr %v", err)
		}
	} else {
		if err := m.clustStateMgr.Stop(); err != nil {
			log.Warnf("failed to stop cluster state mgr %v", err)
		}
	}
	var err error
	m.processors.Range(func(key, value any) bool {
		processor := value.(Processor) //nolint:forcetypeassert
		processor.Stop()
		return true
	})
	log.Debugf("stopped processor manager is now stopped on node %d", *m.cfg.NodeID)
	return err
}

// SetVersionManagerClient - Used in tests to override the default client
func (m *ProcessorManager) SetVersionManagerClient(client vmgr.Client) {
	m.vMgrClient = client
}

func (m *ProcessorManager) GetGroupState(processorID int) (clustmgr.GroupState, bool) {
	o, ok := m.processorNodeMap.Load(processorID)
	if !ok {
		return clustmgr.GroupState{}, false
	}
	return o.(clustmgr.GroupState), true
}

func (m *ProcessorManager) RegisterListener(listenerName string, listener ProcessorListener) []Processor {
	m.procListenersLock.Lock()
	defer m.procListenersLock.Unlock()
	m.processorListeners[listenerName] = listener

	var processors []Processor
	m.processors.Range(func(key, value any) bool {
		proc := value.(Processor)
		if proc.IsLeader() {
			processors = append(processors, proc)
		}
		return true
	})
	return processors
}

func (m *ProcessorManager) UnregisterListener(listenerName string) {
	m.procListenersLock.Lock()
	defer m.procListenersLock.Unlock()
	delete(m.processorListeners, listenerName)
}

func (m *ProcessorManager) MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error) {
	return m.clustStateMgr.MarkGroupAsValid(nodeID, groupID, joinedVersion)
}

func (m *ProcessorManager) GetLeaderNode(processorID int) (int, error) {
	o, ok := m.processorNodeMap.Load(processorID)
	if !ok {
		// This can occur if cluster not ready - client will retry, and it will resolve
		return 0, errors.WithStack(errors.NewTektiteErrorf(errors.Unavailable,
			"no processor available when getting leader node for group %d", processorID))
	}
	groupState := o.(clustmgr.GroupState) //nolint:forcetypeassert
	for _, groupNode := range groupState.GroupNodes {
		if groupNode.Leader {
			return groupNode.NodeID, nil
		}
	}
	panic("group has no leader")
}

func (m *ProcessorManager) GetProcessor(processorID int) (Processor, bool) {
	o, ok := m.processors.Load(processorID)
	if !ok {
		return nil, false
	}
	return o.(Processor), true
}

func (m *ProcessorManager) getInitialVersions() {
	// We call this explicitly on startup - it makes things a bit quicker as don't have to wait for first periodic
	// broadcast from version manager
	common.Go(func() {
		// This will block until the version manager is available
		currentVersion, lastCompleted, lastFlushed, err := m.vMgrClient.GetVersions()
		if err != nil {
			log.Errorf("failed to get current version from vmgr %v", err)
			return
		}
		log.Debugf("node %d proc mgr got initial versions as current:%d completed:%d flushed:%d", *m.cfg.NodeID, currentVersion,
			lastCompleted, lastFlushed)
		m.setVersions(currentVersion, lastCompleted, lastFlushed)
		if err := m.ingestNotifier.StartIngest(lastFlushed); err != nil {
			log.Errorf("failed to start ingest at startup with version %d", lastFlushed)
		}
	})
}

// HandleVersionBroadcast - Called on receipt of current version broadcast
func (m *ProcessorManager) HandleVersionBroadcast(currentVersion int, lastCompleted int, lastFlushed int) {
	log.Debugf("proc mgr got version broadcast cv:%d lc:%d lf:%d", currentVersion, lastCompleted, lastFlushed)
	m.setVersions(currentVersion, lastCompleted, lastFlushed)
}

func (m *ProcessorManager) setVersions(currentVersion int, lastCompleted int, lastFlushed int) {
	m.lock.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			m.lock.Unlock()
		}
	}()
	if m.isStopped() {
		return
	}
	if currentVersion <= m.currWriteVersion {
		// Ignore
		return
	}
	m.currWriteVersion = currentVersion
	lastCompletedIncreased := lastCompleted > m.lastCompletedVersion
	m.lastCompletedVersion = lastCompleted
	// Note that if last completed did not increase (but current version did) that means we are skipping versions
	// - when that happens we do not want to delay versions, we want to do that quickly.
	m.maybeSetVersionsForProcessors(!lastCompletedIncreased)
	if m.shutdownVersion != -1 && m.lastCompletedVersion >= m.shutdownVersion {
		m.shutdownVersionChannel <- struct{}{}
		m.shutdownVersion = -1
	}
	flushCallbacks := m.checkFlush(lastFlushed)
	// callbacks must be called outside lock
	m.lock.Unlock()
	unlocked = true
	for _, cb := range flushCallbacks {
		cb.callback()
	}
}

func (m *ProcessorManager) GetCurrentVersion() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.currWriteVersion
}

func (m *ProcessorManager) maybeSetVersionsForProcessors(doNotDelay bool) {
	if m.lastInjectedBarrierVersion == m.currWriteVersion {
		// Already injected this version
		return
	}
	if m.processorsIdle {
		// All processors idle - don't inject
		return
	}
	if m.barrierInjectTimer != nil {
		// Timer already waiting to inject
		return
	}
	durSinceLastInject := int64(common.NanoTime()) - m.lastBarrierInjectionTime
	if doNotDelay || (m.lastBarrierInjectionTime == -1 || durSinceLastInject >= int64(*m.cfg.MinSnapshotInterval)) {
		// It's been enough time since last injection so inject now, or we're skipping versions
		m.setVersionForProcessors()
	} else {
		// Inject after delay
		delay := int64(*m.cfg.MinSnapshotInterval) - durSinceLastInject
		m.barrierInjectTimer = common.ScheduleTimer(time.Duration(delay), false, func() {
			m.lock.Lock()
			defer m.lock.Unlock()
			m.barrierInjectTimer = nil
			if m.isStopped() {
				return
			}
			m.setVersionForProcessors()
		})
	}
}

// processorsNotIdle is called by a processor when it was idle but has received a batch
// we want to inject barriers if there is one waiting
func (m *ProcessorManager) processorsNotIdle() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.processorsIdle = false
	m.maybeSetVersionsForProcessors(false)
}

func (m *ProcessorManager) checkIdleProcessors() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.isStopped() {
		return
	}

	allIdle := true
	hasProcessors := false
	m.processors.Range(func(_, value any) bool {
		processor := value.(Processor)
		if processor.IsLeader() {
			// We pass in the last completed version from the previous time the check was made, if no new batches
			// have arrived at the processor since then, it is considered idle.
			if !processor.IsIdle(m.checkIdlePrevLastCompleted) {
				allIdle = false
				return false
			}
			hasProcessors = true
		}
		return true
	})

	if allIdle && hasProcessors {
		// All processors are idle - we won't inject barriers while they are idle
		m.processorsIdle = true
		if m.barrierInjectTimer != nil {
			m.barrierInjectTimer.Stop()
			m.barrierInjectTimer = nil
		}
	} else {
		if m.processorsIdle {
			m.processorsIdle = false
			m.maybeSetVersionsForProcessors(false)
		}
	}
	m.checkIdlePrevLastCompleted = m.lastCompletedVersion
}

func (m *ProcessorManager) setVersionForProcessors() {
	m.lastBarrierInjectionTime = int64(common.NanoTime())
	// Set version on all leaders
	m.processors.Range(func(_, value any) bool {
		processor := value.(Processor)
		if processor.IsLeader() {
			m.setVersionForProcessor(processor)
		}
		return true
	})
	m.lastInjectedBarrierVersion = m.currWriteVersion
}

func (m *ProcessorManager) setVersionForProcessor(processor Processor) {
	processorID := processor.ID()

	receiverIDs, ok := m.injectableReceiverIDs[processorID]
	if !ok {
		// Not cached - get it from the ReceiverInfoProvider
		receiverIDs = m.receiverInfoProvider.GetInjectableReceivers(processorID)
		if receiverIDs == nil {
			// empty slice not nil, otherwise won't be cached!
			receiverIDs = []int{}
		}
		m.injectableReceiverIDs[processorID] = receiverIDs
	}
	receiverIDsCopy := make([]int, len(receiverIDs))
	copy(receiverIDsCopy, receiverIDs)
	// We close the version, this will result in barriers being injected
	processor.CloseVersion(m.currWriteVersion, receiverIDsCopy)
}

func (m *ProcessorManager) AfterReceiverChange() {
	m.lock.Lock()
	defer m.lock.Unlock()
	log.Debugf("node %d AfterReceiverChange called", *m.cfg.NodeID)
	// We invalidate any cached receiver info here and in processors
	m.injectableReceiverIDs = map[int][]int{}
	// Invalidate the partition mappings
	m.invalidatePartitionMappings()

	// Invalidate cached receiver info in the processors
	m.processors.Range(func(key, value any) bool {
		processor := value.(*processor)
		if processor.IsLeader() {
			processor.InvalidateCachedReceiverInfo()
		}
		return true
	})

	if m.currWriteVersion == -1 {
		// This can happen if called right after startup, ok since nothing will be cached yet anyway
		return
	}
	// doom currently in progress versions as its possible versions could get stuck
	m.doomVersion(m.currWriteVersion + 1)
}

func (m *ProcessorManager) doomVersion(doomedVersion int) {
	m.vMgrClient.VersionComplete(doomedVersion, -1, -1, true, func(err error) {
		if err == nil {
			return
		}
		if !common.IsUnavailableError(err) {
			log.Errorf("failed to doom version %v", err)
			return
		}
		// Retry when unavailable
		common.ScheduleTimer(1*time.Second, false, func() {
			if m.isStopped() {
				return
			}
			m.doomVersion(doomedVersion)
		})
	})
}

func (m *ProcessorManager) RegisterStateHandler(handler clustmgr.ClusterStateHandler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.stateHandlers = append(m.stateHandlers, handler)
}

func (m *ProcessorManager) callClusterStateHandlers(cs clustmgr.ClusterState) error {
	// Call cluster state handlers. This is done in the manager as the manager lock needs to be held while we're doing
	// this
	for _, handler := range m.stateHandlers {
		if err := handler(cs); err != nil {
			return err
		}
	}
	return nil
}

func (m *ProcessorManager) HandleClusterState(cs clustmgr.ClusterState) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	log.Debugf("node %d received cluster state %v", *m.cfg.NodeID, cs)
	if m.isStopped() {
		log.Debugf("node %d is stopped", *m.cfg.NodeID)

		// We must ensure that no cluster states occur after processor manager is stopped - this can cause
		// cluster version to be updated on the store which could allow memtables to be pushed to store
		// after a new leader is active
		return nil
	}
	if m.frozen {
		log.Debugf("node %d is frozen", *m.cfg.NodeID)

		// The cluster is about to be shutdown - we do not want processors failing over when we finally shut them down
		return nil
	}
	if err := m.callClusterStateHandlers(cs); err != nil {
		return err
	}
	newState := map[int][]clustmgr.GroupNode{}
	for processorID, nids := range cs.GroupStates {
		newState[processorID] = nids
	}

	for processorID, gs := range newState {
		if err := m.processGroupState(processorID, gs, cs.Version); err != nil {
			return err
		}
	}

	var err error
	m.processors.Range(func(k, v any) bool {
		processorID := k.(int) //nolint:forcetypeassert
		proc := v.(Processor)  //nolint:forcetypeassert
		groupState, ok := newState[processorID]
		found := false
		if ok {
			for _, groupNode := range groupState {
				if groupNode.NodeID == *m.cfg.NodeID {
					found = true
					break
				}
			}
		}
		if !found {
			proc.Stop()
			m.processors.Delete(processorID)
			m.processorNodeMap.Delete(processorID)
		}
		return true
	})
	if err != nil {
		return err
	}
	m.invalidatePartitionMappings()

	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		currClusterVersion := int(atomic.LoadInt64(&m.clusterVersion))
		if cs.Version <= currClusterVersion {
			panic("received cluster version <= previous cluster version")
		}
	}

	// the node could have fallen out of the cluster,
	// but we still could have received the cluster state, without this node in it.
	// in this case we don't want to set the cluster version here, it's used when executing queries, so we return
	// before that
	inCluster := false
	for _, gn := range cs.GroupStates[0] {
		if gn.NodeID == *m.cfg.NodeID {
			inCluster = true
			break
		}
	}
	if !inCluster {
		log.Debugf("node %d not in cluster", *m.cfg.NodeID)
		return nil
	}

	atomic.StoreInt64(&m.clusterVersion, int64(cs.Version))

	if m.lastClusterState != nil && m.failureEnabled {
		if err := m.failure.maybeRunFailure(&cs); err != nil {
			return err
		}
	} else {
		log.Debugf("node %d not calling maybeRunFailure as has lastclusterstate %t failure enabled %t", *m.cfg.NodeID,
			m.lastClusterState != nil, m.failureEnabled)
	}

	m.lastClusterState = &cs
	return err
}

func (m *ProcessorManager) processGroupState(processorID int, gs []clustmgr.GroupNode, clusterVersion int) error {
	sufficientReplicas := len(gs) >= *m.cfg.MinReplicas

	if !sufficientReplicas {
		log.Debugf("insufficient replicas (need %d) for group %v ", m.cfg.MinReplicas, gs)
	}

	gsCopy := make([]clustmgr.GroupNode, len(gs))
	copy(gsCopy, gs)
	// We store a map of processorID -> node ids
	// This must be done before the processors are initialised to allow the levelManager to be accessed
	m.processorNodeMap.Store(processorID, clustmgr.GroupState{
		ClusterVersion: clusterVersion,
		GroupNodes:     gsCopy,
	})
	for _, groupNode := range gsCopy {
		if *m.cfg.NodeID == groupNode.NodeID {
			var proc Processor
			o, ok := m.processors.Load(processorID)
			if !ok {
				// create the processor
				batchHandler := m.batchHandlerFactory(processorID)
				proc = NewProcessor(processorID, m.cfg, m.store, m, batchHandler, m.receiverInfoProvider)
				log.Debugf("node %d created new processor %d", *m.cfg.NodeID, processorID)
				proc.SetVersionCompleteHandler(func(version int, requiredCompletions int, commandID int, doom bool, cf func(error)) {
					m.vMgrClient.VersionComplete(version, requiredCompletions, commandID, doom, cf)
				})
				proc.SetNotIdleNotifier(m.processorsNotIdle)
				if m.replicatorFactory != nil {
					standalone := len(m.cfg.ClusterAddresses) == 1
					// create the replicator
					var replicator Replicator
					if standalone {
						replicator = &noopReplicator{proc: proc}
					} else {
						replicator = m.replicatorFactory(processorID, m.cfg, proc, m, groupNode.JoinedVersion)
					}
					if m.isLevelManagerProcessor(processorID) && m.levelMgrInitialisedCallback != nil {
						replicator.SetInitialisedCallback(m.levelMgrInitialisedCallback)
					}
					proc.SetReplicator(replicator)
				}
				m.processors.Store(processorID, proc)
			} else {
				proc = o.(Processor) //nolint:forcetypeassert
			}
			if groupNode.Leader {
				currLeader := proc.IsLeader()
				if !currLeader {
					proc.SetLeader()
				}
				replicator := proc.GetReplicator()
				if replicator != nil {
					var newLeaderVersion int
					if !currLeader {
						newLeaderVersion = clusterVersion
					} else {
						newLeaderVersion = -1
					}
					replicator.SetLeader(sufficientReplicas, newLeaderVersion)
				}
				if !ok {
					// Newly created processor
					if m.currWriteVersion != -1 {
						// We only inject barriers here if the processor is newly created and write version has been set from
						// version manager
						m.setVersionForProcessor(proc)
					}
					m.processorsIdle = false
					log.Debugf("node %d proc mgr calling processor listeners for processor %d",
						*m.cfg.NodeID, proc.ID())
				}
				if !ok || !currLeader {
					// if newly created or processor newly promoted to leader, call the processor listeners
					m.callProcessorListeners(proc, ok)

					if m.isLevelManagerProcessor(processorID) {
						// For level manager we call InitialiseLeader which involves reprocessing replication queue
						// straight away - normally we would wait for first version to arrive, but for level manager
						// we don't need versions in batches, and level manager won't be activated until level manager
						// processor is initialised, so if we waited for version we would wait forever as version manager
						// won't broadcast version until it has loaded last flushed version from level manager which
						// won't return that until it is activated.
						proc.SubmitAction(func() error {
							return proc.GetReplicator().InitialiseLeader(true)
						})
					}
				}
			}
		}
	}
	return nil
}

func (m *ProcessorManager) callProcessorListeners(proc Processor, promoted bool) {
	m.procListenersLock.Lock()
	defer m.procListenersLock.Unlock()
	for _, listener := range m.processorListeners {
		listener(proc, true, promoted)
	}
}

func (m *ProcessorManager) IsReadyAsOfVersion(clusterVersion int) bool {
	currVersion := atomic.LoadInt64(&m.clusterVersion)
	ready := currVersion == -1 || int64(clusterVersion) == currVersion
	if !ready {
		log.Debugf("manager curr cluster version is %d - query requires version %d", currVersion, clusterVersion)
	}
	return ready
}

func (m *ProcessorManager) ClusterVersion() int {
	return int(atomic.LoadInt64(&m.clusterVersion))
}

func (m *ProcessorManager) SetClusterMessageHandlers(remotingServer remoting.Server, vbHandler *remoting.TeeBlockingClusterMessageHandler) {
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageForwardMessage, &forwardMessageHandler{m: m})
	vbHandler.Handlers = append(vbHandler.Handlers, &versionBroadcastMessageHandler{m: m})
}

func (m *ProcessorManager) checkFlushLevelManagerBatches() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.replBatchFlushLock.Lock()
	defer m.replBatchFlushLock.Unlock()
	if m.isStopped() {
		return
	}
	if m.levelManagerFlushCheckTimer == nil {
		panic("timer not set")
	}
	m.levelManagerFlushCheckTimer = nil

	if m.currWriteVersion == -1 || m.replBatchFlushDisabled {
		// Haven't got first version yet or flush is disabled
		m.scheduleLevelManagerFlushCheck(false)
		return
	}

	replicator := m.getLevelManagerReplicator()

	if replicator != nil {
		replicator.MarkProcessedSequenceCheckpoint()
		called := &atomic.Bool{}
		if m.levelManagerFlushNotifier != nil {
			m.levelManagerFlushNotifier.AddFlushedCallback(func(err error) {
				if called.Load() {
					panic("level manager flushed callback called more than once")
				}
				called.Store(true)
				m.lock.Lock()
				defer m.lock.Unlock()
				replicator := m.getLevelManagerReplicator()
				log.Debugf("flushing all level manager replication batches")
				replicator.FlushBatchesFromCheckpoint()
				m.scheduleLevelManagerFlushCheck(false)
			})
		}
	}
}

func (m *ProcessorManager) getLevelManagerReplicator() Replicator {
	p, ok := m.processors.Load(m.cfg.ProcessorCount)
	if !ok {
		return nil
	}
	proc := p.(*processor)
	if !proc.IsLeader() {
		return nil
	}
	return proc.GetReplicator()
}

func (m *ProcessorManager) addFlushedCallback(callback func()) {
	m.flushCallbacks = append(m.flushCallbacks, flushCallbackEntry{
		callback: callback,
		version:  m.currWriteVersion + 1,
	})
}

// called when flushed version broadcast from version manager
func (m *ProcessorManager) checkFlush(lastFlushed int) []flushCallbackEntry {
	var toCall []flushCallbackEntry
	var cb flushCallbackEntry
	for _, cb = range m.flushCallbacks {
		if cb.version <= lastFlushed {
			toCall = append(toCall, cb)
		} else {
			break
		}
	}
	if len(toCall) > 0 {
		m.flushCallbacks = m.flushCallbacks[len(toCall):]
	}
	return toCall
}

func (m *ProcessorManager) storeFlushed(version int) {
	// Note, we use a different lock to the main manager lock here to avoid deadlock where a flush is occurring
	// while we're disabling flush in the store with the manager lock held when detecting a failure
	m.storeFlushedLock.Lock()
	defer m.storeFlushedLock.Unlock()
	if version <= m.prevFlushedVersion {
		return
	}
	cv := atomic.LoadInt64(&m.clusterVersion)
	if cv < 0 {
		// Can get periodic flush before first cluster state arrives
		return
	}
	// The store has flushed all local data to the specified completed version. We can now tell version manager
	// that this has occurred. When all processor managers report in with the same completed version for all processors
	// then flushed version will be set to the specified value and broadcast.

	if err := m.vMgrClient.VersionFlushed(*m.cfg.NodeID, version, m.liveProcessorCount(), int(cv)); err != nil {
		log.Errorf("failed to call VersionFlushed %v", err)
	}
	log.Debugf("node %d called version flushed:%d", *m.cfg.NodeID, version)
	m.prevFlushedVersion = version
}

func (m *ProcessorManager) liveProcessorCount() int {
	liveProcessorCount := 0
	m.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if proc.IsLeader() {
			liveProcessorCount++
		}
		return true
	})
	return liveProcessorCount
}

type flushCallbackEntry struct {
	callback func()
	version  int
}

func (m *ProcessorManager) checkFlushBatches() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.replBatchFlushLock.Lock()
	defer m.replBatchFlushLock.Unlock()
	if m.isStopped() {
		return
	}
	if m.batchFlushCheckTimer == nil {
		panic("timer not set")
	}
	m.batchFlushCheckTimer = nil

	if m.lastMarkedVersion != -1 {
		panic(fmt.Sprintf("node %d foo schedule check batches called but version already marked", *m.cfg.NodeID))
	}

	if m.currWriteVersion == -1 || m.replBatchFlushDisabled {
		// Haven't got first version yet or flush is disabled
		m.scheduleBatchFlushCheck(false)
		return
	}

	// First we call each replicator and ask it to mark the value of the last processed batch sequence
	hasMarked := false
	m.processors.Range(func(key, value any) bool {
		processor := value.(*processor)
		if m.isLevelManagerProcessor(processor.id) {
			// level-manager processor - ignore
			return true
		}
		if processor.IsLeader() {
			replicator := processor.GetReplicator()
			if replicator != nil {
				replicator.MarkProcessedSequenceCheckpoint()
				hasMarked = true
			}
		}
		return true
	})
	if !hasMarked {
		m.scheduleBatchFlushCheck(false)
		return
	}
	fv := m.currWriteVersion + 1

	m.lastMarkedVersion = fv

	// At this point we know that any processed batches will have been assigned a version <= currWriteVersion + 1
	// Then we add callback that will be called when currWriteVersion + 1 has been fully flushed to cloud across the cluster.
	// Then we can remove flushed batches from replication queues.

	called := &atomic.Bool{}

	m.addFlushedCallback(func() {
		if called.Load() {
			panic("callback called more than once")
		}
		called.Store(true)
		m.lock.Lock()
		defer m.lock.Unlock()
		// Now we know all writes from the marked checkpoint have been flushed, we can tell the replicators to
		// replicate a flush message
		m.processors.Range(func(key, value any) bool {
			processor := value.(*processor)
			if m.isLevelManagerProcessor(processor.id) {
				return true
			}
			if processor.IsLeader() {
				replicator := processor.GetReplicator()
				if replicator != nil {
					replicator.FlushBatchesFromCheckpoint()
				}
			}
			return true
		})
		m.lastMarkedVersion = -1
		m.scheduleBatchFlushCheck(false)
	})
}

type versionBroadcastMessageHandler struct {
	m *ProcessorManager
}

func (v *versionBroadcastMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	versionMessage := messageHolder.Message.(*clustermsgs.VersionsMessage)
	v.m.HandleVersionBroadcast(int(versionMessage.CurrentVersion), int(versionMessage.CompletedVersion),
		int(versionMessage.FlushedVersion))
	return nil, nil
}

func (m *ProcessorManager) PrepareForShutdown() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.failoverOccurred = false

	// First we must stop any new replications and wait for all in-progress replications to complete
	// Note, this does not acquiesce level manager replicator - so level manager commands can still be handled
	m.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if m.isLevelManagerProcessor(proc.id) {
			// level manager - ignore
			// if we acquiesce the level manager here too then registering of dead version range won't work
			return true
		}
		m.acquiesceProcessor(proc)
		return true
	})
}

func (m *ProcessorManager) AcquiesceLevelManagerProcessor() error {
	if *m.cfg.LevelManagerEnabled {
		p, ok := m.processors.Load(m.cfg.ProcessorCount)
		if !ok {
			return errors.New("cannot find level manager processor")
		}
		proc := p.(*processor)
		m.acquiesceProcessor(proc)
	}
	return nil
}

func (m *ProcessorManager) acquiesceProcessor(proc *processor) {
	if proc.IsLeader() {
		repli := proc.GetReplicator()
		if repli != nil {
			repli.Acquiesce()
		}
		proc.prepareForShutdown()
	}
}

func (m *ProcessorManager) WaitForProcessingToComplete() {
	// Now we must wait for at least the current version and the version after that to complete - this ensures all data has been
	// processed
	m.lock.Lock()
	if m.wfpCalled {
		// Already been called - ignore
		// This can happen if shutdown call is retried after transient failure
		m.lock.Unlock()
		return
	}
	m.wfpCalled = true

	m.shutdownVersion = m.currWriteVersion + 2
	log.Debugf("%p node %d waiting for at least version %d to be complete", m, *m.cfg.NodeID, m.shutdownVersion)
	m.shutdownVersionChannel = make(chan struct{}, 1)
	shutdownVer := m.shutdownVersion
	m.lock.Unlock()
	<-m.shutdownVersionChannel
	log.Debugf("%p node %d waited for version %d to be complete", m, *m.cfg.NodeID, shutdownVer)
	m.lock.Lock()
	defer m.lock.Unlock()
}

func (m *ProcessorManager) FailoverOccurred() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.failoverOccurred
}

func (m *ProcessorManager) Freeze() {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Prevent any cluster updates being handled - this is called before clean shutdown - when we finally shut down
	// the servers we don't want them failing over as they are shutdown
	m.frozen = true
}

func (m *ProcessorManager) VersionManagerClient() vmgr.Client {
	return m.vMgrClient
}

func (m *ProcessorManager) disableReplBatchFlush(disabled bool) {
	// Note we use a separate lock for this as we don't want to lock proc mgr here as can otherwise cause deadlock where
	// cluster update is being handled at same time
	m.replBatchFlushLock.Lock()
	defer m.replBatchFlushLock.Unlock()
	m.replBatchFlushDisabled = disabled
}

func (m *ProcessorManager) isLevelManagerProcessor(processorID int) bool {
	return *m.cfg.LevelManagerEnabled && processorID == *m.cfg.ProcessorCount
}

type noopReplicator struct {
	proc         Processor
	initCallback func() error
	initialised  bool
}

func (n *noopReplicator) CheckSync() {
}

func (n *noopReplicator) SetInitialisedCallback(callback func() error) {
	n.initCallback = callback
}

func (n *noopReplicator) ResetProcessedSequences() {
}

func (n *noopReplicator) MaybeReprocessQueue(int) error {
	return nil
}

func (n *noopReplicator) ReplicateBatch(processBatch *ProcessBatch, completionFunc func(error)) {
	processBatch.ReplSeq = -2 // means ignore dedup
	n.proc.SubmitAction(func() error {
		n.proc.ProcessBatch(processBatch, completionFunc)
		return nil
	})
}

func (n *noopReplicator) InitialiseLeader(_ bool) error {
	n.proc.CheckInProcessorLoop()
	if n.initialised {
		return nil
	}
	if n.initCallback == nil {
		return nil
	}
	for {
		err := n.initCallback()
		if err != nil && !common.IsUnavailableError(err) {
			return err
		}
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.initialised = true
	return nil
}

func (n *noopReplicator) SetLeader(bool, int) {
}

func (n *noopReplicator) MarkProcessedSequenceCheckpoint() {
}

func (n *noopReplicator) FlushBatchesFromCheckpoint() {
}

func (n *noopReplicator) Acquiesce() {
}

func (n *noopReplicator) Resume() {
}

func (n *noopReplicator) Stop() {
}

func (n *noopReplicator) GetReplicatedBatchCount() int {
	return 0
}
