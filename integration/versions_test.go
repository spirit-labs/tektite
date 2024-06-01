package integration

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sequence"
	"github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

/*
TestSnapshotVersions tests for snapshot version consistency.
It sets up 3 nodes and 12 processors, with 4 receivers:
receiver 1000 forwards to receiver 2000 which forwards to receivers 3000 and 4000.
We inject batches at 1000 until at least 10 versions have completed.
We then gather all the received batches across all processors and verify that the version is monotonically increasing
as the batch ids increase, and that the batches received on the terminal receivers are identical.
*/
func TestSnapshotVersions(t *testing.T) {
	t.Parallel()
	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()

	seqMgr := sequence.NewInMemSequenceManager()

	remotingAddresses := []string{fmt.Sprintf("localhost:%d", testutils.PortProvider.GetPort(t)),
		fmt.Sprintf("localhost:%d", testutils.PortProvider.GetPort(t)),
		fmt.Sprintf("localhost:%d", testutils.PortProvider.GetPort(t))}

	schema := evbatch.NewEventSchema([]string{"id"}, []types.ColumnType{types.ColumnTypeInt})
	numProcessors := 12
	var allProcessors []int
	for i := 0; i < numProcessors; i++ {
		allProcessors = append(allProcessors, i)
	}
	numProcMgrs := 3
	var mgrs []proc.Manager
	var handlers []*handler
	var remotingServers []remoting.Server
	var clustStateMgrs []*testClustStateMgr
	var vmgrs []*vmgr.VersionManager
	/*
		We have 4 receivers:

		1000->2000--->3000
		           |-->4000
	*/
	for i := 0; i < numProcMgrs; i++ {
		clustStateMgr := &testClustStateMgr{}
		clustStateMgrs = append(clustStateMgrs, clustStateMgr)
		hndlr := newHandler()
		hndlr.forwardingInfos = map[int][]*barrierForwardingInfo{
			1000: {
				{
					forwardReceiverID: 2000,
					processorIDs:      allProcessors,
				},
			},
			2000: {
				{
					forwardReceiverID: 3000,
					processorIDs:      allProcessors,
				},
				{
					forwardReceiverID: 4000,
					processorIDs:      allProcessors,
				},
			},
		}
		hndlr.numProcessors = numProcessors
		hndlr.requiredCompletions = 2 * numProcessors
		hndlr.schema = schema
		handlers = append(handlers, hndlr)
		cfg := &conf.Config{}
		cfg.ApplyDefaults()
		cfg.NodeID = &i
		cfg.ClusterAddresses = remotingAddresses
		mgr := proc.NewProcessorManager(clustStateMgr, hndlr, st, cfg, nil, func(processorID int) proc.BatchHandler {
			return hndlr
		}, nil, &testIngestNotifier{})
		err := mgr.Start()
		require.NoError(t, err)
		mgrs = append(mgrs, mgr)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := mgr.Stop()
			require.NoError(t, err)
		}()
		clustStateMgr.SetClusterStateHandler(mgr.HandleClusterState)

		lMgrClient := &testLevelMgrClient{lastFlushedVersion: -1}

		vMgr := vmgr.NewVersionManager(seqMgr, lMgrClient, cfg, remotingAddresses...)
		mgr.RegisterStateHandler(vMgr.HandleClusterState)
		vmgrs = append(vmgrs, vMgr)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := vMgr.Stop()
			require.NoError(t, err)
		}()

		remotingServer := remoting.NewServer(remotingAddresses[i], conf.TLSConfig{})
		err = remotingServer.Start()
		require.NoError(t, err)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := remotingServer.Stop()
			require.NoError(t, err)
		}()
		remotingServers = append(remotingServers, remotingServer)
		teeHandler := &remoting.TeeBlockingClusterMessageHandler{}
		mgr.SetClusterMessageHandlers(remotingServer, teeHandler)
		remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageVersionsMessage, teeHandler)

		vMgr.SetClusterMessageHandlers(remotingServer)
	}

	cs := clustmgr.ClusterState{
		Version: 23,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 1},
			},
		},
	}

	for _, sm := range clustStateMgrs {
		err := sm.sendClusterState(cs)
		require.NoError(t, err)
	}

	vmgrLeader, err := mgrs[0].GetLeaderNode(vmgr.VersionManagerProcessorID)
	require.NoError(t, err)
	vMgr := vmgrs[vmgrLeader]

	// wait for it to activate
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return vMgr.IsActive(), nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Wait for processors to get initial barrier injected and that version to complete
	waitForVersionToComplete(t, vMgr)

	sentBatches := make([][]*proc.ProcessBatch, numProcessors)

	// Process batches until the required number of versions have been completed
	i := 0
	requiredVersions := 10
	startVersion, _, _, err := vMgr.GetVersions()
	require.NoError(t, err)
	for {
		procID := i % numProcessors

		processorSentBatches := sentBatches[procID]

		mgr1 := mgrs[i%numProcMgrs]
		leaderNode, err := mgr1.GetLeaderNode(procID)
		require.NoError(t, err)
		injectMgr := mgrs[leaderNode]
		processor, ok := injectMgr.GetProcessor(procID)
		require.True(t, ok)
		require.NotNil(t, processor)

		colBuilders := evbatch.CreateColBuilders(schema.ColumnTypes())
		colBuilders[0].(*evbatch.IntColBuilder).Append(int64(i))
		evBatch := evbatch.NewBatchFromBuilders(schema, colBuilders...)

		batch := proc.NewProcessBatch(processor.ID(), evBatch, 1000, 0, -1)
		processor.IngestBatch(batch, func(err error) {
			if err != nil {
				panic(err)
			}
		})
		processorSentBatches = append(processorSentBatches, batch)

		i++

		ver, _, _, err := vMgr.GetVersions()
		require.NoError(t, err)
		if ver-startVersion >= requiredVersions {
			break
		}

		// Slow down the ingest so we make sure several versions have been completed
		time.Sleep(1 * time.Millisecond)
	}

	// Wait for the current in-progress versions to complete to make sure all ingests have been processed
	waitForVersionToComplete(t, vMgr)

	type idVersionPair struct {
		id      int
		version int
	}

	// We repeat this for each of the batches injected at a processor
	for _, procSentBatches := range sentBatches {
		// Put the ids in a set for quick lookup
		sentIDs := make(map[int]struct{}, len(procSentBatches))
		for _, batch := range procSentBatches {
			id := batch.EvBatch.GetIntColumn(0).Get(0)
			sentIDs[int(id)] = struct{}{}
		}

		// Gather batches from 3000

		var pairs3000 []idVersionPair
		for i := 0; i < numProcMgrs; i++ {
			hndlr := handlers[i]
			received := hndlr.receivedBatches[3000]
			for _, batch := range received {
				id := batch.EvBatch.GetIntColumn(0).Get(0)
				_, ok := sentIDs[int(id)]
				if ok {
					pairs3000 = append(pairs3000, idVersionPair{
						id:      int(id),
						version: batch.Version,
					})
				}
			}
		}

		// Sort by id
		sort.SliceStable(pairs3000, func(i, j int) bool {
			return pairs3000[i].id < pairs3000[j].id
		})

		// Gather batches from 4000

		var pairs4000 []idVersionPair
		for i := 0; i < numProcMgrs; i++ {
			hndlr := handlers[i]
			received := hndlr.receivedBatches[4000]
			for _, batch := range received {
				id := batch.EvBatch.GetIntColumn(0).Get(0)
				_, ok := sentIDs[int(id)]
				if ok {
					pairs4000 = append(pairs4000, idVersionPair{
						id:      int(id),
						version: batch.Version,
					})
				}
			}
		}

		// Sort by id
		sort.SliceStable(pairs4000, func(i, j int) bool {
			return pairs4000[i].id < pairs4000[j].id
		})

		// 3000 and 4000 received should be identical
		require.Equal(t, len(pairs3000), len(pairs4000))
		for i, p := range pairs3000 {
			q := pairs4000[i]
			require.Equal(t, p.id, q.id)
			require.Equal(t, p.version, q.version)
		}

		// If the snapshots are consistent then the versions of the batches when arranged in ingest order will
		// be monotonically increasing.
		lastVersion := -1
		firstVersion := -1
		for _, pair := range pairs3000 {
			require.GreaterOrEqual(t, pair.version, lastVersion)
			lastVersion = pair.version
			if firstVersion == -1 {
				firstVersion = pair.version
			}
		}
	}

}

func waitForVersionToComplete(t *testing.T, vMgr *vmgr.VersionManager) {
	currVersion, _, _, err := vMgr.GetVersions()
	require.NoError(t, err)
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lastCompleted, err := vMgr.GetLastCompletedVersion()
		if err != nil {
			return false, err
		}
		return lastCompleted == currVersion+1, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

type handler struct {
	lock                sync.Mutex
	numProcessors       int
	requiredCompletions int
	receivedBatches     map[int][]*proc.ProcessBatch
	forwardingInfos     map[int][]*barrierForwardingInfo
	schema              *evbatch.EventSchema
}

func newHandler() *handler {
	return &handler{
		receivedBatches: map[int][]*proc.ProcessBatch{},
	}
}

type barrierForwardingInfo struct {
	lock              sync.Mutex
	forwardReceiverID int
	processorIDs      []int
}

func (h *handler) GetForwardingProcessorCount(int) (int, bool) {
	return h.numProcessors, true
}

func (h *handler) GetTerminalReceiverCount() int {
	return 0
}

func (h *handler) GetInjectableReceivers(int) []int {
	return []int{1000}
}

func (h *handler) GetRequiredCompletions() int {
	return h.requiredCompletions
}

func (h *handler) getReceivedBatches(receiverID int) []*proc.ProcessBatch {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.receivedBatches[receiverID]
}

func (h *handler) HandleProcessBatch(processor proc.Processor, processBatch *proc.ProcessBatch, _ bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	if !processBatch.Barrier {
		processBatch.CheckDeserializeEvBatch(h.schema)
		h.lock.Lock()
		h.receivedBatches[processBatch.ReceiverID] = append(h.receivedBatches[processBatch.ReceiverID], processBatch)
		h.lock.Unlock()
	}
	if h.forwardingInfos != nil {
		forwardInfos, ok := h.forwardingInfos[processBatch.ReceiverID]
		if ok {
			// Note a stream can forward to multiple receivers, e.g. in the case of child streams which have
			// partitions
			var fBatches []*proc.ProcessBatch
			for _, info := range forwardInfos {
				if processBatch.Barrier {
					for _, forwardProcessorID := range info.processorIDs {
						fBatch := proc.NewBarrierProcessBatch(forwardProcessorID, info.forwardReceiverID, processBatch.Version, -1, processor.ID(), processBatch.CommandID)
						fBatches = append(fBatches, fBatch)
					}
				} else {
					// choose one to forward to randomly
					randIndex := rand.Intn(len(info.processorIDs))
					forwardProcessorID := info.processorIDs[randIndex]
					fBatch := proc.NewProcessBatch(forwardProcessorID, processBatch.EvBatch, info.forwardReceiverID, 0, processor.ID())
					fBatches = append(fBatches, fBatch)
				}
			}
			return true, mem.NewBatch(), fBatches, nil
		}
	}
	return true, mem.NewBatch(), nil, nil
}

type testClustStateMgr struct {
	handler func(state clustmgr.ClusterState) error
}

func (t *testClustStateMgr) MarkGroupAsValid(int, int, int) (bool, error) {
	return true, nil
}

func (t *testClustStateMgr) sendClusterState(state clustmgr.ClusterState) error {
	return t.handler(state)
}

func (t *testClustStateMgr) SetClusterStateHandler(handler clustmgr.ClusterStateHandler) {
	t.handler = handler
}

func (t *testClustStateMgr) Start() error {
	return nil
}

func (t *testClustStateMgr) Stop() error {
	return nil
}

func (t *testClustStateMgr) Halt() error {
	return nil
}

type testLevelMgrClient struct {
	lock               sync.Mutex
	lastFlushedVersion int64
}

func (t *testLevelMgrClient) GetStats() (levels.Stats, error) {
	return levels.Stats{}, nil
}

func (t *testLevelMgrClient) GetTableIDsForRange([]byte, []byte) (levels.OverlappingTableIDs, uint64, []levels.VersionRange, error) {
	return nil, 0, nil, nil
}

func (t *testLevelMgrClient) GetPrefixRetentions() ([]retention.PrefixRetention, error) {
	return nil, nil
}

func (t *testLevelMgrClient) RegisterL0Tables(levels.RegistrationBatch) error {
	return nil
}

func (t *testLevelMgrClient) ApplyChanges(levels.RegistrationBatch) error {
	return nil
}

func (t *testLevelMgrClient) RegisterPrefixRetentions([]retention.PrefixRetention) error {
	return nil
}

func (t *testLevelMgrClient) PollForJob() (*levels.CompactionJob, error) {
	return nil, nil
}

func (t *testLevelMgrClient) RegisterDeadVersionRange(levels.VersionRange, string, int) error {
	return nil
}

func (t *testLevelMgrClient) StoreLastFlushedVersion(version int64) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.lastFlushedVersion = version
	return nil
}

func (t *testLevelMgrClient) LoadLastFlushedVersion() (int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.lastFlushedVersion, nil
}

func (t *testLevelMgrClient) Start() error {
	return nil
}

func (t *testLevelMgrClient) Stop() error {
	return nil
}
