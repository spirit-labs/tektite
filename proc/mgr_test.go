package proc

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerHandleClusterState(t *testing.T) {

	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1

	mgr := NewProcessorManager(stateMgr, &testReceiverInfoProvider{}, st, cfg, nil,
		createTestBatchHandler, nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(&testVmgrClient{})
	err = mgr.Start()
	require.NoError(t, err)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	cs := clustmgr.ClusterState{
		Version: 12,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, 2, numProcessors(&mgr.processors))

	p1, ok := mgr.processors.Load(0)
	require.True(t, ok)
	proc1 := p1.(*processor)
	require.False(t, proc1.IsLeader())

	p2, ok := mgr.processors.Load(1)
	require.True(t, ok)
	proc2 := p2.(*processor)
	require.True(t, proc2.IsLeader())

	require.Equal(t, 12, int(atomic.LoadInt64(&mgr.clusterVersion)))
	require.True(t, mgr.IsReadyAsOfVersion(12))

	cs = clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}
	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, 2, numProcessors(&mgr.processors))
	require.Equal(t, 13, int(atomic.LoadInt64(&mgr.clusterVersion)))
	require.True(t, mgr.IsReadyAsOfVersion(13))

	p1, ok = mgr.processors.Load(0)
	require.True(t, ok)
	proc1 = p1.(*processor)
	require.True(t, proc1.IsLeader())

	p2, ok = mgr.processors.Load(1)
	require.True(t, ok)
	proc2 = p2.(*processor)
	require.True(t, proc2.IsLeader())

	cs = clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 3},
			},
		},
	}
	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, 2, numProcessors(&mgr.processors))
	require.Equal(t, 14, int(atomic.LoadInt64(&mgr.clusterVersion)))
	require.True(t, mgr.IsReadyAsOfVersion(14))

	// Cluster version must be exact
	require.False(t, mgr.IsReadyAsOfVersion(13))
	require.False(t, mgr.IsReadyAsOfVersion(15))

	// Now send a cluster state which has no processors for node 1 - this means node 1 has been kicked out of the cluster
	// in this case we do not want cluster version to be updated, as this is used in queries, and we don't want queries
	// to see any new data

	cs = clustmgr.ClusterState{
		Version: 15,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 3},
			},
		},
	}
	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, 0, numProcessors(&mgr.processors))
	// cluster version hasn't changed
	require.Equal(t, 14, int(atomic.LoadInt64(&mgr.clusterVersion)))
	require.True(t, mgr.IsReadyAsOfVersion(14))

	require.False(t, mgr.IsReadyAsOfVersion(13))
	require.False(t, mgr.IsReadyAsOfVersion(15))
}

func TestPartitionMappings(t *testing.T) {

	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1
	cfg.ProcessorCount = 3

	mgr := NewProcessorManager(stateMgr, &testReceiverInfoProvider{}, st, cfg, nil,
		createTestBatchHandler, nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(&testVmgrClient{})
	err = mgr.Start()
	require.NoError(t, err)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	cs := clustmgr.ClusterState{
		Version: 12,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, 3, numProcessors(&mgr.processors))

	np1, err := mgr.NodePartitions("stream1", 10)
	require.NoError(t, err)
	require.Greater(t, len(np1), 0)

	np2, err := mgr.NodePartitions("stream2", 10)
	require.NoError(t, err)
	require.Greater(t, len(np1), 0)

	require.NotEqual(t, np1, np2)

	np1_2, err := mgr.NodePartitions("stream1", 10)
	require.NoError(t, err)
	require.Equal(t, np1, np1_2)

	np2_2, err := mgr.NodePartitions("stream2", 10)
	require.NoError(t, err)
	require.Equal(t, np2, np2_2)

	// Now handle another cluster state - this should cause mapping to be invalidated

	cs = clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	// Mappings should be different

	np1_3, err := mgr.NodePartitions("stream1", 10)
	require.NoError(t, err)
	require.NotEqual(t, np1, np1_3)

	np2_3, err := mgr.NodePartitions("stream2", 10)
	require.NoError(t, err)
	require.NotEqual(t, np2, np2_3)
}

func numProcessors(processors *sync.Map) int {
	numProcessors := 0
	processors.Range(func(key, value any) bool {
		numProcessors++
		return true
	})
	return numProcessors
}

type testClustStateMgr struct {
	handler func(state clustmgr.ClusterState) error
}

func (t *testClustStateMgr) MarkGroupAsValid(int, int, int) (bool, error) {
	return false, nil
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

func TestGetLeaderNodeForProcessor(t *testing.T) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1

	mgr := NewProcessorManager(stateMgr, &testReceiverInfoProvider{}, st, cfg, nil, createTestBatchHandler,
		nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(&testVmgrClient{})
	err = mgr.Start()
	require.NoError(t, err)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	cs := clustmgr.ClusterState{
		Version: 12,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	leader, err := mgr.GetLeaderNode(0)
	require.NoError(t, err)
	require.Equal(t, 0, leader)

	leader, err = mgr.GetLeaderNode(1)
	require.NoError(t, err)
	require.Equal(t, 1, leader)
}

func TestStateHandlers(t *testing.T) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1

	mgr := NewProcessorManager(stateMgr, &testReceiverInfoProvider{}, st, cfg, nil,
		createTestBatchHandler, nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(&testVmgrClient{})
	err = mgr.Start()
	require.NoError(t, err)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	handler1 := &testClustStateHandler{}
	mgr.RegisterStateHandler(handler1.handleState)

	handler2 := &testClustStateHandler{}
	mgr.RegisterStateHandler(handler2.handleState)

	cs := clustmgr.ClusterState{
		Version: 12,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	require.Equal(t, cs, handler1.getClusterState())
	require.Equal(t, cs, handler2.getClusterState())
}

func TestVersionInjection(t *testing.T) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1
	cfg.MinSnapshotInterval = 1 * time.Millisecond

	vmgrClient := &testVmgrClient{currentVersion: 100, vHandler: newVcHandler()}

	receiverProvider := &testReceiverInfoProvider{
		injectableReceiverIDs: map[int][]int{
			0: {1000},
			1: {1000},
			2: {1000},
		},
		requiredCompletions: 3,
	}

	mgr := NewProcessorManager(stateMgr, receiverProvider, st, cfg, nil, createTestBatchHandler,
		nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(vmgrClient)
	err = mgr.Start()
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	cs := clustmgr.ClusterState{
		Version: 12,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	// Sending the cluster state should cause two leader processors to be deployed.
	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	// The processors should have barriers injected into them at current version, which is 100
	// They shouldn't complete as we require 3 completions and there are only 2 processors

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, -1, vmgrClient.vHandler.getCompletedVersion())

	// Now we simulate receiving version broadcasts, which will occur from the version manager when the
	// previous version is complete, with the next current version
	// In this case we send new versions before previous ones are complete - the vmgr is free to do this
	// later versions should supersede earlier ones
	for currentVersion := 101; currentVersion < 110; currentVersion++ {
		mgr.HandleVersionBroadcast(currentVersion, 0, 0)
	}

	// Still there should be no completion
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, -1, vmgrClient.vHandler.getCompletedVersion())

	// Now deploy a new processor and make sure it gets barrier injected and completes version for current version
	cs = clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	// Now it should complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return vmgrClient.vHandler.getCompletedVersion() == 109, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	// Send some more updates - should complete at those versions
	for currentVersion := 110; currentVersion < 120; currentVersion++ {
		mgr.HandleVersionBroadcast(currentVersion, 0, 0)
		cv := currentVersion
		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			return vmgrClient.vHandler.getCompletedVersion() == cv, nil
		}, 5*time.Second, 1*time.Millisecond)
		require.True(t, ok)
		require.NoError(t, err)
	}
}

func TestVersionDelayWhenCompletedIncreases(t *testing.T) {
	testVersionDelay(t, true)
}

func TestVersionDelayWhenCompletedDoesNotIncrease(t *testing.T) {
	testVersionDelay(t, false)
}

func testVersionDelay(t *testing.T, increaseCompletedVersion bool) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1
	minSnapshotInterval := 100 * time.Millisecond
	cfg.MinSnapshotInterval = minSnapshotInterval

	vmgrClient := &testVmgrClient{currentVersion: 100, vHandler: newVcHandler()}

	receiverProvider := &testReceiverInfoProvider{
		injectableReceiverIDs: map[int][]int{
			0: {1000},
			1: {1000},
			2: {1000},
		},
		requiredCompletions: 3,
	}

	mgr := NewProcessorManager(stateMgr, receiverProvider, st, cfg, nil, createTestBatchHandler,
		nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(vmgrClient)
	err = mgr.Start()
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	vmgrClient.vHandler.waitForVersionToComplete(t, 100)
	completedVersion := 100
	for version := 101; version < 110; version++ {
		start := common.NanoTime()
		if increaseCompletedVersion {
			mgr.HandleVersionBroadcast(version, completedVersion, 0)
		} else {
			mgr.HandleVersionBroadcast(version, 0, 0)
		}
		vmgrClient.vHandler.waitForVersionToComplete(t, version)
		dur := common.NanoTime() - start
		// Allow for a 10% margin of error
		min := 0.90 * float64(minSnapshotInterval)
		if increaseCompletedVersion {
			require.GreaterOrEqual(t, int(dur), int(min))
			completedVersion++
		} else {
			require.Less(t, int(dur), int(min))
		}
	}
}

//func TestIdleProcessors(t *testing.T) {
//	stateMgr := &testClustStateMgr{}
//
//	st := store.TestStore()
//	err := st.Start()
//	require.NoError(t, err)
//	defer func() {
//		err := st.Stop()
//		require.NoError(t, err)
//	}()
//	cfg := &conf.Config{}
//	cfg.ApplyDefaults()
//	cfg.NodeID = 1
//	minSnapshotInterval := 100 * time.Millisecond
//	cfg.MinSnapshotInterval = minSnapshotInterval
//	cfg.IdleProcessorCheckInterval = 500 * time.Millisecond
//
//	vmgrClient := &testVmgrClient{currentVersion: 100, vHandler: newVcHandler()}
//
//	receiverProvider := &testReceiverInfoProvider{
//		injectableReceiverIDs: map[int][]int{
//			0: {1000},
//			1: {1000},
//			2: {1000},
//		},
//		requiredCompletions: 3,
//	}
//
//	mgr := NewProcessorManager(stateMgr, &testBatchHandler{}, receiverProvider, st, cfg, nil).(*manager)
//	mgr.SetVersionManagerClient(vmgrClient)
//	err = mgr.Start()
//	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)
//
//	cs := clustmgr.ClusterState{
//		Version: 13,
//		GroupStates: [][]clustmgr.GroupNode{
//			{
//				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
//				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
//				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
//			},
//			{
//				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
//				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
//				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
//			},
//			{
//				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
//				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
//				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
//			},
//		},
//	}
//
//	err = stateMgr.sendClusterState(cs)
//	require.NoError(t, err)
//
//	vmgrClient.vHandler.waitForVersionToComplete(t, 100)
//
//	// A few versions will complete but the processors are not processing standard batches
//	// so after a while versions will stop completing.
//	version := 101
//	for ; version < 200; version++ {
//		mgr.HandleVersionBroadcast(version, 0)
//
//		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
//			return vmgrClient.vHandler.getCompletedVersion() == version, nil
//		}, 400*time.Millisecond, 1*time.Millisecond)
//		require.NoError(t, err)
//		if !ok {
//			// timed out waiting to complete
//			// make sure we completed more than one version
//			require.Greater(t, version, 101)
//			break
//		}
//	}
//
//	// Now inject a batch, this should cause versions to start completing straight away
//	p, ok := mgr.processors.Load(0)
//	require.True(t, ok)
//	proc := p.(*processor)
//	ch := make(chan error, 1)
//
//	pb := NewProcessBatch(0, nil, 1000, 0, -1)
//	start := common.NanoTime()
//	proc.IngestBatch(pb, func(err error) {
//		ch <- err
//	})
//	require.NoError(t, <-ch)
//
//	vmgrClient.vHandler.waitForVersionToComplete(t, version)
//	dur := common.NanoTime() - start
//	// Should be very quick, likely much quicker than this
//	require.Less(t, int(dur), int(10*time.Millisecond))
//}

func TestInjectableReceivers(t *testing.T) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1

	vmgrClient := &testVmgrClient{currentVersion: 100, vHandler: newVcHandler()}

	// Only processors 0 and 1 have injectable receivers
	receiverProvider := &testReceiverInfoProvider{
		injectableReceiverIDs: map[int][]int{
			0: {1000, 2000, 3000},
			1: {1000},
		},
		requiredCompletions: 4,
	}

	mgr := NewProcessorManager(stateMgr, receiverProvider, st, cfg, nil, createTestBatchHandler,
		nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(vmgrClient)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)
	err = mgr.Start()

	// Deploy 3 processors
	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	// Initial version should complete
	vmgrClient.vHandler.waitForVersionToComplete(t, 100)

	p0, ok := mgr.processors.Load(0)
	require.True(t, ok)
	proc0 := p0.(*processor)

	// proc 0 should have barriers injected for receivers 1000, 2000, 3000
	_, ok = proc0.barrierInfos[1000]
	require.True(t, ok)
	_, ok = proc0.barrierInfos[2000]
	require.True(t, ok)
	_, ok = proc0.barrierInfos[3000]
	require.True(t, ok)

	p1, ok := mgr.processors.Load(1)
	require.True(t, ok)
	proc1 := p1.(*processor)

	// proc 0 should have barriers injected for receivers 1000
	_, ok = proc1.barrierInfos[1000]
	require.True(t, ok)
	_, ok = proc1.barrierInfos[2000]
	require.False(t, ok)
	_, ok = proc1.barrierInfos[3000]
	require.False(t, ok)

	// proc 2 should have no barriers injected
	p2, ok := mgr.processors.Load(2)
	require.True(t, ok)
	proc2 := p2.(*processor)

	_, ok = proc2.barrierInfos[1000]
	require.False(t, ok)
	_, ok = proc2.barrierInfos[2000]
	require.False(t, ok)
	_, ok = proc2.barrierInfos[3000]
	require.False(t, ok)
}

func TestBarrierForwarding(t *testing.T) {
	stateMgr := &testClustStateMgr{}

	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = 1

	cfg.ClusterAddresses = []string{"localhost:7887", "localhost:7888"}
	serverAddress := cfg.ClusterAddresses[1]

	vmgrClient := &testVmgrClient{currentVersion: 100, vHandler: newVcHandler()}

	batchHandler := newForwardingBatchHandler()
	batchHandler.requiredCompletions = 2
	batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
		1000: {{
			forwardReceiverID: 1001,
			processorIDs:      []int{0, 1},
		}},
	}
	batchHandler.forwardingProcessorCounts = map[int]int{
		1001: 2,
	}
	batchHandler.injectableReceivers = map[int][]int{
		0: {1000},
		1: {1000},
	}

	remotingServer := remoting.NewServer(serverAddress, conf.TLSConfig{})
	err = remotingServer.Start()
	require.NoError(t, err)
	defer func() {
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	mgr := NewProcessorManager(stateMgr, batchHandler, st, cfg, nil, func(processorID int) BatchHandler {
		return batchHandler
	}, nil, &testIngestNotifier{}).(*ProcessorManager)
	mgr.SetVersionManagerClient(vmgrClient)
	stateMgr.SetClusterStateHandler(mgr.HandleClusterState)
	err = mgr.Start()

	remotingServer.RegisterMessageHandler(remoting.ClusterMessageForwardMessage, &forwardMessageHandler{m: mgr})

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 3},
			},
		},
	}

	err = stateMgr.sendClusterState(cs)
	require.NoError(t, err)

	// Initial version should complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return vmgrClient.vHandler.getCompletedVersion() == 100, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)
}

type testClustStateHandler struct {
	lock sync.Mutex
	cs   clustmgr.ClusterState
}

func (tc *testClustStateHandler) handleState(cs clustmgr.ClusterState) error {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.cs = cs
	return nil
}

func (tc *testClustStateHandler) getClusterState() clustmgr.ClusterState {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.cs
}

type testVmgrClient struct {
	lock           sync.Mutex
	currentVersion int
	vHandler       vcHandler
}

func (t *testVmgrClient) FailureDetected(int, int) error {
	panic("not implemented")
}

func (t *testVmgrClient) GetLastFailureFlushedVersion(int) (int, error) {
	panic("not implemented")
}

func (t *testVmgrClient) FailureComplete(int, int) error {
	panic("not implemented")
}

func (t *testVmgrClient) IsFailureComplete(int) (bool, error) {
	panic("not implemented")
}

func (t *testVmgrClient) VersionFlushed(int, int, int, int) error {
	return nil
}

func (t *testVmgrClient) DoomInProgressVersions(nextCompletableVersion int) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.currentVersion = nextCompletableVersion
	return nil
}

func (t *testVmgrClient) VersionComplete(version int, requiredCompletions int, commandID int, doomed bool, cf func(error)) {
	t.vHandler.versionComplete(version, requiredCompletions, commandID, doomed, cf)
}

func (t *testVmgrClient) GetVersions() (int, int, int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.currentVersion, 0, 0, nil
}

func (t *testVmgrClient) Start() error {
	return nil
}

func (t *testVmgrClient) Stop() error {
	return nil
}

func createTestBatchHandler(int) BatchHandler {
	return &testBatchHandler{}
}

type testIngestNotifier struct {
}

func (t *testIngestNotifier) StopIngest() error {
	return nil
}

func (t *testIngestNotifier) StartIngest(int) error {
	return nil
}
