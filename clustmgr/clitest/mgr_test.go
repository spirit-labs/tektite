package clitest

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestManagerClusterState(t *testing.T) {
	t.Parallel()
	cleanUp(t)

	mgrs, chans := startManagers(t, t.Name(), 3, 2, 3)
	defer func() {
		for _, mgr := range mgrs {
			//goland:noinspection GoUnhandledErrorResult
			mgr.Stop()
		}
	}()

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 0,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, false, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
			{
				clustmgr.GroupNode{0, false, false, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
		},
	})
}

func TestManagerNodeStoppedAndRestarted(t *testing.T) {
	t.Parallel()

	cleanUp(t)

	mgrs, chans := startManagers(t, t.Name(), 3, 2, 3)
	defer func() {
		for _, mgr := range mgrs {
			//goland:noinspection GoUnhandledErrorResult
			mgr.Stop()
		}
	}()

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 0,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, false, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
			{
				clustmgr.GroupNode{0, false, false, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
		},
	})

	// Now mark as valid
	ok, err := mgrs[2].GetClient().MarkGroupAsValid(0, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(1, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
		},
	})

	err = mgrs[1].Stop()
	require.NoError(t, err)

	var newChans []chan clustmgr.ClusterState
	for i, ch := range chans {
		if i != 1 {
			newChans = append(newChans, ch)
		}
	}

	verifyClusterStates(t, newChans, clustmgr.ClusterState{
		Version: 2,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{2, true, true, 0},
			},
		},
	})

	mgrs[1], chans[1] = startManager(t, t.Name(), 1, 3, 2)

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 3,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
				clustmgr.GroupNode{1, false, false, 3},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{2, true, true, 0},
				clustmgr.GroupNode{1, false, false, 3},
			},
		},
	})
}

func TestManagerBounceNodeQuickly(t *testing.T) {
	t.Parallel()

	cleanUp(t)

	mgrs, chans := startManagers(t, t.Name(), 3, 2, 3)
	defer func() {
		for _, mgr := range mgrs {
			//goland:noinspection GoUnhandledErrorResult
			mgr.Stop()
		}
	}()

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 0,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, false, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
			{
				clustmgr.GroupNode{0, false, false, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
		},
	})

	// Now mark as valid
	ok, err := mgrs[2].GetClient().MarkGroupAsValid(0, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(1, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
		},
	})

	// Stop and immediately restart the node
	err = mgrs[1].Stop()
	require.NoError(t, err)
	mgrs[1], chans[1] = startManager(t, t.Name(), 1, 3, 2)

	// It shouldn't have any leaders
	expectedGroupStates :=
		[][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
				clustmgr.GroupNode{1, false, false, 2},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{2, true, true, 0},
				clustmgr.GroupNode{1, false, false, 2},
			},
		}

	actCs := waitForGroupStates(t, expectedGroupStates, chans)
	require.Greater(t, actCs.GroupStates[0][2].JoinedVersion, 1)
	require.Greater(t, actCs.GroupStates[1][2].JoinedVersion, 1)
}

func TestManagerFailoverToOneNodeThenRestartLeadersPreserved(t *testing.T) {
	t.Parallel()

	cleanUp(t)

	mgrs, chans := startManagers(t, t.Name(), 3, 2, 3)
	defer stopMgrs(mgrs)

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 0,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, false, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
			{
				clustmgr.GroupNode{0, false, false, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, false, 0},
			},
		},
	})

	// Now mark as valid
	ok, err := mgrs[2].GetClient().MarkGroupAsValid(0, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(1, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgrs[2].GetClient().MarkGroupAsValid(2, 1, 0)
	require.NoError(t, err)
	require.True(t, ok)

	verifyClusterStates(t, chans, clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{0, true, true, 0},
				clustmgr.GroupNode{1, false, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
			{
				clustmgr.GroupNode{0, false, true, 0},
				clustmgr.GroupNode{1, true, true, 0},
				clustmgr.GroupNode{2, false, true, 0},
			},
		},
	})

	// Stop nodes 0 and 2, verify all leaders are on node 1

	// Stop and immediately restart the node
	err = mgrs[0].Stop()
	require.NoError(t, err)
	err = mgrs[2].Stop()
	require.NoError(t, err)

	expectedGroupStates := [][]clustmgr.GroupNode{
		{
			clustmgr.GroupNode{1, true, true, 0},
		},
		{
			clustmgr.GroupNode{1, true, true, 0},
		},
	}

	actCs := waitForGroupStates(t, expectedGroupStates, []chan clustmgr.ClusterState{chans[1]})
	require.Greater(t, actCs.Version, 1)
	// Check the joined version
	require.Equal(t, 0, actCs.GroupStates[0][0].JoinedVersion)

	// Start nodes 0 and 2

	mgrs[0], chans[0] = startManager(t, t.Name(), 0, 2, 3)
	mgrs[2], chans[2] = startManager(t, t.Name(), 2, 2, 3)

	expectedGroupStates = [][]clustmgr.GroupNode{
		{
			clustmgr.GroupNode{1, true, true, 0},
			clustmgr.GroupNode{0, false, false, 3},
			clustmgr.GroupNode{2, false, false, 3},
		},
		{
			clustmgr.GroupNode{1, true, true, 0},
			clustmgr.GroupNode{0, false, false, 3},
			clustmgr.GroupNode{2, false, false, 3},
		},
	}

	actCs = waitForGroupStates(t, expectedGroupStates, chans)
	require.Greater(t, actCs.GroupStates[0][1].JoinedVersion, 2)
	require.Greater(t, actCs.GroupStates[0][2].JoinedVersion, 2)
	require.Greater(t, actCs.GroupStates[1][1].JoinedVersion, 2)
	require.Greater(t, actCs.GroupStates[1][2].JoinedVersion, 2)
}

func waitForGroupStates(t *testing.T, groupStates [][]clustmgr.GroupNode, chans []chan clustmgr.ClusterState) clustmgr.ClusterState {
	var csRet clustmgr.ClusterState
	for _, ch := range chans {
	outerloop:
		for {
			select {
			case csr := <-ch:
				// We don't compare the joined version here as this is non-deterministic in tests
				if groupStatesEqualNoJoinedVersion(groupStates, csr.GroupStates) {
					csRet = csr
					break outerloop
				}
			case <-time.After(10 * time.Second):
				require.Fail(t, "timeout waiting for cluster state")
			}
		}
	}
	return csRet
}

func groupStatesEqualNoJoinedVersion(gsExpect [][]clustmgr.GroupNode, gsActual [][]clustmgr.GroupNode) bool {
	if len(gsExpect) != len(gsActual) {
		return false
	}
	for i, expectedGS := range gsExpect {
		actGS := gsActual[i]
		if len(expectedGS) != len(actGS) {
			return false
		}
		for j, expGn := range expectedGS {
			actGn := actGS[j]
			if expGn.Valid != actGn.Valid ||
				expGn.Leader != actGn.Leader ||
				expGn.NodeID != actGn.NodeID {
				return false
			}
		}
	}
	return true
}

func verifyClusterStates(t *testing.T, stateChans []chan clustmgr.ClusterState, states ...clustmgr.ClusterState) {
	for _, expectedState := range states {
		for _, ch := range stateChans {
			actState := <-ch
			require.Equalf(t, expectedState, actState, "expected state:\n%v\nactual state:\n%v", expectedState, actState)
		}
	}
}

func startManagers(t *testing.T, prefix string, numNodes int, numGroups int, maxReplicas int) ([]*clustmgr.ClusteredStateManager, []chan clustmgr.ClusterState) {
	var mgrs []*clustmgr.ClusteredStateManager
	var chans []chan clustmgr.ClusterState
	for i := 0; i < numNodes; i++ {
		mgr, ch := startManager(t, prefix, i, numGroups, maxReplicas)
		mgrs = append(mgrs, mgr)
		chans = append(chans, ch)
	}
	return mgrs, chans
}

func startManager(t *testing.T, prefix string, nodeID int, numGroups int, maxReplicas int) (*clustmgr.ClusteredStateManager, chan clustmgr.ClusterState) {
	mgr := clustmgr.NewClusteredStateManager(prefix, "test_cluster", nodeID,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		numGroups, maxReplicas)
	ch := make(chan clustmgr.ClusterState, 100)
	mgr.SetClusterStateHandler(func(state clustmgr.ClusterState) error {
		ch <- state
		return nil
	})
	err := mgr.Start()
	require.NoError(t, err)
	return mgr, ch
}

func stopMgrs(mgrs []*clustmgr.ClusteredStateManager) {
	for _, mgr := range mgrs {
		mgr.Freeze()
	}
	for _, mgr := range mgrs {
		//goland:noinspection GoUnhandledErrorResult
		mgr.Stop()
	}
}
