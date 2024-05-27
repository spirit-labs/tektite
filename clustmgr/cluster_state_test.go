package clustmgr

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	cs := ClusterState{
		Version: 3,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{2, false, false, 0},
				GroupNode{1, false, false, 3},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{2, true, true, 0},
				GroupNode{1, false, false, 3},
			},
		},
	}
	bytes := []byte{1, 2, 3}
	bytes = cs.Serialize(bytes)
	require.NotNil(t, bytes)

	cs2 := ClusterState{}
	cs2.Deserialize(bytes, 3)

	require.Equal(t, cs, cs2)
}

func TestStateInitialState(t *testing.T) {

	activeNodes := map[int]bool{
		0: true,
		1: true,
		2: true,
	}
	ok, newStates := calculateGroupStates(nil, activeNodes, 2, 3, 0)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     0,
		GroupStates: newStates,
	}
	// Leaders are allocated left to right
	require.Equal(t, ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
			},
		},
	}, cs2)
}

func TestAddReplicas(t *testing.T) {
	activeNodes := map[int]bool{
		0: true,
		1: true,
		2: true,
	}
	ok, newStates := calculateGroupStates(nil, activeNodes, 2, 5, 0)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     0,
		GroupStates: newStates,
	}
	require.Equal(t, ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
			},
		},
	}, cs2)

	// Now add some more nodes
	activeNodes = map[int]bool{
		0: false,
		1: false,
		2: false,
		3: true,
		4: true,
	}
	ok, newStates = calculateGroupStates(&cs2, activeNodes, 2, 5, 1)
	require.True(t, ok)
	cs3 := ClusterState{
		Version:     1,
		GroupStates: newStates,
	}
	// Replicas should be added but leaders should remain the same
	require.Equal(t, ClusterState{
		Version: 1,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
				GroupNode{3, false, false, 1},
				GroupNode{4, false, false, 1},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
				GroupNode{3, false, false, 1},
				GroupNode{4, false, false, 1},
			},
		},
	}, cs3)

}

func TestWontAddReplicasAlreadyAtMax(t *testing.T) {
	activeNodes := map[int]bool{
		0: true,
		1: true,
		2: true,
	}
	ok, newStates := calculateGroupStates(nil, activeNodes, 2, 3, 0)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     0,
		GroupStates: newStates,
	}
	require.Equal(t, ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
			},
		},
	}, cs2)

	// Now add some more nodes
	activeNodes = map[int]bool{
		0: false,
		1: false,
		2: false,
		3: true,
		4: true,
	}
	// Cluster state shouldn't change as we already have max replicas
	ok, newStates = calculateGroupStates(&cs2, activeNodes, 2, 3, 1)
	require.True(t, ok)
	cs3 := ClusterState{
		Version:     0,
		GroupStates: newStates,
	}
	require.Equal(t, cs2, cs3)
}

func TestStateSimpleLoseNode(t *testing.T) {

	csInitial := ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, true, 0},
				GroupNode{2, false, true, 0},
			},
			{
				GroupNode{0, false, true, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, true, 0},
			},
		},
	}

	// Lose node 1
	activeNodes := map[int]bool{
		0: false,
		2: false,
	}
	ok, newStates := calculateGroupStates(&csInitial, activeNodes, 2, 3, 1)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     1,
		GroupStates: newStates,
	}
	// Leader on lost node moves but other leaders stay where they are
	require.Equal(t, ClusterState{
		Version: 1,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{2, false, true, 0},
			},
			{
				GroupNode{0, false, true, 0},
				GroupNode{2, true, true, 0},
			},
		},
	}, cs2)
}

func TestStateSimpleLoseAllButOne(t *testing.T) {

	csInitial := ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, true, 0},
				GroupNode{2, false, true, 0},
			},
			{
				GroupNode{0, false, true, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, true, 0},
			},
		},
	}

	// Lose nodes 0 and 2
	activeNodes := map[int]bool{
		1: false,
	}
	ok, newStates := calculateGroupStates(&csInitial, activeNodes, 2, 3, 1)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     1,
		GroupStates: newStates,
	}
	// Node 0 has both leaders
	require.Equal(t, ClusterState{
		Version: 1,
		GroupStates: [][]GroupNode{
			{
				GroupNode{1, true, true, 0},
			},
			{
				GroupNode{1, true, true, 0},
			},
		},
	}, cs2)

	// Now add nodes back - node 0 should retain leaders
	activeNodes = map[int]bool{
		0: true,
		1: false,
		2: true,
	}
	ok, newStates = calculateGroupStates(&cs2, activeNodes, 2, 3, 2)
	require.True(t, ok)
	cs3 := ClusterState{
		Version:     2,
		GroupStates: newStates,
	}
	// Node 0 has both leaders
	require.Equal(t, ClusterState{
		Version: 2,
		GroupStates: [][]GroupNode{
			{
				GroupNode{1, true, true, 0},
				GroupNode{0, false, false, 2},
				GroupNode{2, false, false, 2},
			},
			{
				GroupNode{1, true, true, 0},
				GroupNode{0, false, false, 2},
				GroupNode{2, false, false, 2},
			},
		},
	}, cs3)
}

func TestStateMaxReplicas(t *testing.T) {

	activeNodes := map[int]bool{
		0: true,
		1: true,
		2: true,
		3: true,
		4: true,
	}
	ok, newStates := calculateGroupStates(nil, activeNodes, 5, 3, 0)
	require.True(t, ok)
	cs2 := ClusterState{
		Version:     0,
		GroupStates: newStates,
	}
	require.Equal(t,
		ClusterState{
			Version: 0x0,
			GroupStates: [][]GroupNode{
				{
					{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 0x0},
					{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 0x0},
					{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 0x0},
				},
				{
					{NodeID: 3, Leader: true, Valid: true, JoinedVersion: 0x0},
					{NodeID: 4, Leader: false, Valid: false, JoinedVersion: 0x0},
					{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 0x0},
				},
				{
					{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 0x0},
					{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 0x0},
					{NodeID: 3, Leader: false, Valid: false, JoinedVersion: 0x0},
				},
				{
					{NodeID: 4, Leader: true, Valid: true, JoinedVersion: 0x0},
					{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 0x0},
					{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 0x0},
				},
				{
					{NodeID: 2, Leader: true, Valid: true, JoinedVersion: 0x0},
					{NodeID: 3, Leader: false, Valid: false, JoinedVersion: 0x0},
					{NodeID: 4, Leader: false, Valid: false, JoinedVersion: 0x0},
				}}}, cs2)
}

func TestStateBounceNode(t *testing.T) {

	cs1 := ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, true, 0},
				GroupNode{2, false, true, 0},
			},
			{
				GroupNode{0, false, true, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, true, 0},
			},
		},
	}

	// bounce node 1 by readding with added = true - this is what would happen if a node is quickly lost and re-added
	// before the next cluster state was calculated
	activeNodes := map[int]bool{
		0: false,
		1: true,
		2: false,
	}
	ok, newStates := calculateGroupStates(&cs1, activeNodes, 2, 5, 1)
	require.True(t, ok)
	cs3 := ClusterState{
		Version:     1,
		GroupStates: newStates,
	}
	require.Equal(t, ClusterState{
		Version: 1,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{2, false, true, 0},
				GroupNode{1, false, false, 1},
			},
			{
				GroupNode{0, false, true, 0},
				GroupNode{2, true, true, 0},
				GroupNode{1, false, false, 1},
			},
		},
	}, cs3)
}

func TestBounceDataloss(t *testing.T) {

	cs1 := ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
			},
		},
	}

	//try and bounce node 1, but there are no valid replicas - so data loss could occur
	activeNodes := map[int]bool{
		0: false,
		1: true,
		2: false,
	}
	ok, _ := calculateGroupStates(&cs1, activeNodes, 2, 5, 1)
	require.False(t, ok)
}

func TestFailoverDataloss(t *testing.T) {

	cs1 := ClusterState{
		Version: 0,
		GroupStates: [][]GroupNode{
			{
				GroupNode{0, true, true, 0},
				GroupNode{1, false, false, 0},
				GroupNode{2, false, false, 0},
			},
			{
				GroupNode{0, false, false, 0},
				GroupNode{1, true, true, 0},
				GroupNode{2, false, false, 0},
			},
		},
	}

	//fail node 1, but there are no valid replicas - so data loss could occur
	activeNodes := map[int]bool{
		0: false,
		2: false,
	}
	ok, _ := calculateGroupStates(&cs1, activeNodes, 2, 5, 1)
	require.False(t, ok)
}
