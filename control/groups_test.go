package control

import (
	"fmt"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGroupCoordinatorControllerRoundRobin(t *testing.T) {
	cg := NewGroupCoordinatorController(func() int {
		return 1
	})
	numMembers := 10
	state := applyClusterStateForMembers(numMembers, cg)

	numGroups := 100
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		address, epoch, err := cg.GetGroupCoordinatorInfo(groupID)
		require.NoError(t, err)
		require.Equal(t, 1, epoch)
		// should be assigned to members in a round-robin fashion
		require.Equal(t, address, getAddressFromMember(state.Members[i%numMembers]))
	}
}

func TestGroupCoordinatorControllerMemberLeft(t *testing.T) {
	cg := NewGroupCoordinatorController(func() int {
		return 1
	})
	numMembers := 10
	state := applyClusterStateForMembers(numMembers, cg)

	numGroups := 100
	groupToAddress := map[string]string{}
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		address, epoch, err := cg.GetGroupCoordinatorInfo(groupID)
		require.NoError(t, err)
		require.Equal(t, 1, epoch)
		groupToAddress[groupID] = address
	}

	newMembers := []cluster.MembershipEntry{state.Members[0], state.Members[1], state.Members[2], state.Members[4],
		state.Members[6], state.Members[7], state.Members[8]}
	newAddresses := map[string]struct{}{}
	for _, member := range newMembers {
		newAddresses[getAddressFromMember(member)] = struct{}{}
	}

	cg.MembershipChanged(&cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 2,
		Members:        newMembers,
	})

	// Get members again
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		address, epoch, err := cg.GetGroupCoordinatorInfo(groupID)
		require.NoError(t, err)
		oldAddress, ok := groupToAddress[groupID]
		require.True(t, ok)
		_, isInNewMembers := newAddresses[oldAddress]
		if isInNewMembers {
			// member didn't leave so address should be the same
			require.Equal(t, oldAddress, address)
			// epoch should be the same
			require.Equal(t, 1, epoch)
		} else {
			require.NotEqual(t, oldAddress, address)
			// epoch should have increased
			require.Equal(t, 2, epoch)
		}
	}
}

func TestGroupCoordinatorCheckEpochs(t *testing.T) {
	cg := NewGroupCoordinatorController(func() int {
		return 1
	})
	numMembers := 10
	state := applyClusterStateForMembers(numMembers, cg)

	numGroups := 100
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		_, epoch, err := cg.GetGroupCoordinatorInfo(groupID)
		require.NoError(t, err)
		require.Equal(t, i+1, epoch)
		newState := *state
		// update the state cluster version each time so epoch increases
		newState.ClusterVersion++
		cg.MembershipChanged(&newState)
		state = &newState
	}

	// First create some correct epochs
	var epochInfos []EpochInfo
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		epochInfos = append(epochInfos, EpochInfo{
			Key:   groupID,
			Epoch: i + 1,
		})
	}

	epochsOK := cg.CheckGroupEpochs(epochInfos)
	require.Equal(t, numGroups, len(epochsOK))
	for _, ok := range epochsOK {
		require.True(t, ok)
	}

	// And some all wrong
	epochInfos = nil
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		epochInfos = append(epochInfos, EpochInfo{
			Key:   groupID,
			Epoch: i + 2,
		})
	}

	epochsOK = cg.CheckGroupEpochs(epochInfos)
	require.Equal(t, numGroups, len(epochsOK))
	for _, ok := range epochsOK {
		require.False(t, ok)
	}

	// Alternating right and wrong
	epochInfos = nil
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		var epoch int
		if i%2 == 0 {
			epoch = i + 1
		} else {
			epoch = i + 2
		}
		epochInfos = append(epochInfos, EpochInfo{
			Key:   groupID,
			Epoch: epoch,
		})
	}

	epochsOK = cg.CheckGroupEpochs(epochInfos)
	require.Equal(t, numGroups, len(epochsOK))
	for i, ok := range epochsOK {
		if i%2 == 0 {
			require.True(t, ok)
		} else {
			require.False(t, ok)
		}
	}
}

func TestGroupCoordinatorCheckEpochControllerWriteKey(t *testing.T) {
	activatedVersion := 46
	cg := NewGroupCoordinatorController(func() int {
		return activatedVersion
	})
	numMembers := 10
	state := cluster.MembershipState{
		LeaderVersion:  23,
		ClusterVersion: 23,
	}
	applyClusterState(numMembers, &state, cg)

	epochsOK := cg.CheckGroupEpochs([]EpochInfo{
		{
			Key:   controllerWriterKey,
			Epoch: activatedVersion,
		},
	})
	require.Equal(t, 1, len(epochsOK))
	require.True(t, epochsOK[0])

	epochsOK = cg.CheckGroupEpochs([]EpochInfo{
		{
			Key:   controllerWriterKey,
			Epoch: 654,
		},
	})
	require.Equal(t, 1, len(epochsOK))
	require.False(t, epochsOK[0])
}

func getAddressFromMember(member cluster.MembershipEntry) string {
	memberData := common.MembershipData{}
	memberData.Deserialize(member.Data, 0)
	return memberData.KafkaListenerAddress
}

func applyClusterStateForMembers(numMembers int, cg *CoordinatorController) *cluster.MembershipState {
	state := cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
	}
	applyClusterState(numMembers, &state, cg)
	return &state
}

func applyClusterState(numMembers int, state *cluster.MembershipState, cg *CoordinatorController) {
	for i := 0; i < numMembers; i++ {
		memberData := common.MembershipData{
			KafkaListenerAddress: fmt.Sprintf("address-%d", i),
		}
		state.Members = append(state.Members, cluster.MembershipEntry{
			ID:   int32(i),
			Data: memberData.Serialize(nil),
		})
	}
	cg.MembershipChanged(state)
}
