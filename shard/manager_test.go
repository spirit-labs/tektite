package shard

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestManagerAddRemoveShard(t *testing.T) {
	numShards := 10
	numMembers := 3
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)

	newMembers := updateMembership(t, 1, managers, 0, 1, 2)
	checkMapping(t, managers, numShards, newMembers)

	// Now remove a member
	newMembers = updateMembership(t, 2, managers, 1, 2)
	checkMapping(t, managers, numShards, newMembers)

	// Now reduce to zero
	newMembers = updateMembership(t, 3, managers)
	checkMapping(t, managers, numShards, newMembers)
}

// TestManagerShardRandomAddRemove - exercises the shard mapping logic by creating random combinations of members
// and verify the mapping of shard to manager is correct
func TestManagerShardRandomAddRemove(t *testing.T) {
	numShards := 10
	numMembers := 5
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)
	for i := 0; i < 100; i++ {

		var memberIndexes []int
		for j := 0; j < numMembers; j++ {
			// toss a coin to see if member is added
			if rand.Intn(2) == 0 {
				memberIndexes = append(memberIndexes, j)
			}
		}
		// shuffle them
		rand.Shuffle(len(memberIndexes), func(i, j int) { memberIndexes[i], memberIndexes[j] = memberIndexes[j], memberIndexes[i] })
		newMembers := updateMembership(t, i, managers, memberIndexes...)
		checkMapping(t, managers, numShards, newMembers)
	}
}

func updateMembership(t *testing.T, clusterVersion int, managers []*Manager, memberIndexes ...int) []cluster.MembershipEntry {
	now := time.Now().UnixMilli()
	var members []cluster.MembershipEntry
	for _, memberIndex := range memberIndexes {
		members = append(members, cluster.MembershipEntry{
			Address:    managers[memberIndex].transportServer.Address(),
			UpdateTime: now,
		})
	}
	newState := cluster.MembershipState{
		ClusterVersion: clusterVersion,
		Members:        members,
	}
	for _, mgr := range managers {
		err := mgr.MembershipChanged(newState)
		require.NoError(t, err)
	}
	return members
}

func checkMapping(t *testing.T, managers []*Manager, numShards int, members []cluster.MembershipEntry) {
	mapping := expectedShardMemberMapping(numShards, members)
	for shardID, address := range mapping {
		for _, mgr := range managers {
			if address == mgr.transportServer.Address() {
				// shard must be on this member
				s, ok := mgr.GetShard(shardID)
				require.True(t, ok)
				require.NotNil(t, s)
			} else {
				// must not be here
				s, ok := mgr.GetShard(shardID)
				require.False(t, ok)
				require.Nil(t, s)
			}
		}
	}
}

func expectedShardMemberMapping(numShards int, members []cluster.MembershipEntry) map[int]string {
	mapping := map[int]string{}
	if len(members) > 0 {
		for shard := 0; shard < numShards; shard++ {
			i := shard % len(members)
			mapping[shard] = members[i].Address
		}
	}
	return mapping
}

func TestClientNoMembersOnCreation(t *testing.T) {
	numShards := 10
	numMembers := 3
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)

	// There are no members in the cluster at this point

	_, err := managers[0].Client(7)
	require.Error(t, err)
	// caller should get an unavailable error so it can retry
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "manager has not received cluster membership", err.Error())
}

func TestClientWrongClusterVersion(t *testing.T) {
	numShards := 10
	numMembers := 3
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)
	updateMembership(t, 1, managers, 0, 1, 2)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	cl, err := managers[0].Client(7)
	require.NoError(t, err)

	// Now update membership again so cluster version increases
	updateMembership(t, 2, managers, 0, 1)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "shard manager - cluster version mismatch", err.Error())

	_, err = cl.QueryTablesInRange(nil, nil)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "shard manager - cluster version mismatch", err.Error())
}

func TestManagerUseClosedClient(t *testing.T) {
	numShards := 10
	numMembers := 3
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0, 1, 2)

	cl, err := managers[0].Client(7)
	require.NoError(t, err)
	// close client before using it
	err = cl.Close()
	require.NoError(t, err)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	// Should not be an unavailable error - caller should not be re-using closed connection so would be a programming error
	require.False(t, common.IsUnavailableError(err))

	// create new client
	cl, err = managers[0].Client(7)
	require.NoError(t, err)
	// use it
	err = cl.ApplyLsmChanges(batch)
	require.NoError(t, err)

	// close it
	err = cl.Close()
	require.NoError(t, err)

	// try and use it again
	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.False(t, common.IsUnavailableError(err))
}

func TestManagerApplyChanges(t *testing.T) {
	numShards := 10
	numMembers := 3
	managers, tearDown := setupManagers(t, numMembers, numShards)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0, 1, 2)

	cl, err := managers[0].Client(7)
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	err = cl.ApplyLsmChanges(batch)
	require.NoError(t, err)

	res, err := cl.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID := res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))

	// And with nil keyStart and end
	res, err = cl.QueryTablesInRange(nil, nil)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID = res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))
}

func setupManagers(t *testing.T, numMembers int, numShards int) ([]*Manager, func(t *testing.T)) {
	localTransports := transport.NewLocalTransports()
	var addresses []string
	var managers []*Manager
	for i := 0; i < numMembers; i++ {
		address := uuid.New().String()
		addresses = append(addresses, address)
		transportServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		objStore := dev.NewInMemStore(0)
		mgr := NewManager(numShards, stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix,
			objStore, localTransports.CreateConnection, transportServer, lsm.ManagerOpts{})
		err = mgr.Start()
		require.NoError(t, err)
		managers = append(managers, mgr)
	}
	return managers, func(t *testing.T) {
		for _, manager := range managers {
			err := manager.Stop()
			require.NoError(t, err)
		}
	}
}
