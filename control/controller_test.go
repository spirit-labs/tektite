package control

import (
	"errors"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/streammeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestClientNoMembersOnCreation(t *testing.T) {
	managers, tearDown := setupManagers(t, 1)
	defer tearDown(t)

	// There are no members in the cluster at this point
	_, err := managers[0].Client()
	require.Error(t, err)
	// caller should get an unavailable error so it can retry
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller has not received cluster membership", err.Error())
}

func TestClientWrongClusterVersion(t *testing.T) {
	managers, tearDown := setupManagers(t, 1)
	defer tearDown(t)
	updateMembership(t, 1, managers, 0)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	cl, err := managers[0].Client()
	require.NoError(t, err)

	// Now update membership again so cluster version increases
	updateMembership(t, 2, managers, 0)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - cluster version mismatch", err.Error())

	_, err = cl.QueryTablesInRange(nil, nil)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - cluster version mismatch", err.Error())
}

func TestManagerUseClosedClient(t *testing.T) {
	managers, tearDown := setupManagers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0)

	cl, err := managers[0].Client()
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
	cl, err = managers[0].Client()
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
	managers, tearDown := setupManagers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0)

	cl, err := managers[0].Client()
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

func TestManagerRegisterL0(t *testing.T) {

	managers, tearDown := setupManagers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0)

	cl, err := managers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	// First get some offsets
	offs, err := cl.GetOffsets([]offsets.GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  100,
		},
		{
			TopicID:     8,
			PartitionID: 1,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(offs))

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	regEntry := lsm.RegistrationEntry{
		Level:      0,
		TableID:    tableID,
		MinVersion: 123,
		MaxVersion: 1235,
		KeyStart:   keyStart,
		KeyEnd:     keyEnd,
		AddedTime:  uint64(time.Now().UnixMilli()),
		NumEntries: 1234,
		TableSize:  12345567,
	}
	writtenOffs := []offsets.UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			OffsetStart: offs[0],
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 2,
			OffsetStart: offs[1],
			NumOffsets:  100,
		},
		{
			TopicID:     8,
			PartitionID: 1,
			OffsetStart: offs[2],
			NumOffsets:  100,
		},
	}

	err = cl.RegisterL0Table(writtenOffs, regEntry)
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

func TestManagerGetOffsets(t *testing.T) {
	managers, tearDown := setupManagersWithOffsetProviders(t, 1, testTopicProvider, testOffsetLoader)
	defer tearDown(t)

	updateMembership(t, 1, managers, 0)

	cl, err := managers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	offs, err := cl.GetOffsets([]offsets.GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 0,
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  200,
		},
		{
			TopicID:     8,
			PartitionID: 0,
			NumOffsets:  150,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(offs))

	require.Equal(t, 1234+1, int(offs[0]))
	require.Equal(t, 3456+1, int(offs[1]))
	require.Equal(t, 5678+1, int(offs[2]))
	require.NoError(t, err)
}

func setupManagers(t *testing.T, numMembers int) ([]*Controller, func(t *testing.T)) {
	return setupManagersWithOffsetProviders(t, numMembers, testTopicProvider, testOffsetLoader)
}

func setupManagersWithOffsetProviders(t *testing.T, numMembers int, topicProvider streammeta.TopicInfoProvider,
	offsetLoader offsets.PartitionOffsetLoader) ([]*Controller, func(t *testing.T)) {
	loaderFactory := func() (offsets.PartitionOffsetLoader, error) {
		return offsetLoader, nil
	}
	localTransports := transport.NewLocalTransports()
	var managers []*Controller
	for i := 0; i < numMembers; i++ {
		address := uuid.New().String()
		transportServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		objStore := dev.NewInMemStore(0)
		cfg := NewConf()
		mgr := NewController(cfg, objStore, localTransports.CreateConnection, transportServer, topicProvider, loaderFactory)
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

type testPartitionOffsetLoader struct {
	topicOffsets map[int]map[int]int64
}

func (t *testPartitionOffsetLoader) LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error) {
	to, ok := t.topicOffsets[topicID]
	if !ok {
		return 0, errors.New("topic not found")
	}
	po, ok := to[partitionID]
	if !ok {
		return 0, errors.New("partition not found")
	}
	return po, nil
}

func updateMembership(t *testing.T, clusterVersion int, managers []*Controller, memberIndexes ...int) []cluster.MembershipEntry {
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

var testTopicProvider = &streammeta.SimpleTopicInfoProvider{
	Infos: map[string]streammeta.TopicInfo{
		"topic1": {
			TopicID:        7,
			PartitionCount: 4,
		},
		"topic2": {
			TopicID:        8,
			PartitionCount: 2,
		},
	},
}

var testOffsetLoader = &testPartitionOffsetLoader{
	topicOffsets: map[int]map[int]int64{
		7: {
			0: 1234,
			1: 3456,
			2: 0,
			3: -1,
		},
		8: {
			0: 5678,
			1: 3456,
		},
	},
}
