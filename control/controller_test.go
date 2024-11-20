package control

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestClientNoMembersOnCreation(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	// There are no members in the cluster at this point
	_, err := controllers[0].Client()
	require.Error(t, err)
	// caller should get an unavailable error so it can retry
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller has not received cluster membership", err.Error())
}

func TestClientWrongClusterVersion(t *testing.T) {
	controllers, tearDown := setupControllers(t, 2)
	defer tearDown(t)
	updateMembership(t, 1, 1, controllers, 0)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	// Now update membership again so leader changes
	updateMembership(t, 2, 2, controllers, 0)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - leader version mismatch", err.Error())

	_, err = cl.QueryTablesInRange(nil, nil)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - leader version mismatch", err.Error())
}

func TestClientControllerNotLeader(t *testing.T) {
	controllers, tearDown := setupControllers(t, 2)
	defer tearDown(t)
	updateMembership(t, 1, 1, controllers, 0, 1)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	// Now remove node 0 (leader)
	updateMembership(t, 2, 2, controllers, 1)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller is not started", err.Error())

	_, err = cl.QueryTablesInRange(nil, nil)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller is not started", err.Error())
}

func TestControllerUseClosedClient(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)

	cl, err := controllers[0].Client()
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
	cl, err = controllers[0].Client()
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

func TestControllerApplyChanges(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)

	cl, err := controllers[0].Client()
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

func TestControllerPrePushAndRegisterL0(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controllers[0])

	cl, err := controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	offs, seq, _, err := cl.PrePush([]offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
				{
					PartitionID: 2,
					NumOffsets:  150,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  50,
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(offs))
	require.Equal(t, 1000, offs[0].TopicID)
	require.Equal(t, 2, len(offs[0].PartitionInfos))
	require.Equal(t, 1, offs[0].PartitionInfos[0].PartitionID)
	require.Equal(t, 99, int(offs[0].PartitionInfos[0].Offset))
	require.Equal(t, 2, offs[0].PartitionInfos[1].PartitionID)
	require.Equal(t, 149, int(offs[0].PartitionInfos[1].Offset))

	require.Equal(t, 1001, offs[1].TopicID)
	require.Equal(t, 1, len(offs[1].PartitionInfos))
	require.Equal(t, 1, offs[1].PartitionInfos[0].PartitionID)
	require.Equal(t, 49, int(offs[1].PartitionInfos[0].Offset))
	require.Equal(t, 1, int(seq))

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

	err = cl.RegisterL0Table(seq, regEntry)
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

	// There can be more than one table due to the topics that were created
	found := false
	for _, r1 := range res {
		for _, r2 := range r1 {
			if bytes.Equal(tableID, r2.ID) {
				found = true
			}
		}
	}
	require.True(t, found)

	// Now get offset infos
	offsetInfos, err := cl.GetOffsetInfos([]offsets.GetOffsetTopicInfo{
		{
			TopicID:      1000,
			PartitionIDs: []int{1, 2},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsetInfos))
	require.Equal(t, 1000, offsetInfos[0].TopicID)
	require.Equal(t, 2, len(offsetInfos[0].PartitionInfos))
	require.Equal(t, 1, offsetInfos[0].PartitionInfos[0].PartitionID)
	require.Equal(t, 99, int(offsetInfos[0].PartitionInfos[0].Offset))
	require.Equal(t, 2, offsetInfos[0].PartitionInfos[1].PartitionID)
	require.Equal(t, 149, int(offsetInfos[0].PartitionInfos[1].Offset))
}

func TestControllerGroupEpochs(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controllers[0])

	cl, err := controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	numGroups := 10
	var groupIDs []string
	for i := 0; i < numGroups; i++ {
		groupIDs = append(groupIDs, uuid.New().String())
	}

	var epochInfos []EpochInfo
	for _, groupID := range groupIDs {
		memberID, address, groupEpoch, err := cl.GetCoordinatorInfo(groupID)
		require.NoError(t, err)
		require.Equal(t, controllers[0].memberID, memberID)
		require.Equal(t, "kafka-address-0:1234", address)
		require.Equal(t, 1, groupEpoch)

		epochInfos = append(epochInfos, EpochInfo{
			Key:   groupID,
			Epoch: 1,
		})
	}

	offs, seq, epochsOK, err := cl.PrePush([]offsets.GenerateOffsetTopicInfo{}, epochInfos)
	require.NoError(t, err)
	require.Equal(t, 0, len(offs))
	require.Equal(t, 1, int(seq))

	require.Equal(t, numGroups, len(epochInfos))
	for _, ok := range epochsOK {
		require.True(t, ok)
	}

	// Now try with incorrect epoch infos
	for i := 0; i < len(epochInfos); i++ {
		if i%2 == 0 {
			epochInfos[i].Epoch++
		}
	}

	offs, seq, epochsOK, err = cl.PrePush([]offsets.GenerateOffsetTopicInfo{}, epochInfos)
	require.NoError(t, err)
	require.Equal(t, 0, len(offs))
	require.Equal(t, 2, int(seq))

	require.Equal(t, numGroups, len(epochInfos))
	for i, ok := range epochsOK {
		require.Equal(t, ok, i%2 != 0)
	}
}

func TestControllerCreateGetDeleteTopics(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	controllers, _, tearDown := setupControllersWithObjectStore(t, 1, objStore)

	updateMembership(t, 1, 1, controllers, 0)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	numTopics := 100
	var infos []topicmeta.TopicInfo

	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		_, _, exists, _ := cl.GetTopicInfo(topicName)
		require.False(t, exists)
		info := topicmeta.TopicInfo{
			Name:           topicName,
			ID:             1000 + i,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = cl.CreateTopic(info)
		require.NoError(t, err)
		received, _, exists, err := cl.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
		infos = append(infos, info)
	}

	// Now restart
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	updateMembership(t, 1, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)

	// Topics should still be there
	for _, info := range infos {
		received, _, exists, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
	}

	// Now delete half of them
	for i := 0; i < len(infos)/2; i++ {
		info := infos[i]
		err = cl.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, exists, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}

	// Rest should still be there
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		received, _, exists, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
	}

	// Restart again
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	updateMembership(t, 1, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)

	for i, info := range infos {
		received, _, exists, err := cl.GetTopicInfo(info.Name)
		if i < len(infos)/2 {
			require.NoError(t, err)
			require.False(t, exists)
		} else {
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, info, received)
		}
	}

	// Delete the rest
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		err = cl.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, exists, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}

	// Restart again
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	defer tearDown(t)
	updateMembership(t, 1, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err = cl.Close()
		require.NoError(t, err)
	}()

	// Should be none
	for _, info := range infos {
		_, _, exists, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestControllerGetAllTopicInfos(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	controllers, _, tearDown := setupControllersWithObjectStore(t, 1, objStore)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	numTopics := 100
	var infos []topicmeta.TopicInfo

	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := topicmeta.TopicInfo{
			Name:           topicName,
			ID:             1000 + i,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = cl.CreateTopic(info)
		require.NoError(t, err)
		infos = append(infos, info)
	}

	allInfos, err := cl.GetAllTopicInfos()
	require.NoError(t, err)
	sort.Slice(allInfos, func(i, j int) bool {
		return allInfos[i].ID < allInfos[j].ID
	})

	require.Equal(t, infos, allInfos)
}

func TestControllerGenerateSequence(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controllers[0])

	cl, err := controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	numSequences := 10
	numVals := 200
	for i := 0; i < numVals; i++ {
		for j := 0; j < numSequences; j++ {
			seq, err := cl.GenerateSequence(fmt.Sprintf("test-sequence%d", j))
			require.NoError(t, err)
			require.Equal(t, i, int(seq))
		}
	}
}

func setupControllers(t *testing.T, numMembers int) ([]*Controller, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	controllers, _, tearDown := setupControllersWithObjectStore(t, numMembers, objStore)
	return controllers, tearDown
}

func setupControllersWithObjectStore(t *testing.T, numMembers int,
	objStore objstore.Client) ([]*Controller, *transport.LocalTransports, func(t *testing.T)) {
	return setupControllersWithObjectStoreAndConfigSetter(t, numMembers, objStore, nil)
}

func setupControllersWithObjectStoreAndConfigSetter(t *testing.T, numMembers int,
	objStore objstore.Client, configSetter func(conf *Conf)) ([]*Controller, *transport.LocalTransports, func(t *testing.T)) {
	localTransports := transport.NewLocalTransports()
	var controllers []*Controller
	for i := 0; i < numMembers; i++ {
		address := uuid.New().String()
		transportServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		cfg := NewConf()
		// Set to a high number as we don't have compaction running and don't want to block L0 adds
		cfg.LsmConf.L0MaxTablesBeforeBlocking = 10000
		if configSetter != nil {
			configSetter(&cfg)
		}
		ctrl := NewController(cfg, objStore, localTransports.CreateConnection, transportServer)
		err = ctrl.Start()
		require.NoError(t, err)
		controllers = append(controllers, ctrl)
		transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicAdded,
			func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte,
				responseWriter transport.ResponseWriter) error {
				return responseWriter(responseBuff, nil)
			})
		transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicDeleted,
			func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte,
				responseWriter transport.ResponseWriter) error {
				return responseWriter(responseBuff, nil)
			})
	}
	return controllers, localTransports, func(t *testing.T) {
		for _, controller := range controllers {
			err := controller.Stop()
			require.NoError(t, err)
		}
	}
}

func updateMembership(t *testing.T, clusterVersion int, leaderVersion int, controllers []*Controller,
	memberIndexes ...int) []cluster.MembershipEntry {
	newState := createMembership(clusterVersion, leaderVersion, controllers, memberIndexes...)
	for i, mgr := range controllers {
		err := mgr.MembershipChanged(int32(i), newState)
		require.NoError(t, err)
	}
	return newState.Members
}

func createMembership(clusterVersion int, leaderVersion int, controllers []*Controller, memberIndexes ...int) cluster.MembershipState {
	now := time.Now().UnixMilli()
	var members []cluster.MembershipEntry
	for i, memberIndex := range memberIndexes {
		membershipData := common.MembershipData{
			ClusterListenAddress: controllers[memberIndex].transportServer.Address(),
			// Make up a fake kafka address
			KafkaListenerAddress: fmt.Sprintf("kafka-address-%d:1234", i),
			Location:             controllers[i].cfg.AzInfo,
		}
		members = append(members, cluster.MembershipEntry{
			ID:         int32(memberIndex),
			Data:       membershipData.Serialize(nil),
			UpdateTime: now,
		})
	}
	return cluster.MembershipState{
		ClusterVersion: clusterVersion,
		LeaderVersion:  leaderVersion,
		Members:        members,
	}
}

func setupTopics(t *testing.T, controller *Controller) {
	cl, err := controller.Client()
	require.NoError(t, err)
	err = cl.CreateTopic(topicmeta.TopicInfo{
		Name:           "topic1",
		PartitionCount: 4,
		RetentionTime:  1232123,
	})
	require.NoError(t, err)
	err = cl.CreateTopic(topicmeta.TopicInfo{
		Name:           "topic2",
		PartitionCount: 2,
		RetentionTime:  34464646,
	})
	require.NoError(t, err)
}
