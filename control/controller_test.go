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
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)
	updateMembership(t, 1, controllers, 0)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	batch := createBatch(1, tableID, keyStart, keyEnd)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	// Now update membership again so cluster version increases
	updateMembership(t, 2, controllers, 0)

	err = cl.ApplyLsmChanges(batch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - cluster version mismatch", err.Error())

	_, err = cl.QueryTablesInRange(nil, nil)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
	require.Equal(t, "controller - cluster version mismatch", err.Error())
}

func TestControllerUseClosedClient(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, controllers, 0)

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

	updateMembership(t, 1, controllers, 0)

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

func TestControllerRegisterL0(t *testing.T) {
	controllers, tearDown := setupControllers(t, 1)
	defer tearDown(t)

	updateMembership(t, 1, controllers, 0)
	setupTopics(t, controllers[0])

	cl, err := controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	// First get some offsets
	offs, err := cl.GetOffsets([]offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
				{
					PartitionID: 2,
					NumOffsets:  100,
				},
			},
		},
		{
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
			},
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
	writtenOffs := []offsets.UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[0],
					NumOffsets:  100,
				},
				{
					PartitionID: 2,
					OffsetStart: offs[1],
					NumOffsets:  100,
				},
			},
		},
		{
			TopicID: 1,
			PartitionInfos: []offsets.UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[2],
					NumOffsets:  100,
				},
			},
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
}

func TestControllerCreateGetDeleteTopics(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	controllers, _, tearDown := setupControllersWithObjectStore(t, 1, objStore)

	updateMembership(t, 1, controllers, 0)

	cl, err := controllers[0].Client()
	require.NoError(t, err)

	numTopics := 100
	var infos []topicmeta.TopicInfo

	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		_, _, err := cl.GetTopicInfo(topicName)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
		info := topicmeta.TopicInfo{
			Name:           topicName,
			ID:             i,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = cl.CreateTopic(info)
		require.NoError(t, err)
		received, _, err := cl.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.Equal(t, info, received)
		infos = append(infos, info)
	}

	// Now restart
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	updateMembership(t, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)

	// Topics should still be there
	for _, info := range infos {
		received, _, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.Equal(t, info, received)
	}

	// Now delete half of them
	for i := 0; i < len(infos)/2; i++ {
		info := infos[i]
		err = cl.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, err := cl.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
	}

	// Rest should still be there
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		received, _, err := cl.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.Equal(t, info, received)
	}

	// Restart again
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	updateMembership(t, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)

	for i, info := range infos {
		received, _, err := cl.GetTopicInfo(info.Name)
		if i < len(infos)/2 {
			require.Error(t, err)
			require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
		} else {
			require.NoError(t, err)
			require.Equal(t, info, received)
		}
	}

	// Delete the rest
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		err = cl.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, err := cl.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
	}

	// Restart again
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controllers, _, tearDown = setupControllersWithObjectStore(t, 1, objStore)
	defer tearDown(t)
	updateMembership(t, 1, controllers, 0)
	cl, err = controllers[0].Client()
	require.NoError(t, err)
	defer func() {
		err = cl.Close()
		require.NoError(t, err)
	}()

	// Should be none
	for _, info := range infos {
		_, _, err := cl.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
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

func updateMembership(t *testing.T, clusterVersion int, controllers []*Controller,
	memberIndexes ...int) []cluster.MembershipEntry {
	newState := createMembership(clusterVersion, controllers, memberIndexes...)
	for _, mgr := range controllers {
		err := mgr.MembershipChanged(newState)
		require.NoError(t, err)
	}
	return newState.Members
}

func createMembership(clusterVersion int, controllers []*Controller, memberIndexes ...int) cluster.MembershipState {
	now := time.Now().UnixMilli()
	var members []cluster.MembershipEntry
	for _, memberIndex := range memberIndexes {
		members = append(members, cluster.MembershipEntry{
			Address:    controllers[memberIndex].transportServer.Address(),
			UpdateTime: now,
		})
	}
	return cluster.MembershipState{
		ClusterVersion: clusterVersion,
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
