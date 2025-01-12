package control

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSinglePartitionTableNotification(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	// trigger a notification
	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	waitForNotifications(t, receiver, 1)

	verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, receiver.getNotifications()[0])
}

func TestMultiplePartitionTableNotification(t *testing.T) {
	testMultiplePartitionTableNotification(t, 1000, 3)
	testMultiplePartitionTableNotification(t, 1000, 2)
	testMultiplePartitionTableNotification(t, 1001, 1)
}

func testMultiplePartitionTableNotification(t *testing.T, topicID int, partitionID int) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(topicID, partitionID, receiver.memberID, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
				{
					PartitionID: 2,
					NumOffsets:  1000,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  10,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	waitForNotifications(t, receiver, 1)

	notif := receiver.getNotifications()[0]

	verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, notif)
}

func TestRegisterTableListenerReturnsLRO(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	lro, err := cl.RegisterTableListener(1000, 1, receiver.memberID, 0)
	require.NoError(t, err)
	require.Equal(t, -1, int(lro))

	// trigger a notification

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  125,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	waitForNotifications(t, receiver, 1)
	notif := receiver.getNotifications()[0]
	verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, notif)

	lro, err = cl.RegisterTableListener(1000, 1, receiver.memberID, 0)
	require.NoError(t, err)
	require.Equal(t, 124, int(lro))
}

func TestMultipleRegistrations(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for more than one partition

	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(1000, 2, receiver.memberID, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(1001, 1, receiver.memberID, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(1001, 0, receiver.memberID, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
				{
					PartitionID: 2,
					NumOffsets:  1000,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  10,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	waitForNotifications(t, receiver, 1)

	time.Sleep(10 * time.Millisecond)

	notifs := receiver.getNotifications()

	// Should only be received once
	require.Equal(t, 1, len(notifs))

	verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, notifs[0])
}

func TestMultipleReceivers(t *testing.T) {
	numReceivers := 10
	cl, receivers, tearDown := setupAndRegisterReceivers(t, numReceivers)
	defer tearDown(t)

	for _, receiver := range receivers {
		_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
		require.NoError(t, err)
	}

	// trigger a notification

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
				{
					PartitionID: 2,
					NumOffsets:  1000,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  10,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	for _, receiver := range receivers {

		waitForNotifications(t, receiver, 1)

		notifs := receiver.getNotifications()

		require.Equal(t, 1, len(notifs))

		verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, notifs[0])
	}

}

func TestNotRegisteredForPartition(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for different partition
	_, err := cl.RegisterTableListener(1000, 1, receiver.memberID, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
				{
					PartitionID: 2,
					NumOffsets:  1000,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  10,
				},
			},
		},
	}

	triggerTableAddedNotification(t, cl, offsetInfos)

	// Shouldn't get notification
	time.Sleep(50 * time.Millisecond)
	notifs := receiver.getNotifications()
	require.Equal(t, 0, len(notifs))
}

func TestMultipleNotifications(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	numNotifs := 10
	for i := 0; i < numNotifs; i++ {
		offsetInfos := []offsets.GenerateOffsetTopicInfo{
			{
				TopicID: 1000,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{
						PartitionID: 3,
						NumOffsets:  125,
					},
					{
						PartitionID: 2,
						NumOffsets:  1000,
					},
				},
			},
			{
				TopicID: 1001,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{
						PartitionID: 1,
						NumOffsets:  10,
					},
				},
			},
		}

		tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

		waitForNotifications(t, receiver, i+1)

		notifs := receiver.getNotifications()
		notif := notifs[len(notifs)-1]

		verifyTableRegisteredNotification(t, i, tableID, writtenOffs, notif)
	}

}

func TestNotificationLeaderVersion(t *testing.T) {
	cl, receivers, tearDown := setupAndRegisterReceiversWithLeaderVersion(t, 1, 23)
	receiver := receivers[0]
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
			},
		},
	}
	triggerTableAddedNotification(t, cl, offsetInfos)

	waitForNotifications(t, receiver, 1)

	notifs := receiver.getNotifications()
	notif := notifs[0]

	require.Equal(t, 23, notif.LeaderVersion)
}

func TestInvalidateListeners(t *testing.T) {
	numReceivers := 3

	cl, receivers, tearDown := setupAndRegisterReceivers(t, numReceivers)
	defer tearDown(t)
	for _, receiver := range receivers {
		_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
		require.NoError(t, err)
	}

	// send some notifications
	numNotifs := 10
	for i := 0; i < numNotifs; i++ {
		offsetInfos := []offsets.GenerateOffsetTopicInfo{
			{
				TopicID: 1000,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{
						PartitionID: 3,
						NumOffsets:  125,
					},
					{
						PartitionID: 2,
						NumOffsets:  1000,
					},
				},
			},
			{
				TopicID: 1001,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{
						PartitionID: 1,
						NumOffsets:  10,
					},
				},
			},
		}

		tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

		for _, receiver := range receivers {
			waitForNotifications(t, receiver, i+1)

			notifs := receiver.getNotifications()
			notif := notifs[len(notifs)-1]

			verifyTableRegisteredNotification(t, i, tableID, writtenOffs, notif)
		}
	}

	// Invalidate the first one by sending next resetSequence
	_, err := cl.RegisterTableListener(1000, 3, receivers[0].memberID, 1)
	require.NoError(t, err)

	// Send another notification
	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
				{
					PartitionID: 2,
					NumOffsets:  1000,
				},
			},
		},
		{
			TopicID: 1001,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  10,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	for i, receiver := range receivers {
		waitForNotifications(t, receiver, numNotifs+1)

		notifs := receiver.getNotifications()
		notif := notifs[len(notifs)-1]

		var expectedSeq int
		if i == 0 {
			// The first one should have had listeners invalidated and sequence reset
			expectedSeq = 0
		} else {
			expectedSeq = numNotifs
		}
		verifyTableRegisteredNotification(t, expectedSeq, tableID, writtenOffs, notif)
	}
}

func TestListenersRemovedOnMembershipChange(t *testing.T) {

	numReceivers := 3

	objStore := dev.NewInMemStore(0)
	controllers, _, tearDown := setupControllersWithObjectStore(t, 3, objStore)
	defer tearDown(t)
	for _, controller := range controllers {
		controller.transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite,
			func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
				return responseWriter(responseBuff, nil)
			})
	}

	updateMembership(t, 1, 1, controllers, 0, 1, 2)

	var receivers []*notificationReceiver
	for i := 0; i < numReceivers; i++ {
		memberID := controllers[i].MemberID()
		receiver := &notificationReceiver{
			memberID: memberID,
		}
		controllers[i].transportServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)
		receivers = append(receivers, receiver)
	}

	controller := controllers[0]
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)

	for _, receiver := range receivers {
		_, err = cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
		require.NoError(t, err)
	}

	// send a notification
	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  125,
				},
			},
		},
	}

	tableID, writtenOffs := triggerTableAddedNotification(t, cl, offsetInfos)

	// Make sure notification received by all receivers
	for _, receiver := range receivers {
		waitForNotifications(t, receiver, 1)
		notifs := receiver.getNotifications()
		verifyTableRegisteredNotification(t, 0, tableID, writtenOffs, notifs[0])
	}

	// Make sure all receivers are registered
	for _, receiver := range receivers {
		require.True(t, controller.tableListeners.hasListenerForMemberID(receiver.memberID))
	}

	// Now remove member 1 from cluster

	newState := createMembership(2, 1, controllers, 0, 2)
	err = controller.MembershipChanged(controllers[0].MemberID(), newState)
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)
	cl, err = controller.Client()
	require.NoError(t, err)

	require.True(t, controller.tableListeners.hasListenerForMemberID(receivers[0].memberID))
	require.False(t, controller.tableListeners.hasListenerForMemberID(receivers[1].memberID))
	require.True(t, controller.tableListeners.hasListenerForMemberID(receivers[2].memberID))

	// Now remove one more

	newState = createMembership(2, 1, controllers, 0)
	err = controller.MembershipChanged(controllers[0].MemberID(), newState)
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)
	cl, err = controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	require.True(t, controller.tableListeners.hasListenerForMemberID(receivers[0].memberID))
	require.False(t, controller.tableListeners.hasListenerForMemberID(receivers[1].memberID))
	require.False(t, controller.tableListeners.hasListenerForMemberID(receivers[2].memberID))
}

func TestPeriodicNotification(t *testing.T) {
	interval := 1 * time.Millisecond
	cl, receivers, tearDown := setupAndRegisterReceiversWithLeaderVersionAndConfigSetter(t, 1, 1, func(conf *Conf) {
		conf.TableNotificationInterval = interval
	})
	defer tearDown(t)
	receiver := receivers[0]

	// register for notifications
	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	testutils.WaitUntil(t, func() (bool, error) {
		return len(receiver.getNotifications()) >= 10, nil
	})

	notifs := receiver.getNotifications()
	for i, notif := range notifs {
		require.Equal(t, i, int(notif.Sequence))
		// should be empty
		require.Equal(t, 0, len(notif.Infos))
	}
}

func TestNotificationWithMultipleTables(t *testing.T) {
	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(1000, 3, receiver.memberID, 0)
	require.NoError(t, err)

	offsetInfos := []offsets.GenerateOffsetTopicInfo{
		{
			TopicID: 1000,
			PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  100,
				},
			},
		},
	}

	numRegs := 10
	for i := 0; i < numRegs; i++ {
		_, seq, _, err := cl.PrePush(offsetInfos, nil)
		require.NoError(t, err)
		require.Equal(t, i+1, int(seq))
	}

	// Now register in reverse order which will cause them to be delayed and re-ordered
	var tableIds []sst.SSTableID
	for i := numRegs - 1; i >= 0; i-- {
		regEntry := createRegEntry()
		err = cl.RegisterL0Table(int64(i+1), regEntry)
		require.NoError(t, err)
		tableIds = append([]sst.SSTableID{regEntry.TableID}, tableIds...)
	}

	testutils.WaitUntil(t, func() (bool, error) {
		return len(receiver.getNotifications()) == 1, nil
	})

	notif := receiver.getNotifications()[0]
	require.Equal(t, numRegs, len(notif.TableIDs))
	require.Equal(t, tableIds, notif.TableIDs)
}

func setupAndRegisterReceiver(t *testing.T) (Client, *notificationReceiver, func(t *testing.T)) {
	cl, receivers, tearDown := setupAndRegisterReceivers(t, 1)
	return cl, receivers[0], tearDown
}

func setupAndRegisterReceivers(t *testing.T, numReceivers int) (Client, []*notificationReceiver, func(t *testing.T)) {
	return setupAndRegisterReceiversWithLeaderVersion(t, numReceivers, 1)
}

func setupAndRegisterReceiversWithLeaderVersion(t *testing.T, numReceivers int, leaderVersion int) (Client, []*notificationReceiver, func(t *testing.T)) {
	return setupAndRegisterReceiversWithLeaderVersionAndConfigSetter(t, numReceivers, leaderVersion, func(conf *Conf) {})
}

func setupAndRegisterReceiversWithLeaderVersionAndConfigSetter(t *testing.T, numReceivers int, leaderVersion int,
	configSetter func(conf *Conf)) (Client, []*notificationReceiver, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStoreAndConfigSetter(t, 1, objStore, configSetter)
	for _, controller := range controllers {
		controller.transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite,
			func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
				return responseWriter(responseBuff, nil)
			})
	}
	controller := controllers[0]

	membership := createMembership(1, leaderVersion, controllers, 0)

	var receivers []*notificationReceiver
	for i := 0; i < numReceivers; i++ {
		receiverAddress := uuid.New().String()
		receiverServer, err := localTransports.NewLocalServer(receiverAddress)
		require.NoError(t, err)
		receiverServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite,
			func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
				return responseWriter(responseBuff, nil)
			})
		memberID := int32(i)
		receiver := &notificationReceiver{
			memberID: memberID,
		}
		receivers = append(receivers, receiver)
		receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)
		membershipData := common.MembershipData{ClusterListenAddress: receiverAddress}
		membership.Members = append(membership.Members, cluster.MembershipEntry{
			ID:         memberID,
			Data:       membershipData.Serialize(nil),
			UpdateTime: time.Now().UnixMilli(),
		})
	}
	err := controllers[0].MembershipChanged(0, membership)
	require.NoError(t, err)

	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)

	return cl, receivers, func(t *testing.T) {
		err := cl.Close()
		require.NoError(t, err)
		tearDown(t)
	}
}

func waitForNotifications(t *testing.T, receiver *notificationReceiver, numNotifications int) {
	testutils.WaitUntil(t, func() (bool, error) {
		return len(receiver.getNotifications()) == numNotifications, nil
	})
}

func verifyTableRegisteredNotification(t *testing.T, sequence int, tableID sst.SSTableID, offs []offsets.OffsetTopicInfo, notif TablesRegisteredNotification) {
	require.Equal(t, sequence, int(notif.Sequence))
	require.Equal(t, []sst.SSTableID{tableID}, notif.TableIDs)
	require.Equal(t, len(offs), len(notif.Infos))
	for i, topicInfo := range offs {
		require.Equal(t, topicInfo.TopicID, notif.Infos[i].TopicID)
		require.Equal(t, len(topicInfo.PartitionInfos), len(notif.Infos[i].PartitionInfos))
		for j, partInfo := range topicInfo.PartitionInfos {
			require.Equal(t, partInfo.PartitionID, notif.Infos[i].PartitionInfos[j].PartitionID)
			require.Equal(t, partInfo.Offset, notif.Infos[i].PartitionInfos[j].Offset)
		}
	}
}

func triggerTableAddedNotification(t *testing.T, cl Client, offInfos []offsets.GenerateOffsetTopicInfo) (sst.SSTableID, []offsets.OffsetTopicInfo) {
	offs, seq, _, err := cl.PrePush(offInfos, nil)
	require.NoError(t, err)

	regEntry := createRegEntry()

	// cause a notification to be sent
	err = cl.RegisterL0Table(seq, regEntry)
	require.NoError(t, err)

	return regEntry.TableID, offs
}

func createRegEntry() lsm.RegistrationEntry {
	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	tableID := []byte(uuid.New().String())
	return lsm.RegistrationEntry{
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
}

type notificationReceiver struct {
	lock     sync.Mutex
	received []TablesRegisteredNotification
	memberID int32
}

func (n *notificationReceiver) receivedNotification(_ *transport.ConnectionContext, request []byte, _ []byte,
	_ transport.ResponseWriter) error {
	var notif TablesRegisteredNotification
	notif.Deserialize(request, 0)
	n.lock.Lock()
	defer n.lock.Unlock()
	n.received = append(n.received, notif)
	return nil
}

func (n *notificationReceiver) getNotifications() []TablesRegisteredNotification {
	n.lock.Lock()
	defer n.lock.Unlock()
	copied := make([]TablesRegisteredNotification, len(n.received))
	copy(copied, n.received)
	return copied
}
