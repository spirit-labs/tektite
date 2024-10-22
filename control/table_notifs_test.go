package control

import (
	"github.com/google/uuid"
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
	_, err := cl.RegisterTableListener(0, 3, receiver.address, 0)
	require.NoError(t, err)

	// trigger a notification
	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
	testMultiplePartitionTableNotification(t, 0, 3)
	testMultiplePartitionTableNotification(t, 0, 2)
	testMultiplePartitionTableNotification(t, 1, 1)
}

func testMultiplePartitionTableNotification(t *testing.T, topicID int, partitionID int) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for notifications
	_, err := cl.RegisterTableListener(topicID, partitionID, receiver.address, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	lro, err := cl.RegisterTableListener(0, 1, receiver.address, 0)
	require.NoError(t, err)
	require.Equal(t, -1, int(lro))

	// trigger a notification

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	lro, err = cl.RegisterTableListener(0, 1, receiver.address, 0)
	require.NoError(t, err)
	require.Equal(t, 124, int(lro))
}

func TestMultipleRegistrations(t *testing.T) {

	cl, receiver, tearDown := setupAndRegisterReceiver(t)
	defer tearDown(t)

	// register for more than one partition

	_, err := cl.RegisterTableListener(0, 3, receiver.address, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(0, 2, receiver.address, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(1, 1, receiver.address, 0)
	require.NoError(t, err)

	_, err = cl.RegisterTableListener(1, 0, receiver.address, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStore(t, 1, objStore)
	defer tearDown(t)
	controller := controllers[0]

	var receivers []*notificationReceiver
	for i := 0; i < numReceivers; i++ {
		address := uuid.New().String()
		receiverServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		receiver := &notificationReceiver{
			address: address,
		}
		receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)
		receivers = append(receivers, receiver)
	}

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	for _, receiver := range receivers {
		_, err = cl.RegisterTableListener(0, 3, receiver.address, 0)
		require.NoError(t, err)
	}

	// trigger a notification

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
	_, err := cl.RegisterTableListener(0, 1, receiver.address, 0)
	require.NoError(t, err)

	// trigger a notification

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
	_, err := cl.RegisterTableListener(0, 3, receiver.address, 0)
	require.NoError(t, err)

	numNotifs := 10
	for i := 0; i < numNotifs; i++ {
		offsetInfos := []offsets.GetOffsetTopicInfo{
			{
				TopicID: 0,
				PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
				TopicID: 1,
				PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStore(t, 1, objStore)
	controller := controllers[0]
	defer tearDown(t)

	address := uuid.New().String()
	receiverServer, err := localTransports.NewLocalServer(address)
	require.NoError(t, err)
	receiver := &notificationReceiver{
		address: address,
	}
	receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)

	updateMembership(t, 1, 23, controllers, 0)
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	// register for notifications
	_, err = cl.RegisterTableListener(0, 3, receiver.address, 0)
	require.NoError(t, err)

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStore(t, 1, objStore)
	defer tearDown(t)
	controller := controllers[0]

	var receivers []*notificationReceiver
	for i := 0; i < numReceivers; i++ {
		address := uuid.New().String()
		receiverServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		receiver := &notificationReceiver{
			address: address,
		}
		receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)
		receivers = append(receivers, receiver)
	}

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	for _, receiver := range receivers {
		_, err = cl.RegisterTableListener(0, 3, receiver.address, 0)
		require.NoError(t, err)
	}

	// send some notifications
	numNotifs := 10
	for i := 0; i < numNotifs; i++ {
		offsetInfos := []offsets.GetOffsetTopicInfo{
			{
				TopicID: 0,
				PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
				TopicID: 1,
				PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
	_, err = cl.RegisterTableListener(0, 3, receivers[0].address, 1)
	require.NoError(t, err)

	// Send another notification
	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
			TopicID: 1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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

	var receivers []*notificationReceiver
	for i := 0; i < numReceivers; i++ {
		address := controllers[i].transportServer.Address()
		receiver := &notificationReceiver{
			address: address,
		}
		controllers[i].transportServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)
		receivers = append(receivers, receiver)
	}

	updateMembership(t, 1, 1, controllers, 0, 1, 2)

	controller := controllers[0]
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)

	for _, receiver := range receivers {
		_, err = cl.RegisterTableListener(0, 3, receiver.address, 0)
		require.NoError(t, err)
	}

	// send a notification
	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
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
		require.True(t, controller.tableListeners.hasListenerForAddress(receiver.address))
	}

	// Now remove member 1 from cluster

	newState := createMembership(2, 1, controllers, 0, 2)
	err = controller.MembershipChanged(newState)
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)
	cl, err = controller.Client()
	require.NoError(t, err)

	require.True(t, controller.tableListeners.hasListenerForAddress(receivers[0].address))
	require.False(t, controller.tableListeners.hasListenerForAddress(receivers[1].address))
	require.True(t, controller.tableListeners.hasListenerForAddress(receivers[2].address))

	// Now remove one more

	newState = createMembership(2, 1, controllers, 0)
	err = controller.MembershipChanged(newState)
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)
	cl, err = controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()

	require.True(t, controller.tableListeners.hasListenerForAddress(receivers[0].address))
	require.False(t, controller.tableListeners.hasListenerForAddress(receivers[1].address))
	require.False(t, controller.tableListeners.hasListenerForAddress(receivers[2].address))
}

func TestPeriodicNotification(t *testing.T) {

	interval := 1 * time.Millisecond
	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStoreAndConfigSetter(t, 1, objStore, func(conf *Conf) {
		conf.TableNotificationInterval = interval
	})
	defer tearDown(t)

	address := uuid.New().String()
	receiverServer, err := localTransports.NewLocalServer(address)
	require.NoError(t, err)
	receiver := &notificationReceiver{
		address: address,
	}
	receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)

	controller := controllers[0]
	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controller)

	cl, err := controllers[0].Client()
	require.NoError(t, err)
	// register for notifications
	_, err = cl.RegisterTableListener(0, 3, receiver.address, 0)
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
	_, err := cl.RegisterTableListener(0, 3, receiver.address, 0)
	require.NoError(t, err)

	offsetInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: 0,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  100,
				},
			},
		},
	}

	numRegs := 10
	for i := 0; i < numRegs; i++ {
		_, seq, err := cl.GetOffsets(offsetInfos)
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
	objStore := dev.NewInMemStore(0)
	controllers, localTransports, tearDown := setupControllersWithObjectStore(t, 1, objStore)
	controller := controllers[0]

	address := uuid.New().String()
	receiverServer, err := localTransports.NewLocalServer(address)
	require.NoError(t, err)
	receiver := &notificationReceiver{
		address: address,
	}
	receiverServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, receiver.receivedNotification)

	updateMembership(t, 1, 1, controllers, 0)
	setupTopics(t, controller)

	cl, err := controller.Client()
	require.NoError(t, err)

	return cl, receiver, func(t *testing.T) {
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

func triggerTableAddedNotification(t *testing.T, cl Client, offInfos []offsets.GetOffsetTopicInfo) (sst.SSTableID, []offsets.OffsetTopicInfo) {
	offs, seq, err := cl.GetOffsets(offInfos)
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
	address  string
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
