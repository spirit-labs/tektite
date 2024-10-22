package control

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"time"
)

type tableListeners struct {
	lock                    sync.RWMutex
	notificationInterval    time.Duration
	partitionListeners      map[int]map[int][]int
	tableAddedListeners     map[int]*tableAddedListener
	memberIDToListenerIDMap map[int32]int
	listenerIDSequence      int
	notifTimer              *time.Timer
	membersMap              map[int32]struct{}
	connectionFactory       transport.ConnectionFactory
	started                 bool
	leaderVersion           int
}

type tableAddedListener struct {
	lock          sync.Mutex
	connFactory   transport.ConnectionFactory
	connection    transport.Connection
	address       string
	memberID      int32
	resetSequence int64
	sequence      int64
	lastSentTime  uint64
}

const notificationWriteTimeout = 2 * time.Second

func newTableListeners(notificationInterval time.Duration, connectionFactory transport.ConnectionFactory) *tableListeners {
	return &tableListeners{
		partitionListeners:      map[int]map[int][]int{},
		tableAddedListeners:     map[int]*tableAddedListener{},
		memberIDToListenerIDMap: map[int32]int{},
		membersMap:              make(map[int32]struct{}),
		notificationInterval:    notificationInterval,
		connectionFactory:       connectionFactory,
	}
}

func (t *tableListeners) start() {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.started {
		return
	}
	t.scheduleNotifTimer()
	t.started = true
}

func (t *tableListeners) stop() {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.started {
		return
	}
	for _, listener := range t.tableAddedListeners {
		listener.closeConnection()
	}
	t.notifTimer.Stop()
	t.started = false
}

func (t *tableListeners) scheduleNotifTimer() {
	t.notifTimer = time.AfterFunc(t.notificationInterval, func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if !t.started {
			return
		}
		t.maybeSendEmptyNotification()
		t.scheduleNotifTimer()
	})
}

func (t *tableListeners) maybeSendEmptyNotification() {
	now := arista.NanoTime()
	period := uint64(t.notificationInterval.Nanoseconds())
	var notif TablesRegisteredNotification
	notif.LeaderVersion = t.leaderVersion
	buff := notif.Serialize(nil)
	for _, listener := range t.tableAddedListeners {
		listener.maybeSendPeriodicNotification(buff, now, period)
	}
}

func (t *tableListeners) membershipChanged(newState *cluster.MembershipState) {
	t.lock.Lock()
	defer t.lock.Unlock()
	membersMap := make(map[int32]struct{}, len(newState.Members))
	for _, member := range newState.Members {
		membersMap[member.ID] = struct{}{}
	}
	for memberID := range t.membersMap {
		_, exists := membersMap[memberID]
		if !exists {
			// member has left cluster
			t.unregisterListenersForMemberID(memberID)
		}
	}
	t.membersMap = membersMap
	t.leaderVersion = newState.LeaderVersion
}

func (t *tableListeners) unregisterListenersForMemberID(memberID int32) {
	listenerID, ok := t.memberIDToListenerIDMap[memberID]
	if !ok {
		// no listeners
		return
	}
	for _, partitionMap := range t.partitionListeners {
		for partitionID, listenerIDs := range partitionMap {
			// optimise for case where not found more often than found
			foundPos := -1
			for i, id := range listenerIDs {
				if id == listenerID {
					foundPos = i
					break
				}
			}
			if foundPos != -1 {
				// remove the listener
				newListeners := make([]int, 0, len(listenerIDs))
				for i, id := range listenerIDs {
					if id != listenerID {
						newListeners = append(newListeners, i)
					}
				}
				partitionMap[partitionID] = newListeners
			}
		}
	}
	delete(t.tableAddedListeners, listenerID)
	delete(t.memberIDToListenerIDMap, memberID)
}

func (t *tableListeners) hasListenerForMemberID(memberID int32) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, exists := t.memberIDToListenerIDMap[memberID]
	return exists
}

func (t *tableListeners) maybeRegisterListenerForPartition(memberID int32, address string, topicID int, partitionID int, resetSequence int64) {
	if t.isRegisteredForPartition(memberID, topicID, partitionID, resetSequence) {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	id, ok := t.memberIDToListenerIDMap[memberID]
	if !ok {
		id = t.listenerIDSequence
		t.listenerIDSequence++
		t.memberIDToListenerIDMap[memberID] = id
		t.tableAddedListeners[id] = &tableAddedListener{
			connFactory: t.connectionFactory,
			address:     address,
			memberID:    memberID,
		}
	}
	partitionMap, ok := t.partitionListeners[topicID]
	if !ok {
		partitionMap = map[int][]int{}
		t.partitionListeners[topicID] = partitionMap
	}
	partitionMap[partitionID] = append(partitionMap[partitionID], id)
}

func (t *tableListeners) isRegisteredForPartition(memberID int32, topicID int, partitionID int, resetSequence int64) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	id, ok := t.memberIDToListenerIDMap[memberID]
	if !ok {
		return false
	}
	listener, ok := t.tableAddedListeners[id]
	if !ok {
		return false
	}
	if listener.resetSequence != resetSequence {
		// The resetSequence has changed - the fetcher sends an incremented resetSequence when it detects a
		// discontinuity in the notification sequence, this causes table notification listeners to be invalidated.
		t.resetListener(listener, resetSequence)
	}
	partitionMap, ok := t.partitionListeners[topicID]
	if !ok {
		return false
	}
	agents, ok := partitionMap[partitionID]
	if !ok {
		return false
	}
	for _, a := range agents {
		if a == id {
			return true
		}
	}
	return false
}

func (t *tableListeners) resetListener(listener *tableAddedListener, resetSequence int64) {
	t.lock.RUnlock()
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		t.lock.RLock()
	}()
	t.unregisterListenersForMemberID(listener.memberID)
	listener.resetSequence = resetSequence
}

func (t *tableListeners) sendTableRegisteredNotification(tableIDs []sst.SSTableID, infos []offsets.OffsetTopicInfo) error {
	t.lock.RLock()
	defer t.lock.RUnlock()
	// Figure out which agents are interested in this notification
	agentIDs := map[int]struct{}{}
	for _, topicInfo := range infos {
		partitionAgents, ok := t.partitionListeners[topicInfo.TopicID]
		if ok {
			for _, partitionInfo := range topicInfo.PartitionInfos {
				ags, ok := partitionAgents[partitionInfo.PartitionID]
				if ok {
					for _, ag := range ags {
						agentIDs[ag] = struct{}{}
					}
				}
			}
		}
	}
	// Create notification
	notif := TablesRegisteredNotification{
		LeaderVersion: t.leaderVersion,
		TableIDs:      tableIDs,
		Infos:         infos,
	}
	// Look up agents and send notification. It is sent one way (fire and forget) so we don't block waiting for a
	// response from each agent.
	buff := notif.Serialize(nil)
	for agentID := range agentIDs {
		listener, ok := t.tableAddedListeners[agentID]
		if !ok {
			panic("cannot find agent listener")
		}
		if err := listener.sendNotificationNoLock(buff); err != nil {
			if common.IsUnavailableError(err) {
				// Sending a notification is best effort. If we there is a temporary network error then we log and
				// ignore. If the fetcher receives a gap in sequence number it will invalidate it's state.
				log.Warnf("unable to send controller readable offset notification: %v", err)
			} else {
				return err
			}
		}
	}
	return nil
}

func (l *tableAddedListener) getConnection() (transport.Connection, error) {
	if l.connection != nil {
		return l.connection, nil
	}
	conn, err := l.connFactory(l.address)
	if err != nil {
		return nil, err
	}
	socketConn, ok := conn.(*transport.SocketTransportConnection)
	if ok {
		// Set to a lower value than the default as we don't want to block too long
		socketConn.SetWriteTimeout(notificationWriteTimeout)
	}
	l.connection = conn
	return conn, nil
}

func (l *tableAddedListener) closeConnection() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.closeConnectionNoLock()
}

func (l *tableAddedListener) closeConnectionNoLock() {
	if l.connection != nil {
		if err := l.connection.Close(); err != nil {
			// Ignore
		}
		l.connection = nil
	}
}

func (l *tableAddedListener) maybeSendPeriodicNotification(buff []byte, now uint64, sendInterval uint64) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.lastSentTime == 0 || now+l.lastSentTime >= sendInterval {
		if err := l.sendNotificationNoLock(buff); err != nil {
			log.Warnf("failed to send periodic notification: %v", err)
		}
	}
}

func (l *tableAddedListener) sendNotificationNoLock(buff []byte) error {
	conn, err := l.getConnection()
	if err != nil {
		return err
	}
	// We copy the buffer and change the sequence to avoid serializing multiple times
	copied := make([]byte, len(buff))
	copy(copied, buff)
	// This assumes sequence is the first member in the serialized form
	binary.BigEndian.PutUint64(copied, uint64(l.sequence))
	l.sequence++
	l.lastSentTime = arista.NanoTime()
	if err := conn.SendOneway(transport.HandlerIDFetcherTableRegisteredNotification, copied); err != nil {
		l.closeConnectionNoLock()
		return err
	}
	return nil
}
