package kafkaserver

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/kafkaserver/protocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"math"
	"sync"
	"time"
)

/*
GroupCoordinator manages Kafka consumer groups.
*/
type GroupCoordinator struct {
	cfg                       *conf.Config
	processorProvider         processorProvider
	streamMgr                 streamMgr
	metaProvider              MetadataProvider
	groups                    map[string]*group
	groupsLock                sync.RWMutex
	timers                    sync.Map
	consumerOffsetsPPM        map[int]int
	forwarder                 batchForwarder
	partitionProcessorMapping map[int]int
}

const ConsumerOffsetsMappingID = "sys.consumer_offsets"
const ConsumerOffsetsPartitionCount = 10

var ConsumerOffsetsColumnNames = []string{"group_id", "topic_id", "partition_id", "k_offset"}
var ConsumerOffsetsColumnTypes = []types.ColumnType{types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
var ConsumerOffsetsSchema = evbatch.NewEventSchema(ConsumerOffsetsColumnNames, ConsumerOffsetsColumnTypes)

type batchForwarder interface {
	ForwardBatch(batch *proc.ProcessBatch, replicate bool, completionFunc func(error))
}

type streamMgr interface {
	RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int, slabID int,
		schema *opers.OperatorSchema, keyCols []string) error
	RegisterChangeListener(listener func(streamName string, deployed bool))
}

func NewGroupCoordinator(cfg *conf.Config, provider processorProvider, streamMgr streamMgr,
	metaProvider MetadataProvider, forwarder batchForwarder) (*GroupCoordinator, error) {
	schema := &opers.OperatorSchema{
		EventSchema:     ConsumerOffsetsSchema,
		PartitionScheme: opers.NewPartitionScheme(ConsumerOffsetsMappingID, ConsumerOffsetsPartitionCount, false, cfg.ProcessorCount),
	}
	keyCols := []string{"group_id", "topic_id", "partition_id"}
	if err := streamMgr.RegisterSystemSlab(ConsumerOffsetsMappingID, common.KafkaOffsetsReceiverID, -1,
		common.KafkaOffsetsSlabID, schema, keyCols); err != nil {
		return nil, err
	}
	return &GroupCoordinator{
		cfg:                cfg,
		processorProvider:  provider,
		streamMgr:          streamMgr,
		metaProvider:       metaProvider,
		groups:             map[string]*group{},
		consumerOffsetsPPM: schema.PartitionScheme.PartitionProcessorMapping,
		forwarder:          forwarder,
	}, nil
}

func (gc *GroupCoordinator) Start() error {
	return nil
}

func (gc *GroupCoordinator) Stop() error {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	for _, g := range gc.groups {
		g.stop()
	}
	return nil
}

func (gc *GroupCoordinator) FindCoordinator(groupID string) int {
	partition := gc.calcConsumerOffsetsPartition(groupID)
	// Then the group coordinator node is the node that hosts the leader of that partition
	return gc.processorProvider.NodeForPartition(partition, ConsumerOffsetsMappingID,
		ConsumerOffsetsPartitionCount)
}

func (gc *GroupCoordinator) calcConsumerOffsetsPartition(groupID string) int {
	// We choose a partition for the groupID
	n := common.DefaultHash([]byte(groupID))
	partition := int(n) % ConsumerOffsetsPartitionCount
	return partition
}

func (gc *GroupCoordinator) checkLeader(groupID string) bool {
	leaderNode := gc.FindCoordinator(groupID)
	return leaderNode == gc.cfg.NodeID
}

func (gc *GroupCoordinator) sendJoinError(complFunc JoinCompletion, errorCode int) {
	complFunc(JoinResult{ErrorCode: errorCode})
}

func (gc *GroupCoordinator) sendSyncError(complFunc SyncCompletion, errorCode int) {
	complFunc(errorCode, nil)
}

func (gc *GroupCoordinator) getState(groupID string) int {
	gc.groupsLock.RLock()
	defer gc.groupsLock.RUnlock()
	group, ok := gc.groups[groupID]
	if !ok {
		return -1
	}
	return group.getState()
}

func (gc *GroupCoordinator) groupHasMember(groupID string, memberID string) bool {
	gc.groupsLock.RLock()
	defer gc.groupsLock.RUnlock()
	group, ok := gc.groups[groupID]
	if !ok {
		return false
	}
	return group.hasMember(memberID)
}

func (gc *GroupCoordinator) JoinGroup(apiVersion int16, groupID string, clientID string, memberID string, protocolType string,
	protocols []ProtocolInfo, sessionTimeout time.Duration, rebalanceTimeout time.Duration, complFunc JoinCompletion) {
	if !gc.checkLeader(groupID) {
		gc.sendJoinError(complFunc, protocol.ErrorCodeNotCoordinator)
		return
	}
	if sessionTimeout < gc.cfg.KafkaMinSessionTimeout || sessionTimeout > gc.cfg.KafkaMaxSessionTimeout {
		gc.sendJoinError(complFunc, protocol.ErrorCodeInvalidSessionTimeout)
		return
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		g = gc.createGroup(groupID)
	}
	g.Join(apiVersion, clientID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout, complFunc)
}

func (gc *GroupCoordinator) SyncGroup(groupID string, memberID string, generationID int, assignments []AssignmentInfo,
	complFunc SyncCompletion) {
	if !gc.checkLeader(groupID) {
		gc.sendSyncError(complFunc, protocol.ErrorCodeNotCoordinator)
		return
	}
	if memberID == "" {
		gc.sendSyncError(complFunc, protocol.ErrorCodeUnknownMemberID)
		return
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		gc.sendSyncError(complFunc, protocol.ErrorCodeGroupIDNotFound)
		return
	}
	g.Sync(memberID, generationID, assignments, complFunc)
}

func (gc *GroupCoordinator) HeartbeatGroup(groupID string, memberID string, generationID int) int {
	if !gc.checkLeader(groupID) {
		return protocol.ErrorCodeNotCoordinator
	}
	if memberID == "" {
		return protocol.ErrorCodeUnknownMemberID
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		return protocol.ErrorCodeGroupIDNotFound
	}
	return g.Heartbeat(memberID, generationID)
}

type MemberLeaveInfo struct {
	MemberID        string
	GroupInstanceID *string
}

func (gc *GroupCoordinator) LeaveGroup(groupID string, leaveInfos []MemberLeaveInfo) int16 {
	if !gc.checkLeader(groupID) {
		return protocol.ErrorCodeNotCoordinator
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		return protocol.ErrorCodeGroupIDNotFound
	}
	return g.Leave(leaveInfos)
}

func (gc *GroupCoordinator) OffsetCommit(groupID string, memberID string, generationID int, topicNames []string,
	partitionIDs [][]int32, offsets [][]int64) [][]int16 {
	numTopics := len(partitionIDs)
	errorCodes := make([][]int16, numTopics)
	for i := 0; i < numTopics; i++ {
		errorCodes[i] = make([]int16, len(partitionIDs[i]))
	}
	if !gc.checkLeader(groupID) {
		return fillAllErrorCodes(protocol.ErrorCodeNotCoordinator, errorCodes)
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		return fillAllErrorCodes(protocol.ErrorCodeGroupIDNotFound, errorCodes)
	}
	return g.offsetCommit(memberID, generationID, topicNames, partitionIDs, offsets, errorCodes)
}

func (gc *GroupCoordinator) OffsetFetch(groupID string, topicNames []string,
	partitionIDs [][]int32) ([][]int64, [][]int16, int16) {
	numTopics := len(partitionIDs)
	errorCodes := make([][]int16, numTopics)
	for i := 0; i < numTopics; i++ {
		errorCodes[i] = make([]int16, len(partitionIDs[i]))
	}
	if !gc.checkLeader(groupID) {
		return nil, nil, protocol.ErrorCodeNotCoordinator
	}
	gc.groupsLock.RLock()
	g, ok := gc.groups[groupID]
	gc.groupsLock.RUnlock()
	if !ok {
		return nil, nil, protocol.ErrorCodeGroupIDNotFound
	}
	offsets, errorCodes := g.offsetFetch(topicNames, partitionIDs, errorCodes)
	return offsets, errorCodes, protocol.ErrorCodeNone
}

func (gc *GroupCoordinator) createGroup(groupID string) *group {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	g, ok := gc.groups[groupID]
	if ok {
		return g
	}
	g = &group{
		gc:                      gc,
		id:                      groupID,
		state:                   stateEmpty,
		members:                 map[string]*member{},
		pendingMemberIDs:        map[string]struct{}{},
		supportedProtocolCounts: map[string]int{},
		committedOffsets:        map[int64]map[int32]int64{},
	}
	gc.groups[groupID] = g
	return g
}

func (gc *GroupCoordinator) setTimer(timerKey string, delay time.Duration, action func()) {
	timer := common.ScheduleTimer(delay, false, action)
	gc.timers.Store(timerKey, timer)
}

func (gc *GroupCoordinator) cancelTimer(timerKey string) {
	t, ok := gc.timers.Load(timerKey)
	if !ok {
		return
	}
	t.(*common.TimerHandle).Stop()
}

func (gc *GroupCoordinator) rescheduleTimer(timerKey string, delay time.Duration, action func()) {
	gc.cancelTimer(timerKey)
	gc.setTimer(timerKey, delay, action)
}

const (
	stateEmpty             = 0
	statePreRebalance      = 1
	stateAwaitingRebalance = 2
	stateActive            = 3
	stateDead              = 4
)

type group struct {
	gc                      *GroupCoordinator
	id                      string
	lock                    sync.Mutex
	state                   int
	members                 map[string]*member
	pendingMemberIDs        map[string]struct{}
	leader                  string
	protocolType            string
	protocolName            string
	supportedProtocolCounts map[string]int
	generationID            int
	memberSeq               int
	assignments             []AssignmentInfo
	initialJoinDelayExpired bool
	stopped                 bool
	newMemberAdded          bool
	committedOffsets        map[int64]map[int32]int64
}

type member struct {
	protocols        []ProtocolInfo
	joinCompletion   JoinCompletion
	syncCompletion   SyncCompletion
	new              bool
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
}

type MemberInfo struct {
	MemberID string
	MetaData []byte
}

type ProtocolInfo struct {
	Name     string
	Metadata []byte
}

func (g *group) stop() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.stopped = true
	g.gc.cancelTimer(g.id)
	for memberID := range g.members {
		g.gc.cancelTimer(memberID)
	}
}

type JoinCompletion func(result JoinResult)

type JoinResult struct {
	ErrorCode      int
	MemberID       string
	LeaderMemberID string
	ProtocolName   string
	GenerationID   int
	Members        []MemberInfo
}

type SyncCompletion func(errorCode int, assignment []byte)

type HeartbeatCompletion func(errorCode int)

type AssignmentInfo struct {
	MemberID   string
	Assignment []byte
}

func (g *group) updateSupportedProtocols(protocols []ProtocolInfo, add bool) {
	for _, protocol := range protocols {
		if add {
			g.supportedProtocolCounts[protocol.Name]++
		} else {
			g.supportedProtocolCounts[protocol.Name]--
		}
	}
}

// canSupportProtocols returns true if at least one of the candidate protocols is already supported by all members
func (g *group) canSupportProtocols(protocols []ProtocolInfo) bool {
	for _, protocol := range protocols {
		count, ok := g.supportedProtocolCounts[protocol.Name]
		if ok && count == len(g.members) {
			return true
		}
	}
	return false
}

func (g *group) chooseProtocol() string {
	// We "vote" on which protocol to use, by looking at each member and voting for the left most protocol which is
	// supported by all members
	allSupportedProtocols := map[string]struct{}{}
	for protocolName, count := range g.supportedProtocolCounts {
		if count == len(g.members) {
			allSupportedProtocols[protocolName] = struct{}{}
		}
	}
	votes := map[string]int{}
	for _, member := range g.members {
		for _, protocol := range member.protocols {
			_, ok := allSupportedProtocols[protocol.Name]
			if ok {
				votes[protocol.Name]++
				break
			}
		}
	}
	maxVotes := 0
	winner := ""
	for protocolName, count := range votes {
		if count > maxVotes {
			maxVotes = count
			winner = protocolName
		}
	}
	return winner
}

func (g *group) Join(apiVersion int16, clientID string, memberID string, protocolType string, protocols []ProtocolInfo,
	sessionTimeout time.Duration, rebalanceTimeout time.Duration, complFunc JoinCompletion) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.state != stateEmpty && !g.canSupportProtocols(protocols) {
		complFunc(JoinResult{ErrorCode: protocol.ErrorCodeInconsistentGroupProtocol, MemberID: ""})
		return
	}
	if memberID == "" {
		memberID = generateMemberID(clientID)
		g.gc.setTimer(memberID, sessionTimeout, func() {
			g.sessionTimeoutExpired(memberID)
		})
		if apiVersion >= 4 {
			// As of KIP-394 Kafka broker doesn't let members join until they call in with a non-empty member id
			// We send back and error and the member will call back in with the member-id
			g.pendingMemberIDs[memberID] = struct{}{}
			complFunc(JoinResult{ErrorCode: protocol.ErrorCodeUnknownMemberID, MemberID: memberID})
			return
		}
	}
	switch g.state {
	case stateEmpty:
		// The first to join is the leader
		g.leader = memberID
		g.protocolType = protocolType
		g.addMember(memberID, protocols, sessionTimeout, rebalanceTimeout, complFunc)
		g.newMemberAdded = false
		g.state = statePreRebalance
		// The first time the join stage is attempted we don't try to complete the join until after a delay - this
		// handles the case when a system starts and many clients join around the same time - we want to avoid
		// re-balancing too much.
		g.scheduleInitialJoinDelay(g.getRebalanceTimeout())
	case statePreRebalance:
		_, ok := g.members[memberID]
		if ok {
			// member already exists
			g.updateMember(memberID, protocols, complFunc)
		} else {
			// adding new member
			g.addMember(memberID, protocols, sessionTimeout, rebalanceTimeout, complFunc)
		}
		if g.initialJoinDelayExpired {
			// If we have gone through join before we can potentially complete the join now, otherwise a timer
			// will have already been set and join will complete when it fires
			g.maybeCompleteJoin()
		}
	case stateAwaitingRebalance:
		member, ok := g.members[memberID]
		if !ok {
			// Join new member
			// For any members waiting sync we complete response with rebalance-in-progress and empty assignments
			// Members will then re-join
			g.resetSync()
			g.addMember(memberID, protocols, sessionTimeout, rebalanceTimeout, complFunc)
		} else {
			// existing member
			if !protocolInfosEqual(member.protocols, protocols) {
				// protocols have changed - reset the sync, and trigger rebalance
				g.resetSync()
				g.updateMember(memberID, protocols, complFunc)
			} else {
				// just return current state
				g.sendJoinResult(memberID, complFunc)
			}
		}
	case stateActive:
		_, ok := g.members[memberID]
		if !ok {
			g.addMember(memberID, protocols, sessionTimeout, rebalanceTimeout, complFunc)
			g.triggerRebalance()
		} else {
			// existing member
			if g.leader == memberID {
				// leader is rejoining
				// trigger rebalance
				g.updateMember(memberID, protocols, complFunc)
				g.triggerRebalance()
			} else {
				// not leader
				g.sendJoinResult(memberID, complFunc)
			}
		}
	case stateDead:
		complFunc(JoinResult{ErrorCode: protocol.ErrorCodeCoordinatorNotAvailable, MemberID: memberID})
		return
	}
}

func (g *group) scheduleInitialJoinDelay(remaining time.Duration) {
	g.gc.setTimer(g.id, g.gc.cfg.KafkaInitialJoinDelay, func() {
		g.initialJoinTimerExpired(remaining)
	})
}

func (g *group) initialJoinTimerExpired(remaining time.Duration) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.stopped {
		return
	}
	if g.newMemberAdded {
		// A new member has been added
		g.newMemberAdded = false // we only count additional members added during join - not the first onw
		// Extend the timer in another chunk of initial-join-delay up to a max total delay of rebalance-timeout
		remaining -= g.gc.cfg.KafkaInitialJoinDelay
		if remaining > 0 {
			g.scheduleInitialJoinDelay(remaining)
			return
		}
	}
	g.initialJoinDelayExpired = true
	g.maybeCompleteJoin()
}

func (g *group) getRebalanceTimeout() time.Duration {
	var maxTimeout time.Duration
	for _, member := range g.members {
		if member.rebalanceTimeout > maxTimeout {
			maxTimeout = member.rebalanceTimeout
		}
	}
	return maxTimeout
}

func (g *group) resetSync() {
	g.assignments = nil
	for _, member := range g.members {
		if member.syncCompletion != nil {
			member.syncCompletion(protocol.ErrorCodeRebalanceInProgress, nil)
		}
	}
	g.triggerRebalance()
}

func protocolInfosEqual(infos1 []ProtocolInfo, infos2 []ProtocolInfo) bool {
	if len(infos1) != len(infos2) {
		return false
	}
	for i, info1 := range infos1 {
		info2 := infos2[i]
		if info1.Name != info2.Name {
			return false
		}
		if !bytes.Equal(info1.Metadata, info2.Metadata) {
			return false
		}
	}
	return true
}

func (g *group) triggerRebalance() {
	if len(g.members) == 0 {
		panic("no members in group")
	}
	g.state = statePreRebalance
	g.gc.rescheduleTimer(g.id, g.getRebalanceTimeout(), func() {
		g.handleJoinTimeout()
	})
}

func (g *group) handleJoinTimeout() {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.stopped || g.state != statePreRebalance {
		return
	}
	// We waited long enough for the join to complete.
	// Not all members have rejoined (if they had, then join would have completed).
	// So, remove any that have not rejoined. If we removed the leader then choose a new leader.
	// If none left, then transition to Empty
	// If there are any left, then, complete the join
	removedLeader := false
	for memberID, member := range g.members {
		if member.joinCompletion == nil {
			g.removeMember(memberID)
			if g.leader == memberID {
				removedLeader = true
			}
		}
	}
	if len(g.members) == 0 {
		// None left - transition to empty
		g.state = stateEmpty
		g.generationID++
		return
	}
	if removedLeader {
		// choose new leader
		g.leader = g.chooseNewLeader()
		g.assignments = nil
	}
	// complete the join
	g.generationID++
	g.sendJoinResults()
}

func (g *group) chooseNewLeader() string {
	for memberID := range g.members {
		return memberID
	}
	return ""
}

func (g *group) addMember(memberID string, protocols []ProtocolInfo, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration, complFunc JoinCompletion) {
	g.members[memberID] = &member{
		protocols:        protocols,
		joinCompletion:   complFunc,
		sessionTimeout:   sessionTimeout,
		rebalanceTimeout: rebalanceTimeout,
		new:              true,
	}
	g.updateSupportedProtocols(protocols, true)
	delete(g.pendingMemberIDs, memberID)
	g.newMemberAdded = true
	// This seems to model the Kafka broker behaviour: Initially when a member attempts to join with no member id
	// a session timeout timer is started with delay session_timeout. Then, when the member calls back in to join
	// with the actual member id, that timer is cancelled and another timer is set with delay equal to new_member_join_timeout
	// which is hardcoded to 5 mins in the Kafka broker and is greater than session timeout (default 45 seconds). Then, when the
	// join is successful the timer is again cancelled and rescheduled with session_timeout again.
	// The code only allows new members to be timed out during join, so this behaviour has the effect of increasing
	// the timeout for new members in the join phase.
	g.gc.rescheduleTimer(memberID, g.gc.cfg.KafkaNewMemberJoinTimeout, func() {
		g.sessionTimeoutExpired(memberID)
	})
}

func (g *group) updateMember(memberID string, protocols []ProtocolInfo, complFunc JoinCompletion) {
	member := g.members[memberID]
	g.updateSupportedProtocols(member.protocols, false)
	member.protocols = protocols
	g.updateSupportedProtocols(member.protocols, true)
	member.joinCompletion = complFunc
	delete(g.pendingMemberIDs, memberID)
}

func (g *group) removeMember(memberID string) bool {
	member, ok := g.members[memberID]
	if !ok {
		return false
	}
	delete(g.members, memberID)
	delete(g.pendingMemberIDs, memberID)
	g.updateSupportedProtocols(member.protocols, false)
	g.gc.cancelTimer(memberID)
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.assignments = nil
	}
	return true
}

func (g *group) maybeCompleteJoin() bool {
	if len(g.pendingMemberIDs) > 0 {
		// We still have members which need to call back in with their member-id to join
		return false
	}
	joinWaitersCount := 0
	for _, member := range g.members {
		if member.joinCompletion != nil {
			joinWaitersCount++
		}
	}
	if joinWaitersCount != len(g.members) {
		// We're waiting for existing members to rejoin
		return false
	}
	g.generationID++
	g.sendJoinResults()
	return true
}

func (g *group) sendJoinResults() {
	g.protocolName = g.chooseProtocol()
	memberInfos := g.createMemberInfos()
	for memberID, member := range g.members {
		g.sendJoinResultWithMembers(memberID, memberInfos, member.joinCompletion)
		member.joinCompletion = nil
		member.new = false
		g.gc.rescheduleTimer(memberID, member.sessionTimeout, func() {
			g.sessionTimeoutExpired(memberID)
		})
	}
	g.state = stateAwaitingRebalance
	// Now we can set a timer for sync timeout
	genID := g.generationID
	g.gc.rescheduleTimer(g.id, g.getRebalanceTimeout(), func() {
		g.handleSyncTimeout(genID)
	})
}

func (g *group) handleSyncTimeout(genId int) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.stopped || g.state != stateAwaitingRebalance {
		return
	}
	if genId != g.generationID {
		log.Warn("sync timeout for wrong generation")
		return
	}
	if g.state != stateAwaitingRebalance {
		return
	}
	removedLeader := false
	for memberID, member := range g.members {
		if member.syncCompletion == nil {
			if memberID == g.leader {
				removedLeader = true
			}
			g.removeMember(memberID)
		}
	}
	if len(g.members) > 0 {
		if removedLeader {
			g.leader = g.chooseNewLeader()
			g.assignments = nil
		}
		g.triggerRebalance()
	}
}

func (g *group) createMemberInfos() []MemberInfo {
	memberInfos := make([]MemberInfo, 0, len(g.members))
	for memberID, member := range g.members {
		var meta []byte
		for _, protocol := range member.protocols {
			if protocol.Name == g.protocolName {
				meta = protocol.Metadata
				break
			}
		}
		if meta == nil {
			panic("cannot find protocol")
		}
		memberInfos = append(memberInfos, MemberInfo{
			MemberID: memberID,
			MetaData: meta,
		})
	}
	return memberInfos
}

func (g *group) sendJoinResult(memberID string, complFunc JoinCompletion) {
	memberInfos := g.createMemberInfos()
	g.sendJoinResultWithMembers(memberID, memberInfos, complFunc)
}

func (g *group) sendJoinResultWithMembers(memberID string, memberInfos []MemberInfo, complFunc JoinCompletion) {
	jr := JoinResult{
		ErrorCode:      protocol.ErrorCodeNone,
		MemberID:       memberID,
		LeaderMemberID: g.leader,
		ProtocolName:   g.protocolName,
		GenerationID:   g.generationID,
		Members:        memberInfos,
	}
	if memberID != g.leader {
		jr.Members = nil
	}
	complFunc(jr)
}

func (g *group) Sync(memberID string, generationID int, assignments []AssignmentInfo, complFunc SyncCompletion) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if generationID != g.generationID {
		complFunc(protocol.ErrorCodeIllegalGeneration, nil)
		return
	}
	switch g.state {
	case statePreRebalance:
		complFunc(protocol.ErrorCodeRebalanceInProgress, nil)
		return
	case stateAwaitingRebalance:
		m, ok := g.members[memberID]
		if !ok {
			complFunc(protocol.ErrorCodeUnknownMemberID, nil)
			return
		}
		// sanity - validate assignments
		if len(assignments) > 0 {
			assignMentsMap := make(map[string]struct{}, len(assignments))
			for _, assignment := range assignments {
				_, exists := assignMentsMap[assignment.MemberID]
				if exists {
					log.Errorf("memberID %s exists more than once in assignments provided at sync by member %s", assignment.MemberID, memberID)
					complFunc(protocol.ErrorCodeUnknownServerError, nil)
					return
				}
				assignMentsMap[assignment.MemberID] = struct{}{}
			}
		}

		m.syncCompletion = complFunc
		if g.leader == memberID {
			g.assignments = assignments
		}
		syncWaitersCount := 0
		for _, m2 := range g.members {
			if m2.syncCompletion != nil {
				syncWaitersCount++
			}
		}
		if syncWaitersCount == len(g.members) {
			g.completeSync()
		}
	case stateActive:
		// Just return current assignment
		var assignment []byte
		for _, assignmentInfo := range g.assignments {
			if assignmentInfo.MemberID == memberID {
				assignment = assignmentInfo.Assignment
			}
		}
		if assignment == nil {
			complFunc(protocol.ErrorCodeUnknownMemberID, nil)
			return
		}
		complFunc(protocol.ErrorCodeNone, assignment)
		return
	case stateDead:
		log.Error("received SyncGroup for dead group")
		complFunc(protocol.ErrorCodeUnknownServerError, nil)
		return
	default:
		// should never occur
		panic("unexpected state in sync")
	}
}

func (g *group) completeSync() {
	for _, assignment := range g.assignments {
		m, ok := g.members[assignment.MemberID]
		if !ok {
			panic(fmt.Sprintf("cannot find member in assignments %s", assignment.MemberID))
		}
		m.syncCompletion(protocol.ErrorCodeNone, assignment.Assignment)
		m.syncCompletion = nil
		memberID := assignment.MemberID
		g.gc.rescheduleTimer(memberID, m.sessionTimeout, func() {
			g.sessionTimeoutExpired(memberID)
		})
	}
	// cancel sync timeout
	g.gc.cancelTimer(g.id)
	g.state = stateActive
}

func (g *group) Heartbeat(memberID string, generationID int) int {
	g.lock.Lock()
	defer g.lock.Unlock()
	if generationID != g.generationID {
		return protocol.ErrorCodeIllegalGeneration
	}
	switch g.state {
	case stateEmpty:
		return protocol.ErrorCodeUnknownMemberID
	case statePreRebalance:
		// Re-balance is required - this will cause client to rejoin group
		return protocol.ErrorCodeRebalanceInProgress
	case stateAwaitingRebalance, stateActive:
		member, ok := g.members[memberID]
		if !ok {
			return protocol.ErrorCodeUnknownMemberID
		}
		g.gc.rescheduleTimer(memberID, member.sessionTimeout, func() {
			g.sessionTimeoutExpired(memberID)
		})
		return protocol.ErrorCodeNone
	default:
		log.Warn("heartbeat on dead group")
		return protocol.ErrorCodeNone
	}
}

func (g *group) Leave(leaveInfos []MemberLeaveInfo) int16 {
	g.lock.Lock()
	defer g.lock.Unlock()
	changed := false
	removedLeader := false
	for _, leaveInfo := range leaveInfos {
		removed := g.removeMember(leaveInfo.MemberID)
		if removed {
			if leaveInfo.MemberID == g.leader {
				removedLeader = true
			}
			changed = true
		}
	}
	if changed {
		if len(g.members) > 0 {
			if removedLeader {
				g.leader = g.chooseNewLeader()

				// this causes the problem
				g.assignments = nil
			}
			g.triggerRebalance()
		} else {
			g.state = stateEmpty
		}
	}
	return protocol.ErrorCodeNone
}

func (g *group) getState() int {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.state
}

func (g *group) sessionTimeoutExpired(memberID string) {
	g.lock.Lock()
	defer g.lock.Unlock()
	_, ok := g.pendingMemberIDs[memberID]
	if ok {
		g.removeMember(memberID)
		if g.state == statePreRebalance {
			g.maybeCompleteJoin()
		}
		return
	}
	member, ok := g.members[memberID]
	if !ok {
		// Already been removed
		return
	}
	if !member.new {
		// Any non-new members waiting for join or sync don't get expired
		if member.joinCompletion != nil || member.syncCompletion != nil {
			return
		}
	} else {
		// New members get timed out in join - send back unknown-member-id and they will retry
		if member.joinCompletion != nil {
			jr := JoinResult{
				ErrorCode: protocol.ErrorCodeUnknownMemberID,
				MemberID:  "",
			}
			member.joinCompletion(jr)
			member.joinCompletion = nil
		}
	}
	g.removeMember(memberID)
	if g.leader == memberID {
		g.leader = g.chooseNewLeader()
	}
	if len(g.members) == 0 {
		// No members left
		g.state = stateEmpty
		return
	}
	if g.state == statePreRebalance {
		// We've removed a member - maybe we can complete the join now?
		g.maybeCompleteJoin()
	} else if g.state == stateActive || g.state == stateAwaitingRebalance {
		g.triggerRebalance()
	}
}

func (g *group) hasMember(memberID string) bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	_, ok := g.members[memberID]
	return ok
}

func fillAllErrorCodes(errorCode int16, errorCodes [][]int16) [][]int16 {
	for i := 0; i < len(errorCodes); i++ {
		fillErrorCodes(errorCode, i, errorCodes)
	}
	return errorCodes
}

func fillErrorCodes(errorCode int16, index int, errorCodes [][]int16) {
	for j := 0; j < len(errorCodes[index]); j++ {
		errorCodes[index][j] = errorCode
	}
}

func (g *group) offsetCommit(memberID string, generationID int, topicNames []string, partitionIDs [][]int32,
	offsets [][]int64, errorCodes [][]int16) [][]int16 {
	g.lock.Lock()
	defer g.lock.Unlock()
	if generationID != g.generationID {
		return fillAllErrorCodes(protocol.ErrorCodeIllegalGeneration, errorCodes)
	}
	_, ok := g.members[memberID]
	if !ok {
		return fillAllErrorCodes(protocol.ErrorCodeUnknownMemberID, errorCodes)
	}
	consumerOffsetsPartitionID := g.gc.calcConsumerOffsetsPartition(g.id)
	processorID, ok := g.gc.consumerOffsetsPPM[consumerOffsetsPartitionID]
	if !ok {
		panic("no processor for partition")
	}
	processor := g.gc.processorProvider.GetProcessor(processorID)
	if processor == nil {
		return fillAllErrorCodes(protocol.ErrorCodeUnknownTopicOrPartition, errorCodes)
	}
	if !processor.IsLeader() {
		return fillAllErrorCodes(protocol.ErrorCodeNotLeaderOrFollower, errorCodes)
	}
	colBuilders := evbatch.CreateColBuilders(ConsumerOffsetsColumnTypes)
	for i, topicName := range topicNames {
		topicInfo, ok := g.topicInfoForName(topicName)
		if !ok {
			fillErrorCodes(protocol.ErrorCodeUnknownTopicOrPartition, i, errorCodes)
			continue
		}
		topicID := int64(topicInfo.ConsumerInfoProvider.SlabID())
		topicPartIDs := partitionIDs[i]
		topicOffsets := offsets[i]
		for j, partitionID := range topicPartIDs {
			offset := topicOffsets[j]
			colBuilders[0].(*evbatch.StringColBuilder).Append(g.id)
			colBuilders[1].(*evbatch.IntColBuilder).Append(topicID)
			colBuilders[2].(*evbatch.IntColBuilder).Append(int64(partitionID))
			colBuilders[3].(*evbatch.IntColBuilder).Append(offset)
			log.Debugf("group %s topic %d partition %d committing offset %d", g.id, topicID, partitionID, offset)
		}
	}
	batch := evbatch.NewBatchFromBuilders(ConsumerOffsetsSchema, colBuilders...)
	processBatch := proc.NewProcessBatch(processorID, batch, common.KafkaOffsetsReceiverID,
		consumerOffsetsPartitionID, -1)
	ch := make(chan error, 1)
	processor.GetReplicator().ReplicateBatch(processBatch, func(err error) {
		ch <- err
	})
	err := <-ch
	if err != nil {
		var errorCode int16
		if common.IsUnavailableError(err) {
			log.Warnf("failed to replicate offset commit batch %v", err)
			// If we have a temp error in replicating - e.g. sync in progress, we send back ErrorCodeNotLeaderOrFollower
			// this causes the client to retry
			errorCode = protocol.ErrorCodeNotLeaderOrFollower
		} else {
			log.Errorf("failed to replicate offset commit batch %v", err)
			errorCode = protocol.ErrorCodeUnknownServerError
		}
		return fillAllErrorCodes(errorCode, errorCodes)
	}
	for i, topicName := range topicNames {
		topicInfo, ok := g.topicInfoForName(topicName)
		if !ok {
			panic("unknown topic")
		}
		topicID := int64(topicInfo.ConsumerInfoProvider.SlabID())
		po, ok := g.committedOffsets[topicID]
		if !ok {
			po = map[int32]int64{}
			g.committedOffsets[topicID] = po
		}
		topicPartIDs := partitionIDs[i]
		topicOffsets := offsets[i]
		for j, partitionID := range topicPartIDs {
			offset := topicOffsets[j]
			po[partitionID] = offset
		}
	}
	return errorCodes
}

func (g *group) topicInfoForName(topicName string) (*TopicInfo, bool) {
	topicInfo, ok := g.gc.metaProvider.GetTopicInfo(topicName)
	if !ok || !topicInfo.ConsumeEnabled {
		return nil, false
	}
	return &topicInfo, true
}

func generateMemberID(clientID string) string {
	return fmt.Sprintf("%s-%s", clientID, uuid.New().String())
}

func (g *group) offsetFetch(topicNames []string, partitionIDs [][]int32, errorCodes [][]int16) ([][]int64, [][]int16) {
	g.lock.Lock()
	defer g.lock.Unlock()
	offsets := make([][]int64, len(topicNames))
	for i, topicName := range topicNames {
		topicInfo, ok := g.topicInfoForName(topicName)
		if !ok {
			fillErrorCodes(protocol.ErrorCodeUnknownTopicOrPartition, i, errorCodes)
			continue
		}
		topicID := int64(topicInfo.ConsumerInfoProvider.SlabID())
		topicOffsets, ok := g.committedOffsets[topicID]
		if !ok {
			topicOffsets = map[int32]int64{}
			g.committedOffsets[topicID] = topicOffsets
		}
		topicPartitions := partitionIDs[i]
		topicResOffsets := make([]int64, len(topicPartitions))
		offsets[i] = topicResOffsets
		for j, partitionID := range topicPartitions {
			offset, ok := topicOffsets[partitionID]
			if !ok {
				var err error
				offset, ok, err = g.loadOffset(topicInfo, partitionID)
				if err != nil {
					log.Errorf("failed to load offset %v", err)
					fillAllErrorCodes(protocol.ErrorCodeUnknownServerError, errorCodes)
					return nil, errorCodes
				}
				if !ok {
					offset = -1 // -1 represents no committed offset
				}
			}
			topicResOffsets[j] = offset
		}
	}
	return offsets, errorCodes
}

func (g *group) loadOffset(topicInfo *TopicInfo, partitionID int32) (int64, bool, error) {

	consumerOffsetsPartitionID := g.gc.calcConsumerOffsetsPartition(g.id)

	partitionHash := proc.CalcPartitionHash(ConsumerOffsetsMappingID, uint64(consumerOffsetsPartitionID))
	iterStart := encoding.EncodeEntryPrefix(partitionHash, common.KafkaOffsetsSlabID, 128)

	// keyCols := []string{"group_id", "topic_id", "partition_id"}
	iterStart = append(iterStart, 1) // not null
	iterStart = encoding.KeyEncodeString(iterStart, g.id)

	iterStart = append(iterStart, 1) // not null
	topicID := int64(topicInfo.ConsumerInfoProvider.SlabID())
	iterStart = encoding.KeyEncodeInt(iterStart, topicID)

	iterStart = append(iterStart, 1) // not null
	iterStart = encoding.KeyEncodeInt(iterStart, int64(partitionID))

	iterEnd := common.IncrementBytesBigEndian(iterStart)

	processorID, ok := g.gc.consumerOffsetsPPM[consumerOffsetsPartitionID]
	if !ok {
		panic("no processor for partition")
	}
	processor := g.gc.processorProvider.GetProcessor(processorID)
	if processor == nil {
		return 0, false, errors.NewTektiteErrorf(errors.Unavailable, "processor not available")
	}
	iter, err := processor.NewIterator(iterStart, iterEnd, math.MaxUint64, false)
	if err != nil {
		return 0, false, err
	}
	defer iter.Close()
	valid, curr, err := iter.Next()
	if err != nil {
		return 0, false, err
	}
	if !valid {
		return 0, false, nil
	}
	offset, _ := encoding.ReadUint64FromBufferLE(curr.Value, 1)
	log.Debugf("group:%d topic:%d partition:%d loaded committed offset:%d", g.id, topicID, partitionID, offset)
	return int64(offset), true, nil
}
