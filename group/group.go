package group

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"time"
)

type group struct {
	gc                      *Coordinator
	id                      string
	offsetWriterKey         string
	partHash                []byte
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
	committedOffsets        map[int]map[int32]int64
	groupEpoch              int
}

type member struct {
	protocols        []ProtocolInfo
	joinCompletion   JoinCompletion
	syncCompletion   SyncCompletion
	new              bool
	sessionTimeout   time.Duration
	reBalanceTimeout time.Duration
}

func (g *group) Join(apiVersion int16, clientID string, memberID string, protocolType string, protocols []ProtocolInfo,
	sessionTimeout time.Duration, reBalanceTimeout time.Duration, completionFunc JoinCompletion) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.state != stateEmpty && !g.canSupportProtocols(protocols) {
		completionFunc(JoinResult{ErrorCode: kafkaprotocol.ErrorCodeInconsistentGroupProtocol, MemberID: ""})
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
			completionFunc(JoinResult{ErrorCode: kafkaprotocol.ErrorCodeUnknownMemberID, MemberID: memberID})
			return
		}
	}
	switch g.state {
	case stateEmpty:
		// The first to join is the leader
		g.leader = memberID
		g.protocolType = protocolType
		g.addMember(memberID, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
		g.newMemberAdded = false
		g.state = statePreReBalance
		// The first time the join stage is attempted we don't try to complete the join until after a delay - this
		// handles the case when a system starts and many clients join around the same time - we want to avoid
		// re-balancing too much.
		g.scheduleInitialJoinDelay(g.getReBalanceTimeout())
	case statePreReBalance:
		_, ok := g.members[memberID]
		if ok {
			// member already exists
			g.updateMember(memberID, protocols, completionFunc)
		} else {
			// adding new member
			g.addMember(memberID, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
		}
		if g.initialJoinDelayExpired {
			// If we have gone through join before we can potentially complete the join now, otherwise a timer
			// will have already been set and join will complete when it fires
			g.maybeCompleteJoin()
		}
	case stateAwaitingReBalance:
		member, ok := g.members[memberID]
		if !ok {
			// Join new member
			// For any members waiting sync we complete response with reBalance-in-progress and empty assignments
			// Members will then re-join
			g.resetSync()
			g.addMember(memberID, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
		} else {
			// existing member
			if !protocolInfosEqual(member.protocols, protocols) {
				// protocols have changed - reset the sync, and trigger reBalance
				g.resetSync()
				g.updateMember(memberID, protocols, completionFunc)
			} else {
				// just return current state
				g.sendJoinResult(memberID, completionFunc)
			}
		}
	case stateActive:
		_, ok := g.members[memberID]
		if !ok {
			g.addMember(memberID, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
			g.triggerReBalance()
		} else {
			// existing member
			if g.leader == memberID {
				// leader is rejoining
				// trigger reBalance
				g.updateMember(memberID, protocols, completionFunc)
				g.triggerReBalance()
			} else {
				// not leader
				g.sendJoinResult(memberID, completionFunc)
			}
		}
	case stateDead:
		completionFunc(JoinResult{ErrorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable, MemberID: memberID})
		return
	}
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

func (g *group) scheduleInitialJoinDelay(remaining time.Duration) {
	g.gc.setTimer(g.id, g.gc.cfg.InitialJoinDelay, func() {
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
		// Extend the timer in another chunk of initial-join-delay up to a max total delay of reBalance-timeout
		remaining -= g.gc.cfg.InitialJoinDelay
		if remaining > 0 {
			g.scheduleInitialJoinDelay(remaining)
			return
		}
	}
	g.initialJoinDelayExpired = true
	g.maybeCompleteJoin()
}

func (g *group) getReBalanceTimeout() time.Duration {
	var maxTimeout time.Duration
	for _, member := range g.members {
		if member.reBalanceTimeout > maxTimeout {
			maxTimeout = member.reBalanceTimeout
		}
	}
	return maxTimeout
}

func (g *group) resetSync() {
	g.assignments = nil
	for _, member := range g.members {
		if member.syncCompletion != nil {
			member.syncCompletion(kafkaprotocol.ErrorCodeRebalanceInProgress, nil)
		}
	}
	g.triggerReBalance()
}

func (g *group) triggerReBalance() {
	if len(g.members) == 0 {
		panic("no members in group")
	}
	g.state = statePreReBalance
	g.gc.rescheduleTimer(g.id, g.getReBalanceTimeout(), func() {
		g.handleJoinTimeout()
	})
}

func (g *group) handleJoinTimeout() {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.stopped || g.state != statePreReBalance {
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
	reBalanceTimeout time.Duration, completionFunc JoinCompletion) {
	g.members[memberID] = &member{
		protocols:        protocols,
		joinCompletion:   completionFunc,
		sessionTimeout:   sessionTimeout,
		reBalanceTimeout: reBalanceTimeout,
		new:              true,
	}
	g.updateSupportedProtocols(protocols, true)
	delete(g.pendingMemberIDs, memberID)
	g.newMemberAdded = true
	// This seems to model the Kafka broker behaviour: Initially when a member attempts to join with no member id
	// a session timeout timer is started with delay session_timeout. Then, when the member calls back in to join
	// with the actual member id, that timer is cancelled and another timer is set with delay equal to new_member_join_timeout
	// which is hardcoded to 5 minutes in the Kafka broker and is greater than session timeout (default 45 seconds). Then, when the
	// join is successful the timer is again cancelled and rescheduled with session_timeout again.
	// The code only allows new members to be timed out during join, so this behaviour has the effect of increasing
	// the timeout for new members in the join phase.
	g.gc.rescheduleTimer(memberID, g.gc.cfg.NewMemberJoinTimeout, func() {
		g.sessionTimeoutExpired(memberID)
	})
}

func (g *group) updateMember(memberID string, protocols []ProtocolInfo, completionFunc JoinCompletion) {
	member := g.members[memberID]
	g.updateSupportedProtocols(member.protocols, false)
	member.protocols = protocols
	g.updateSupportedProtocols(member.protocols, true)
	member.joinCompletion = completionFunc
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
	g.state = stateAwaitingReBalance
	// Now we can set a timer for sync timeout
	genID := g.generationID
	g.gc.rescheduleTimer(g.id, g.getReBalanceTimeout(), func() {
		g.handleSyncTimeout(genID)
	})
}

func (g *group) handleSyncTimeout(genId int) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.stopped || g.state != stateAwaitingReBalance {
		return
	}
	if genId != g.generationID {
		log.Warn("sync timeout for wrong generation")
		return
	}
	if g.state != stateAwaitingReBalance {
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
		g.triggerReBalance()
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

func (g *group) sendJoinResult(memberID string, completionFunc JoinCompletion) {
	memberInfos := g.createMemberInfos()
	g.sendJoinResultWithMembers(memberID, memberInfos, completionFunc)
}

func (g *group) sendJoinResultWithMembers(memberID string, memberInfos []MemberInfo, completionFunc JoinCompletion) {
	jr := JoinResult{
		ErrorCode:      kafkaprotocol.ErrorCodeNone,
		MemberID:       memberID,
		LeaderMemberID: g.leader,
		ProtocolName:   g.protocolName,
		GenerationID:   g.generationID,
		Members:        memberInfos,
	}
	if memberID != g.leader {
		jr.Members = nil
	}
	completionFunc(jr)
}

func (g *group) Sync(memberID string, generationID int, assignments []AssignmentInfo, completionFunc SyncCompletion) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if generationID != g.generationID {
		completionFunc(kafkaprotocol.ErrorCodeIllegalGeneration, nil)
		return
	}
	switch g.state {
	case statePreReBalance:
		completionFunc(kafkaprotocol.ErrorCodeRebalanceInProgress, nil)
		return
	case stateAwaitingReBalance:
		m, ok := g.members[memberID]
		if !ok {
			completionFunc(kafkaprotocol.ErrorCodeUnknownMemberID, nil)
			return
		}
		// sanity - validate assignments
		if len(assignments) > 0 {
			assignmentsMap := make(map[string]struct{}, len(assignments))
			for _, assignment := range assignments {
				_, exists := assignmentsMap[assignment.MemberID]
				if exists {
					log.Errorf("memberID %s exists more than once in assignments provided at sync by member %s", assignment.MemberID, memberID)
					completionFunc(kafkaprotocol.ErrorCodeUnknownServerError, nil)
					return
				}
				assignmentsMap[assignment.MemberID] = struct{}{}
			}
		}

		m.syncCompletion = completionFunc
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
			completionFunc(kafkaprotocol.ErrorCodeUnknownMemberID, nil)
			return
		}
		completionFunc(kafkaprotocol.ErrorCodeNone, assignment)
		return
	case stateDead:
		log.Error("received SyncGroup for dead group")
		completionFunc(kafkaprotocol.ErrorCodeUnknownServerError, nil)
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
		m.syncCompletion(kafkaprotocol.ErrorCodeNone, assignment.Assignment)
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
		return kafkaprotocol.ErrorCodeIllegalGeneration
	}
	switch g.state {
	case stateEmpty:
		return kafkaprotocol.ErrorCodeUnknownMemberID
	case statePreReBalance:
		// Re-balance is required - this will cause client to rejoin group
		return kafkaprotocol.ErrorCodeRebalanceInProgress
	case stateAwaitingReBalance, stateActive:
		member, ok := g.members[memberID]
		if !ok {
			return kafkaprotocol.ErrorCodeUnknownMemberID
		}
		g.gc.rescheduleTimer(memberID, member.sessionTimeout, func() {
			g.sessionTimeoutExpired(memberID)
		})
		return kafkaprotocol.ErrorCodeNone
	default:
		log.Warn("heartbeat on dead group")
		return kafkaprotocol.ErrorCodeNone
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
			g.triggerReBalance()
		} else {
			g.state = stateEmpty
		}
	}
	return kafkaprotocol.ErrorCodeNone
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
		if g.state == statePreReBalance {
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
				ErrorCode: kafkaprotocol.ErrorCodeUnknownMemberID,
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
	if g.state == statePreReBalance {
		// We've removed a member - maybe we can complete the join now?
		g.maybeCompleteJoin()
	} else if g.state == stateActive || g.state == stateAwaitingReBalance {
		g.triggerReBalance()
	}
}

func (g *group) hasMember(memberID string) bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	_, ok := g.members[memberID]
	return ok
}

func fillAllErrorCodesForOffsetCommit(req *kafkaprotocol.OffsetCommitRequest, errorCode int) *kafkaprotocol.OffsetCommitResponse {
	var resp kafkaprotocol.OffsetCommitResponse
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = req.Topics[i].Name
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicData.Partitions))
		for j, partData := range topicData.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partData.PartitionIndex
		}
	}
	for i, topicData := range resp.Topics {
		for j := range topicData.Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = int16(errorCode)
		}
	}
	return &resp
}

func fillAllErrorCodesForOffsetFetch(resp *kafkaprotocol.OffsetFetchResponse, errorCode int) {
	for i, topicData := range resp.Topics {
		for j := range topicData.Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = int16(errorCode)
		}
	}
}

func (g *group) offsetCommit(transactional bool, req *kafkaprotocol.OffsetCommitRequest, resp *kafkaprotocol.OffsetCommitResponse) int {
	g.lock.Lock()
	defer g.lock.Unlock()
	if int(req.GenerationIdOrMemberEpoch) != g.generationID {
		return kafkaprotocol.ErrorCodeIllegalGeneration
	}
	_, ok := g.members[common.SafeDerefStringPtr(req.MemberId)]
	if !ok {
		return kafkaprotocol.ErrorCodeUnknownMemberID
	}
	// Convert to KV pairs
	var kvs []common.KV
	for i, topicData := range req.Topics {
		foundTopic := false
		info, err := g.gc.topicProvider.GetTopicInfo(*topicData.Name)
		if err != nil {
			log.Errorf("failed to find topic %s", *topicData.Name)
		} else {
			foundTopic = true
		}
		for j, partitionData := range topicData.Partitions {
			if !foundTopic {
				resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				continue
			}
			offset := partitionData.CommittedOffset
			// key is [partition_hash, topic_id, partition_id] value is [offset]
			// TODO for transactional we also need to store the producer id and producer epoch
			// and we need to verify that the producer epoch for a group hasn't gone backwards (?)
			var offsetKeyType byte
			if transactional {
				offsetKeyType = offsetKeyTransactional
			} else {
				offsetKeyType = offsetKeyPublic
			}
			key := createOffsetKey(g.partHash, offsetKeyType, info.ID, int(partitionData.PartitionIndex))
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, uint64(offset))
			kvs = append(kvs, common.KV{
				Key:   key,
				Value: value,
			})
			log.Debugf("group %s topic %d partition %d committing offset %d", *req.GroupId, info.ID,
				partitionData.PartitionIndex, offset)
		}
	}
	commitReq := pusher.DirectWriteRequest{
		WriterKey:   g.offsetWriterKey,
		WriterEpoch: g.groupEpoch,
		KVs:         kvs,
	}
	buff := commitReq.Serialize(createRequestBuffer())
	pusherAddress, ok := pusher.ChooseTablePusherForHash(g.partHash, g.gc.membership.Members)
	if !ok {
		// No available pushers
		log.Warnf("cannot commit offsets as no members in cluster")
		return kafkaprotocol.ErrorCodeCoordinatorNotAvailable
	}
	conn, err := g.gc.getConnection(pusherAddress)
	if err != nil {
		log.Warnf("failed to get table pusher connection %v", err)
		return kafkaprotocol.ErrorCodeCoordinatorNotAvailable
	}
	_, err = conn.SendRPC(transport.HandlerIDTablePusherDirectWrite, buff)
	if err != nil {
		if common.IsUnavailableError(err) {
			log.Warnf("failed to handle offset commit: %v", err)
			return kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else {
			log.Errorf("failed to handle offset commit: %v", err)
			return kafkaprotocol.ErrorCodeUnknownServerError
		}
	}
	return kafkaprotocol.ErrorCodeNone
}

const (
	offsetKeyPublic        = byte(1)
	offsetKeyTransactional = byte(2)
)

func createOffsetKey(partHash []byte, offsetKeyType byte, topicID int, partitionID int) []byte {
	var key []byte
	key = append(key, partHash...)
	key = append(key, offsetKeyType)
	key = binary.BigEndian.AppendUint64(key, uint64(topicID))
	key = binary.BigEndian.AppendUint64(key, uint64(partitionID))
	return key
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}

func (g *group) loadOffset(topicID int, partitionID int) (int64, error) {
	key := createOffsetKey(g.partHash, offsetKeyPublic, topicID, partitionID)
	cl, err := g.gc.clientCache.GetClient()
	if err != nil {
		return 0, err
	}
	keyEnd := common.IncBigEndianBytes(key)
	queryRes, err := cl.QueryTablesInRange(key, keyEnd)
	if err != nil {
		return 0, err
	}
	if len(queryRes) == 0 {
		// no stored offset
		return -1, nil
	}
	// We take the first one as that's most recent
	nonOverlapping := queryRes[0]
	res := nonOverlapping[0]
	tableID := res.ID
	sstTable, err := g.gc.tableGetter(tableID)
	if err != nil {
		return 0, err
	}
	iter, err := sstTable.NewIterator(key, keyEnd)
	if err != nil {
		return 0, err
	}
	ok, kv, err := iter.Next()
	if err != nil {
		return 0, err
	}
	if !ok {
		return -1, nil
	}
	if len(kv.Value) == 0 {
		// tombstone
		return -1, nil
	}
	offset := int64(binary.BigEndian.Uint64(kv.Value))
	return offset, nil
}

func (g *group) offsetFetch(req *kafkaprotocol.OffsetFetchRequest, resp *kafkaprotocol.OffsetFetchResponse) {
	g.lock.Lock()
	defer g.lock.Unlock()
	for i, topicData := range req.Topics {
		foundTopic := false
		topicName := common.SafeDerefStringPtr(topicData.Name)
		topicInfo, err := g.gc.topicProvider.GetTopicInfo(topicName)
		if err != nil {
			log.Errorf("failed to find topic %s", topicName)
		} else {
			foundTopic = true
		}
		for j, partitionID := range topicData.PartitionIndexes {
			if !foundTopic {
				resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				continue
			}
			offset, err := g.loadOffset(topicInfo.ID, int(partitionID))
			if err != nil {
				var errCode int
				if common.IsUnavailableError(err) {
					log.Warnf("failed to load offset %v", err)
					errCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
				} else {
					log.Errorf("failed to load offset %v", err)
					errCode = kafkaprotocol.ErrorCodeUnknownServerError
				}
				resp.Topics[i].Partitions[j].ErrorCode = int16(errCode)
				continue
			}
			resp.Topics[i].Partitions[j].CommittedOffset = offset
		}
	}
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

func generateMemberID(clientID string) string {
	return fmt.Sprintf("%s-%s", clientID, uuid.New().String())
}
