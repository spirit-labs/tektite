package group

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	cfg           Conf
	lock          sync.RWMutex
	started       bool
	kafkaAddress  string
	topicProvider topicInfoProvider
	clientCache   *control.ClientCache
	connCaches    *transport.ConnCaches
	tableGetter   sst.TableGetter
	groups        map[string]*group
	timers        sync.Map
	membership    cluster.MembershipState
}

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error)
}

type Conf struct {
	MinSessionTimeout       time.Duration
	MaxSessionTimeout       time.Duration
	DefaultRebalanceTimeout time.Duration
	DefaultSessionTimeout   time.Duration
	InitialJoinDelay        time.Duration
	NewMemberJoinTimeout    time.Duration
}

func NewConf() Conf {
	return Conf{
		MinSessionTimeout:       DefaultMinSessionTimeout,
		MaxSessionTimeout:       DefaultMaxSessionTimeout,
		DefaultRebalanceTimeout: DeafultDefaultRebalanceTimeout,
		DefaultSessionTimeout:   DefaultDefaultSessionTimeout,
		InitialJoinDelay:        DefaultInitialJoinDelay,
		NewMemberJoinTimeout:    DefaultNewMemberJoinTimeout,
	}
}

func (c *Conf) Validate() error {
	return nil
}

const (
	DefaultMinSessionTimeout       = 6 * time.Second
	DefaultMaxSessionTimeout       = 30 * time.Minute
	DefaultInitialJoinDelay        = 3 * time.Second
	DefaultNewMemberJoinTimeout    = 5 * time.Minute
	DeafultDefaultRebalanceTimeout = 5 * time.Minute
	DefaultDefaultSessionTimeout   = 45 * time.Second
)

func NewCoordinator(cfg Conf, topicProvider topicInfoProvider, controlClientCache *control.ClientCache,
	connCaches *transport.ConnCaches, tableGetter sst.TableGetter) (*Coordinator, error) {
	return &Coordinator{
		cfg:           cfg,
		groups:        map[string]*group{},
		topicProvider: topicProvider,
		clientCache:   controlClientCache,
		tableGetter:   tableGetter,
		connCaches:    connCaches,
	}, nil
}

func (c *Coordinator) SetKafkaAddress(address string) {
	c.kafkaAddress = address
}

func (c *Coordinator) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	c.started = true
	return nil
}

func (c *Coordinator) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	for _, g := range c.groups {
		g.stop()
	}
	c.started = false
	return nil
}

func (c *Coordinator) MembershipChanged(_ int32, memberState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.membership = memberState
	return nil
}

func (c *Coordinator) checkStarted() error {
	if !c.started {
		return errors.New("coordinator is not started")
	}
	return nil
}

func (c *Coordinator) HandleFindCoordinatorRequest(req *kafkaprotocol.FindCoordinatorRequest,
	completionFunc func(resp *kafkaprotocol.FindCoordinatorResponse) error) error {
	log.Debugf("received FindCoordinatorRequest on address: %s for key %s key type: %d", c.kafkaAddress, common.SafeDerefStringPtr(req.Key), req.KeyType)
	var resp kafkaprotocol.FindCoordinatorResponse
	var prefix string
	if req.KeyType == 0 {
		// group coordinator
		prefix = "g."
	} else if req.KeyType == 1 {
		// transaction coordinator
		prefix = "t."
	} else {
		resp.ErrorCode = kafkaprotocol.ErrorCodeInvalidRequest
		resp.ErrorMessage = common.StrPtr(fmt.Sprintf("invalid key type %d", req.KeyType))
	}
	if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
		key := common.SafeDerefStringPtr(req.Key)
		memberID, address, err := c.findCoordinator(prefix + key)
		if err != nil {
			if common.IsUnavailableError(err) {
				log.Warnf("failed to find coordinator: %v", err)
				resp.ErrorCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
				resp.ErrorMessage = common.StrPtr("no coordinator available")
			} else {
				log.Errorf("failed to find coordinator: %v", err)
				resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
			}
		} else {
			host, sPort, err := net.SplitHostPort(address)
			var port int
			if err == nil {
				port, err = strconv.Atoi(sPort)
			}
			if err != nil {
				log.Errorf("failed to parse address: %v", err)
				resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
			} else {
				resp.NodeId = memberID
				resp.Host = &host
				resp.Port = int32(port)
				log.Debugf("coordinator for %s is node %d host:%s port:%d", common.SafeDerefStringPtr(req.Key), memberID, host, port)
			}
		}
	}
	if resp.ErrorCode != kafkaprotocol.ErrorCodeNone {
		// host is not nullable
		resp.Host = common.StrPtr("")
	}
	return completionFunc(&resp)
}

func (c *Coordinator) findCoordinator(key string) (int32, string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return 0, "", err
	}
	cl, err := c.clientCache.GetClient()
	if err != nil {
		return 0, "", err
	}
	memberID, address, _, err := cl.GetCoordinatorInfo(key)
	return memberID, address, err
}

func (c *Coordinator) HandleJoinGroupRequest(clientHost string, hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.JoinGroupRequest,
	completionFunc func(resp *kafkaprotocol.JoinGroupResponse) error) error {
	infos := make([]ProtocolInfo, len(req.Protocols))
	for i, protoInfo := range req.Protocols {
		infos[i] = ProtocolInfo{
			Name:     common.SafeDerefStringPtr(protoInfo.Name),
			Metadata: common.ByteSliceCopy(protoInfo.Metadata),
		}
	}
	rebalanceTimeout := c.cfg.DefaultRebalanceTimeout
	if req.RebalanceTimeoutMs != 0 {
		rebalanceTimeout = time.Duration(req.RebalanceTimeoutMs) * time.Millisecond
	}
	sessionTimeout := c.cfg.DefaultSessionTimeout
	if req.SessionTimeoutMs != 0 {
		sessionTimeout = time.Duration(req.SessionTimeoutMs) * time.Millisecond
	}
	c.joinGroup(hdr.RequestApiVersion, common.SafeDerefStringPtr(req.GroupId),
		common.SafeDerefStringPtr(hdr.ClientId), clientHost, common.SafeDerefStringPtr(req.MemberId), common.SafeDerefStringPtr(req.ProtocolType),
		infos, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
			var resp kafkaprotocol.JoinGroupResponse
			resp.ErrorCode = int16(result.ErrorCode)
			if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
				resp.GenerationId = int32(result.GenerationID)
				resp.Leader = &result.LeaderMemberID
				resp.ProtocolName = &result.ProtocolName
				resp.MemberId = &result.MemberID
				resp.Members = make([]kafkaprotocol.JoinGroupResponseJoinGroupResponseMember, len(result.Members))
				for i, m := range result.Members {
					memberID := m.MemberID
					resp.Members[i].MemberId = &memberID
					resp.Members[i].Metadata = m.MetaData
				}
			} else {
				// These are non-nullable fields and need to be provided even in case of error
				resp.ProtocolName = common.StrPtr("")
				resp.Leader = common.StrPtr("")
				resp.MemberId = common.StrPtr("")
			}
			if err := completionFunc(&resp); err != nil {
				log.Errorf("failed to send join group response %v", err)
			}
		})
	return nil
}

func (c *Coordinator) joinGroup(apiVersion int16, groupID string, clientID string, clientHost string, memberID string,
	protocolType string, protocols []ProtocolInfo, sessionTimeout time.Duration, reBalanceTimeout time.Duration,
	completionFunc JoinCompletion) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		log.Warnf("coordinator is not started: %v", err)
		c.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeUnknownServerError)
		return
	}
	if sessionTimeout < c.cfg.MinSessionTimeout || sessionTimeout > c.cfg.MaxSessionTimeout {
		c.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeInvalidSessionTimeout)
		return
	}
	g, ok := c.getGroup(groupID)
	if !ok {
		cl, err := c.clientCache.GetClient()
		if err != nil {
			log.Warnf("failed to get controller client to get coordinator info: %v", err)
			c.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
			return
		}
		_, address, groupEpoch, err := cl.GetCoordinatorInfo(createCoordinatorKey(groupID))
		if err != nil {
			log.Warnf("failed to get coordinator info: %v", err)
			c.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
			return
		}
		if address != c.kafkaAddress {
			c.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeNotCoordinator)
			return
		}
		g = c.createGroup(groupID, groupEpoch)
	}
	g.Join(apiVersion, clientID, clientHost, memberID, protocolType, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
}

func (c *Coordinator) HandleSyncGroupRequest(req *kafkaprotocol.SyncGroupRequest,
	completionFunc func(resp *kafkaprotocol.SyncGroupResponse) error) error {
	assignments := make([]AssignmentInfo, len(req.Assignments))
	for i, assignment := range req.Assignments {
		mem := *assignment.MemberId
		assignments[i] = AssignmentInfo{
			MemberID:   mem,
			Assignment: assignment.Assignment,
		}
	}
	c.syncGroup(*req.GroupId, *req.MemberId, int(req.GenerationId), assignments, func(errorCode int, assignment []byte) {
		var resp kafkaprotocol.SyncGroupResponse
		resp.ErrorCode = int16(errorCode)
		if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
			resp.Assignment = assignment
		}
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send sync group response %v", err)
		}
	})
	return nil
}

func (c *Coordinator) syncGroup(groupID string, memberID string, generationID int, assignments []AssignmentInfo,
	completionFunc SyncCompletion) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		log.Warnf("coordinator is not started: %v", err)
		c.sendSyncError(completionFunc, kafkaprotocol.ErrorCodeUnknownServerError)
		return
	}
	if memberID == "" {
		c.sendSyncError(completionFunc, kafkaprotocol.ErrorCodeUnknownMemberID)
		return
	}
	g, ok := c.getGroup(groupID)
	if !ok {
		c.sendSyncError(completionFunc, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return
	}
	g.Sync(memberID, generationID, assignments, completionFunc)
}

func (c *Coordinator) HandleHeartbeatRequest(req *kafkaprotocol.HeartbeatRequest,
	completionFunc func(resp *kafkaprotocol.HeartbeatResponse) error) error {
	errCode := c.heartbeatGroup(common.SafeDerefStringPtr(req.GroupId),
		common.SafeDerefStringPtr(req.MemberId), int(req.GenerationId))
	return completionFunc(&kafkaprotocol.HeartbeatResponse{
		ErrorCode: int16(errCode),
	})
}

func (c *Coordinator) heartbeatGroup(groupID string, memberID string, generationID int) int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		log.Warn("coordinator is not started")
		return kafkaprotocol.ErrorCodeUnknownServerError
	}
	if memberID == "" {
		return kafkaprotocol.ErrorCodeUnknownMemberID
	}
	g, ok := c.getGroup(groupID)
	if !ok {
		return kafkaprotocol.ErrorCodeGroupIDNotFound
	}
	return g.Heartbeat(memberID, generationID)
}

func (c *Coordinator) HandleLeaveGroupRequest(req *kafkaprotocol.LeaveGroupRequest,
	completionFunc func(resp *kafkaprotocol.LeaveGroupResponse) error) error {
	leaveInfos := []MemberLeaveInfo{{MemberID: *req.MemberId}}
	errorCode := c.leaveGroup(*req.GroupId, leaveInfos)
	var resp kafkaprotocol.LeaveGroupResponse
	resp.ErrorCode = errorCode
	return completionFunc(&resp)
}

func (c *Coordinator) leaveGroup(groupID string, leaveInfos []MemberLeaveInfo) int16 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		log.Warn("coordinator is not started")
		return kafkaprotocol.ErrorCodeUnknownServerError
	}
	g, ok := c.getGroup(groupID)
	if !ok {
		return kafkaprotocol.ErrorCodeGroupIDNotFound
	}
	return g.Leave(leaveInfos)
}

func (c *Coordinator) OffsetCommit(req *kafkaprotocol.OffsetCommitRequest) (*kafkaprotocol.OffsetCommitResponse, error) {
	return c.offsetCommit(false, req)
}

func (c *Coordinator) offsetCommit(transactional bool, req *kafkaprotocol.OffsetCommitRequest) (*kafkaprotocol.OffsetCommitResponse, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return nil, err
	}
	var resp kafkaprotocol.OffsetCommitResponse
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = req.Topics[i].Name
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicData.Partitions))
		for j, partData := range topicData.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partData.PartitionIndex
		}
	}
	groupID := *req.GroupId
	g, ok := c.getGroup(groupID)
	if !ok {
		return fillAllErrorCodesForOffsetCommit(req, kafkaprotocol.ErrorCodeGroupIDNotFound), nil
	}
	errCode := g.offsetCommit(transactional, req, &resp)
	if errCode != kafkaprotocol.ErrorCodeNone {
		return fillAllErrorCodesForOffsetCommit(req, errCode), nil
	}
	return &resp, nil
}

func (c *Coordinator) OffsetCommitTransactional(req *kafkaprotocol.TxnOffsetCommitRequest) (*kafkaprotocol.TxnOffsetCommitResponse, error) {
	ocr := &kafkaprotocol.OffsetCommitRequest{
		GroupId:                   req.GroupId,
		GenerationIdOrMemberEpoch: req.GenerationId,
		MemberId:                  req.MemberId,
		GroupInstanceId:           req.GroupInstanceId,
		Topics:                    make([]kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic, len(req.Topics)),
	}
	for i, topicData := range req.Topics {
		ocr.Topics[i] = kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic{
			Name:       topicData.Name,
			Partitions: make([]kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition, len(topicData.Partitions)),
		}
		for j, partitionData := range topicData.Partitions {
			ocr.Topics[i].Partitions[j] = kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
				PartitionIndex:  partitionData.PartitionIndex,
				CommittedOffset: partitionData.CommittedOffset,
			}
		}
	}
	resp, err := c.offsetCommit(true, ocr)
	if err != nil {
		return nil, err
	}
	tResp := &kafkaprotocol.TxnOffsetCommitResponse{
		Topics: make([]kafkaprotocol.TxnOffsetCommitResponseTxnOffsetCommitResponseTopic, len(resp.Topics)),
	}
	for i, topicData := range resp.Topics {
		tResp.Topics[i] = kafkaprotocol.TxnOffsetCommitResponseTxnOffsetCommitResponseTopic{
			Name:       topicData.Name,
			Partitions: make([]kafkaprotocol.TxnOffsetCommitResponseTxnOffsetCommitResponsePartition, len(topicData.Partitions)),
		}
		for j, partitionData := range topicData.Partitions {
			tResp.Topics[i].Partitions[j] = kafkaprotocol.TxnOffsetCommitResponseTxnOffsetCommitResponsePartition{
				PartitionIndex: partitionData.PartitionIndex,
			}
		}
	}
	return tResp, nil
}

func (c *Coordinator) OffsetDelete(req *kafkaprotocol.OffsetDeleteRequest) (*kafkaprotocol.OffsetDeleteResponse, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return nil, err
	}
	var resp kafkaprotocol.OffsetDeleteResponse
	resp.Topics = make([]kafkaprotocol.OffsetDeleteResponseOffsetDeleteResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = req.Topics[i].Name
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetDeleteResponseOffsetDeleteResponsePartition, len(topicData.Partitions))
		for j, partData := range topicData.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partData.PartitionIndex
		}
	}
	groupID := *req.GroupId
	g, ok := c.getGroup(groupID)
	if !ok {
		return fillAllErrorCodesForOffsetDelete(req, kafkaprotocol.ErrorCodeGroupIDNotFound), nil
	}
	errCode := g.offsetDelete(req, &resp)
	if errCode != kafkaprotocol.ErrorCodeNone {
		return fillAllErrorCodesForOffsetDelete(req, errCode), nil
	}
	return &resp, nil
}

func (c *Coordinator) CompleteTx(groupID string, pid int64, abort bool) error {
	return nil
}

func (c *Coordinator) OffsetFetch(req *kafkaprotocol.OffsetFetchRequest) (*kafkaprotocol.OffsetFetchResponse, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return nil, err
	}
	var resp kafkaprotocol.OffsetFetchResponse
	resp.Topics = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = req.Topics[i].Name
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponsePartition, len(topicData.PartitionIndexes))
		for j, index := range topicData.PartitionIndexes {
			resp.Topics[i].Partitions[j].PartitionIndex = index
		}
	}
	groupID := common.SafeDerefStringPtr(req.GroupId)
	g, ok := c.getGroup(groupID)
	if !ok {
		fillAllErrorCodesForOffsetFetch(&resp, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return &resp, nil
	}
	g.offsetFetch(req, &resp)
	return &resp, nil
}

func (c *Coordinator) ListGroups(req *kafkaprotocol.ListGroupsRequest) (*kafkaprotocol.ListGroupsResponse, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return nil, err
	}
	var respGroups []kafkaprotocol.ListGroupsResponseListedGroup
	for _, g := range c.groups {
		gsString := groupStateToString(g.state)
		if matchesFilters(req.StatesFilter, gsString) &&
			matchesFilters(req.TypesFilter, "consumer") {
			respGroups = append(respGroups, kafkaprotocol.ListGroupsResponseListedGroup{
				GroupId:      common.StrPtr(g.id),
				ProtocolType: common.StrPtr(g.protocolType),
				GroupState:   common.StrPtr(gsString),
				GroupType:    common.StrPtr("consumer"),
			})
		}
	}
	// sort by group id
	sort.SliceStable(respGroups, func(i, j int) bool {
		return strings.Compare(common.SafeDerefStringPtr(respGroups[i].GroupId),
			common.SafeDerefStringPtr(respGroups[j].GroupId)) < 0
	})
	return &kafkaprotocol.ListGroupsResponse{
		Groups: respGroups,
	}, nil
}

func matchesFilters(filters []*string, s string) bool {
	if len(filters) == 0 {
		return true
	}
	s = strings.ToLower(s)
	for _, filter := range filters {
		f := common.SafeDerefStringPtr(filter)
		f = strings.ToLower(strings.TrimSpace(f))
		if f == s {
			return true
		}
	}
	return false
}

func (c *Coordinator) DescribeGroups(req *kafkaprotocol.DescribeGroupsRequest) (*kafkaprotocol.DescribeGroupsResponse, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return nil, err
	}
	var respGroups []kafkaprotocol.DescribeGroupsResponseDescribedGroup
	for _, pGroupID := range req.Groups {
		groupID := common.SafeDerefStringPtr(pGroupID)
		g, ok := c.groups[groupID]
		if !ok {
			respGroups = append(respGroups, kafkaprotocol.DescribeGroupsResponseDescribedGroup{
				GroupId:   common.StrPtr(groupID),
				ErrorCode: kafkaprotocol.ErrorCodeInvalidGroupID,
			})
		} else {
			groupState := groupStateToString(g.state)
			respGroup := kafkaprotocol.DescribeGroupsResponseDescribedGroup{
				GroupId:      common.StrPtr(g.id),
				ProtocolType: common.StrPtr(g.protocolType),
				GroupState:   common.StrPtr(groupState),
			}
			if g.state == StateActive {
				respGroup.ProtocolData = common.StrPtr(g.protocolName)
			}
			if g.state != StateDead {
				assignmentMap := make(map[string]AssignmentInfo, len(g.members))
				for _, info := range g.assignments {
					assignmentMap[info.MemberID] = info
				}
				memberInfos := g.createMemberInfos()
				memberInfosMap := make(map[string]MemberInfo, len(memberInfos))
				for _, memberInfo := range memberInfos {
					memberInfosMap[memberInfo.MemberID] = memberInfo
				}
				for memberID, m := range g.members {
					respMember := kafkaprotocol.DescribeGroupsResponseDescribedGroupMember{
						MemberId:   common.StrPtr(memberID),
						ClientId:   common.StrPtr(m.clientID),
						ClientHost: common.StrPtr(m.clientHost),
					}
					if g.state == StateActive {
						assignInfo, ok := assignmentMap[memberID]
						if !ok {
							panic("cannot find assignment for group")
						}
						respMember.MemberAssignment = assignInfo.Assignment
						memberInfo, ok := memberInfosMap[memberID]
						if !ok {
							panic("cannot find member info for group")
						}
						respMember.MemberMetadata = memberInfo.MetaData
					}
					respGroup.Members = append(respGroup.Members, respMember)
				}
			}
			respGroups = append(respGroups, respGroup)
		}
	}
	return &kafkaprotocol.DescribeGroupsResponse{
		Groups: respGroups,
	}, nil
}

func (c *Coordinator) getGroup(groupID string) (*group, bool) {
	g, ok := c.groups[groupID]
	return g, ok
}

func (c *Coordinator) sendJoinError(completionFunc JoinCompletion, errorCode int) {
	completionFunc(JoinResult{ErrorCode: errorCode})
}

func (c *Coordinator) sendSyncError(completionFunc SyncCompletion, errorCode int) {
	completionFunc(errorCode, nil)
}

func (c *Coordinator) getState(groupID string) GroupState {
	g, ok := c.groups[groupID]
	if !ok {
		return -1
	}
	return g.getState()
}

func (c *Coordinator) groupHasMember(groupID string, memberID string) bool {
	g, ok := c.getGroup(groupID)
	if !ok {
		return false
	}
	return g.hasMember(memberID)
}

func (c *Coordinator) createGroup(groupID string, groupEpoch int) *group {
	c.lock.RUnlock()
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		c.lock.RLock()
	}()
	g, ok := c.groups[groupID]
	if ok {
		return g
	}
	offsetWriterKey := createCoordinatorKey(groupID)
	partHash, err := parthash.CreateHash([]byte(offsetWriterKey))
	if err != nil {
		panic(err) // doesn't happen
	}
	g = &group{
		gc:                      c,
		id:                      groupID,
		groupEpoch:              groupEpoch,
		partHash:                partHash,
		offsetWriterKey:         offsetWriterKey,
		state:                   StateEmpty,
		members:                 map[string]*member{},
		pendingMemberIDs:        map[string]struct{}{},
		supportedProtocolCounts: map[string]int{},
		committedOffsets:        map[int]map[int32]int64{},
	}
	c.groups[groupID] = g
	return g
}

func (c *Coordinator) setTimer(timerKey string, delay time.Duration, action func()) {
	timer := common.ScheduleTimer(delay, false, action)
	c.timers.Store(timerKey, timer)
}

func (c *Coordinator) cancelTimer(timerKey string) {
	t, ok := c.timers.Load(timerKey)
	if !ok {
		return
	}
	t.(*common.TimerHandle).Stop()
}

func (c *Coordinator) rescheduleTimer(timerKey string, delay time.Duration, action func()) {
	c.cancelTimer(timerKey)
	c.setTimer(timerKey, delay, action)
}

func createCoordinatorKey(groupID string) string {
	// prefix with 'g.' to disambiguate with transaction coordinator keys
	return "g." + groupID
}

type GroupState int

const (
	StateEmpty             = GroupState(0)
	StatePreReBalance      = GroupState(1)
	StateAwaitingReBalance = GroupState(2)
	StateActive            = GroupState(3)
	StateDead              = GroupState(4)
)

func groupStateToString(gs GroupState) string {
	switch gs {
	case StateEmpty:
		return "empty"
	case StatePreReBalance:
		return "assigning"
	case StateAwaitingReBalance:
		return "reconciling"
	case StateActive:
		return "stable"
	case StateDead:
		return "dead"
	default:
		panic("unknown group state")
	}
}

type MemberInfo struct {
	MemberID string
	MetaData []byte
}

type ProtocolInfo struct {
	Name     string
	Metadata []byte
}

type MemberLeaveInfo struct {
	MemberID        string
	GroupInstanceID *string
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
