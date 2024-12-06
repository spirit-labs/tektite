package agent

import (
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"net"
	"strconv"
	"testing"
)

func TestConsumerGroups(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"

	findCoordinatorReq := &kafkaprotocol.FindCoordinatorRequest{
		Key: common.StrPtr(groupID),
	}
	findCoordinatorResp := &kafkaprotocol.FindCoordinatorResponse{}
	r, err := conn.SendRequest(findCoordinatorReq, kafkaprotocol.APIKeyFindCoordinator, 0, findCoordinatorResp)
	require.NoError(t, err)
	findCoordinatorResp = r.(*kafkaprotocol.FindCoordinatorResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(findCoordinatorResp.ErrorCode))
	require.Equal(t, agent.controller.MemberID(), findCoordinatorResp.NodeId)
	address := agent.cfg.KafkaListenerConfig.Address
	host, sport, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.Equal(t, host, *findCoordinatorResp.Host)
	port, err := strconv.Atoi(sport)
	require.NoError(t, err)
	require.Equal(t, port, int(findCoordinatorResp.Port))

	joinReq := &kafkaprotocol.JoinGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr("member-1"),
		ProtocolType: common.StrPtr("protocol-type-1"),
		Protocols: []kafkaprotocol.JoinGroupRequestJoinGroupRequestProtocol{
			{
				Name:     common.StrPtr("protocol-1"),
				Metadata: []byte("metadata-1"),
			},
		},
	}
	joinResp := &kafkaprotocol.JoinGroupResponse{}
	r, err = conn.SendRequest(joinReq, kafkaprotocol.ApiKeyJoinGroup, 0, joinResp)
	require.NoError(t, err)
	joinResp = r.(*kafkaprotocol.JoinGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(joinResp.ErrorCode))

	require.Equal(t, 1, int(joinResp.GenerationId))
	require.Equal(t, "protocol-1", *joinResp.ProtocolName)
	require.Equal(t, "member-1", *joinResp.Leader)
	require.Equal(t, "member-1", *joinResp.MemberId)
	require.Equal(t, 1, len(joinResp.Members))
	require.Equal(t, "member-1", *joinResp.Members[0].MemberId)
	require.Equal(t, "metadata-1", string(joinResp.Members[0].Metadata))

	syncReq := &kafkaprotocol.SyncGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr("member-1"),
		GenerationId: 1,
		ProtocolType: common.StrPtr("protocol-type-1"),
		ProtocolName: common.StrPtr("protocol-1"),
		Assignments: []kafkaprotocol.SyncGroupRequestSyncGroupRequestAssignment{
			{
				MemberId:   common.StrPtr("member-1"),
				Assignment: []byte("assignment-1"),
			},
		},
	}
	syncResp := &kafkaprotocol.SyncGroupResponse{}
	r, err = conn.SendRequest(syncReq, kafkaprotocol.ApiKeySyncGroup, 0, syncResp)
	require.NoError(t, err)
	syncResp = r.(*kafkaprotocol.SyncGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(syncResp.ErrorCode))
	require.Equal(t, "assignment-1", string(syncResp.Assignment))

	partitionID := int32(23)

	committedOffset := int64(123123123)

	offsetCommitReq := &kafkaprotocol.OffsetCommitRequest{
		GroupId:                   common.StrPtr(groupID),
		GenerationIdOrMemberEpoch: 1,
		MemberId:                  common.StrPtr("member-1"),
		Topics: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
					{
						PartitionIndex:  partitionID,
						CommittedOffset: committedOffset,
					},
				},
			},
		},
	}
	offsetCommitResp := &kafkaprotocol.OffsetCommitResponse{}
	r, err = conn.SendRequest(offsetCommitReq, kafkaprotocol.APIKeyOffsetCommit, 2, offsetCommitResp)
	require.NoError(t, err)
	offsetCommitResp = r.(*kafkaprotocol.OffsetCommitResponse)
	require.Equal(t, 1, len(offsetCommitResp.Topics))
	require.Equal(t, 1, len(offsetCommitResp.Topics[0].Partitions))
	partitionResp := offsetCommitResp.Topics[0].Partitions[0]
	require.Equal(t, 23, int(partitionResp.PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partitionResp.ErrorCode))
	offsets := getRowsForGroup(t, groupID, agent)
	require.Equal(t, 1, len(offsets))

	offsetFetchReq := &kafkaprotocol.OffsetFetchRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetFetchRequestOffsetFetchRequestTopic{
			{
				Name:             common.StrPtr(topicName),
				PartitionIndexes: []int32{partitionID},
			},
		},
		Groups:        nil,
		RequireStable: false,
	}

	offsetFetchResp := &kafkaprotocol.OffsetFetchResponse{}
	r, err = conn.SendRequest(offsetFetchReq, kafkaprotocol.APIKeyOffsetFetch, 1, offsetFetchResp)
	require.NoError(t, err)
	offsetFetchResp = r.(*kafkaprotocol.OffsetFetchResponse)

	require.Equal(t, 1, len(offsetFetchResp.Topics))
	require.Equal(t, topicName, *offsetCommitResp.Topics[0].Name)
	require.Equal(t, 1, len(offsetFetchResp.Topics[0].Partitions))
	partOffset := offsetFetchResp.Topics[0].Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partOffset.ErrorCode))
	require.Equal(t, partitionID, partOffset.PartitionIndex)
	require.Equal(t, committedOffset, partOffset.CommittedOffset)

	offsetDeleteReq := &kafkaprotocol.OffsetDeleteRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestPartition{
					{
						PartitionIndex: partitionID,
					},
				},
			},
		},
	}
	offsetDeleteResp := &kafkaprotocol.OffsetDeleteResponse{}
	r, err = conn.SendRequest(offsetDeleteReq, kafkaprotocol.APIKeyOffsetDelete, 0, offsetDeleteResp)
	require.NoError(t, err)
	offsetDeleteResp = r.(*kafkaprotocol.OffsetDeleteResponse)

	require.Equal(t, 1, len(offsetDeleteResp.Topics))
	require.Equal(t, topicName, common.SafeDerefStringPtr(offsetDeleteResp.Topics[0].Name))
	require.Equal(t, 1, len(offsetDeleteResp.Topics[0].Partitions))
	delPartitionResp := offsetDeleteResp.Topics[0].Partitions[0]

	require.Equal(t, 23, int(delPartitionResp.PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(delPartitionResp.ErrorCode))

	heartbeatReq := &kafkaprotocol.HeartbeatRequest{
		GroupId:      common.StrPtr(groupID),
		GenerationId: 1,
		MemberId:     common.StrPtr("member-1"),
	}
	heartbeatResp := &kafkaprotocol.HeartbeatResponse{}
	r, err = conn.SendRequest(heartbeatReq, kafkaprotocol.ApiKeyHeartbeat, 0, heartbeatResp)
	require.NoError(t, err)
	heartbeatResp = r.(*kafkaprotocol.HeartbeatResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(heartbeatResp.ErrorCode))

	describeGroupsReq := &kafkaprotocol.DescribeGroupsRequest{
		Groups: []*string{common.StrPtr(groupID)},
	}
	describeGroupResp := &kafkaprotocol.DescribeGroupsResponse{}
	r, err = conn.SendRequest(describeGroupsReq, kafkaprotocol.ApiKeyDescribeGroups, 0, describeGroupResp)
	require.NoError(t, err)
	describeGroupResp = r.(*kafkaprotocol.DescribeGroupsResponse)

	require.Equal(t, 1, len(describeGroupResp.Groups))
	groupResp := describeGroupResp.Groups[0]
	require.Equal(t, groupID, common.SafeDerefStringPtr(groupResp.GroupId))
	require.Equal(t, 1, len(groupResp.Members))
	memberResp := groupResp.Members[0]
	require.Equal(t, "metadata-1", string(memberResp.MemberMetadata))
	require.Equal(t, "assignment-1", string(memberResp.MemberAssignment))
	require.Equal(t, "member-1", common.SafeDerefStringPtr(memberResp.MemberId))
	require.Equal(t, "some-client-id", common.SafeDerefStringPtr(memberResp.ClientId))
	require.Equal(t, "127.0.0.1", common.SafeDerefStringPtr(memberResp.ClientHost))

	leaveGroupReq := &kafkaprotocol.LeaveGroupRequest{
		GroupId:  common.StrPtr(groupID),
		MemberId: common.StrPtr("member-1"),
	}
	leaveGroupResp := &kafkaprotocol.LeaveGroupResponse{}
	r, err = conn.SendRequest(leaveGroupReq, kafkaprotocol.ApiKeyLeaveGroup, 0, leaveGroupResp)
	require.NoError(t, err)
	leaveGroupResp = r.(*kafkaprotocol.LeaveGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(leaveGroupResp.ErrorCode))

	listGroupsReq := &kafkaprotocol.ListGroupsRequest{
		StatesFilter: []*string{common.StrPtr("empty")},
		TypesFilter:  []*string{common.StrPtr("consumer")},
	}
	listGroupResp := &kafkaprotocol.ListGroupsResponse{}
	r, err = conn.SendRequest(listGroupsReq, kafkaprotocol.ApiKeyListGroups, 5, listGroupResp)
	require.NoError(t, err)
	listGroupResp = r.(*kafkaprotocol.ListGroupsResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(listGroupResp.ErrorCode))
	require.Equal(t, 1, len(listGroupResp.Groups))
	listGroupsResp := listGroupResp.Groups[0]
	require.Equal(t, "empty", common.SafeDerefStringPtr(listGroupsResp.GroupState))
	require.Equal(t, "consumer", common.SafeDerefStringPtr(listGroupsResp.GroupType))
	require.Equal(t, "protocol-type-1", common.SafeDerefStringPtr(listGroupsResp.ProtocolType))
	require.Equal(t, groupID, common.SafeDerefStringPtr(listGroupsResp.GroupId))

	// now delete group
	deleteGroupsReq := &kafkaprotocol.DeleteGroupsRequest{
		GroupsNames: []*string{common.StrPtr(groupID), common.StrPtr("unknown")},
	}
	deleteGroupsResp := &kafkaprotocol.DeleteGroupsResponse{}
	r, err = conn.SendRequest(deleteGroupsReq, kafkaprotocol.ApiKeyDeleteGroups, 0, deleteGroupsResp)
	require.NoError(t, err)
	deleteGroupsResp = r.(*kafkaprotocol.DeleteGroupsResponse)
	require.Equal(t, 2, len(deleteGroupsResp.Results))
	res1 := deleteGroupsResp.Results[0]
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(res1.ErrorCode))
	require.Equal(t, groupID, common.SafeDerefStringPtr(res1.GroupId))
	res2 := deleteGroupsResp.Results[1]
	require.Equal(t, kafkaprotocol.ErrorCodeInvalidGroupID, int(res2.ErrorCode))
	require.Equal(t, "unknown", common.SafeDerefStringPtr(res2.GroupId))

	listGroupResp = &kafkaprotocol.ListGroupsResponse{}
	r, err = conn.SendRequest(listGroupsReq, kafkaprotocol.ApiKeyListGroups, 5, listGroupResp)
	require.NoError(t, err)
	listGroupResp = r.(*kafkaprotocol.ListGroupsResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(listGroupResp.ErrorCode))
	require.Equal(t, 0, len(listGroupResp.Groups)) // should be deleted

	// Now check no offset in database
	offsets = getRowsForGroup(t, groupID, agent)
	for _, kv := range offsets {
		log.Infof("key: %v value: %v", kv.Key, kv.Value)
	}
	require.Equal(t, 0, len(offsets))
}

func getRowsForGroup(t *testing.T, groupID string, agent *Agent) []common.KV {
	partHash, err := parthash.CreateHash([]byte("g." + groupID))
	require.NoError(t, err)
	controlClient, err := agent.controlClientCache.GetClient()
	require.NoError(t, err)
	iter, err := queryutils.CreateIteratorForKeyRange(partHash, common.IncBigEndianBytes(partHash), controlClient,
		agent.TableGetter())
	require.NoError(t, err)
	defer iter.Close()
	var kvs []common.KV
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func TestFindCoordinatorError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	// Force no members to get error
	agent.controller.GetGroupCoordinatorController().ClearMembers()

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"
	findCoordinatorReq := &kafkaprotocol.FindCoordinatorRequest{
		Key: common.StrPtr(groupID),
	}
	findCoordinatorResp := &kafkaprotocol.FindCoordinatorResponse{}
	r, err := conn.SendRequest(findCoordinatorReq, kafkaprotocol.APIKeyFindCoordinator, 0, findCoordinatorResp)
	require.NoError(t, err)
	findCoordinatorResp = r.(*kafkaprotocol.FindCoordinatorResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeCoordinatorNotAvailable, int(findCoordinatorResp.ErrorCode))
}

func TestJoinGroupError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	// Force no members to get error
	agent.controller.GetGroupCoordinatorController().ClearMembers()

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"
	joinReq := &kafkaprotocol.JoinGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr("member-1"),
		ProtocolType: common.StrPtr("protocol-type-1"),
		Protocols: []kafkaprotocol.JoinGroupRequestJoinGroupRequestProtocol{
			{
				Name:     common.StrPtr("protocol-1"),
				Metadata: []byte("metadata-1"),
			},
		},
	}
	joinResp := &kafkaprotocol.JoinGroupResponse{}
	r, err := conn.SendRequest(joinReq, kafkaprotocol.ApiKeyJoinGroup, 0, joinResp)
	require.NoError(t, err)
	joinResp = r.(*kafkaprotocol.JoinGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeCoordinatorNotAvailable, int(joinResp.ErrorCode))
}

func TestSyncGroupError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	// Force no members to get error
	agent.controller.GetGroupCoordinatorController().ClearMembers()

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"

	syncReq := &kafkaprotocol.SyncGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr("member-1"),
		GenerationId: 1,
		ProtocolType: common.StrPtr("protocol-type-1"),
		ProtocolName: common.StrPtr("protocol-1"),
		Assignments: []kafkaprotocol.SyncGroupRequestSyncGroupRequestAssignment{
			{
				MemberId:   common.StrPtr("member-1"),
				Assignment: []byte("assignment-1"),
			},
		},
	}
	syncResp := &kafkaprotocol.SyncGroupResponse{}
	r, err := conn.SendRequest(syncReq, kafkaprotocol.ApiKeySyncGroup, 0, syncResp)
	require.NoError(t, err)
	syncResp = r.(*kafkaprotocol.SyncGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, int(syncResp.ErrorCode))
}

func TestOffsetCommitError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"
	partitionID := int32(23)
	committedOffset := int64(123123123)

	offsetCommitReq := &kafkaprotocol.OffsetCommitRequest{
		GroupId:                   common.StrPtr(groupID),
		GenerationIdOrMemberEpoch: 1,
		MemberId:                  common.StrPtr("member-1"),
		Topics: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
					{
						PartitionIndex:  partitionID,
						CommittedOffset: committedOffset,
					},
				},
			},
		},
	}
	offsetCommitResp := &kafkaprotocol.OffsetCommitResponse{}
	r, err := conn.SendRequest(offsetCommitReq, kafkaprotocol.APIKeyOffsetCommit, 2, offsetCommitResp)
	require.NoError(t, err)
	offsetCommitResp = r.(*kafkaprotocol.OffsetCommitResponse)

	require.Equal(t, 1, len(offsetCommitResp.Topics))
	require.Equal(t, 1, len(offsetCommitResp.Topics[0].Partitions))
	partitionResp := offsetCommitResp.Topics[0].Partitions[0]

	require.Equal(t, 23, int(partitionResp.PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, int(partitionResp.ErrorCode))
}

func TestOffsetDeleteError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"
	partitionID := int32(23)

	offsetDeleteReq := &kafkaprotocol.OffsetDeleteRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestPartition{
					{
						PartitionIndex: partitionID,
					},
				},
			},
		},
	}
	offsetDeleteResp := &kafkaprotocol.OffsetDeleteResponse{}
	r, err := conn.SendRequest(offsetDeleteReq, kafkaprotocol.APIKeyOffsetDelete, 0, offsetDeleteResp)
	require.NoError(t, err)
	offsetDeleteResp = r.(*kafkaprotocol.OffsetDeleteResponse)

	require.Equal(t, 1, len(offsetDeleteResp.Topics))
	require.Equal(t, 1, len(offsetDeleteResp.Topics[0].Partitions))
	partitionResp := offsetDeleteResp.Topics[0].Partitions[0]

	require.Equal(t, 23, int(partitionResp.PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, int(partitionResp.ErrorCode))
}

func TestOffsetFetchError(t *testing.T) {
	topicName := "test-topic-1"
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	groupID := "test-group"
	partitionID := int32(23)

	offsetFetchReq := &kafkaprotocol.OffsetFetchRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetFetchRequestOffsetFetchRequestTopic{
			{
				Name:             common.StrPtr(topicName),
				PartitionIndexes: []int32{partitionID},
			},
		},
		Groups:        nil,
		RequireStable: false,
	}

	offsetFetchResp := &kafkaprotocol.OffsetFetchResponse{}
	r, err := conn.SendRequest(offsetFetchReq, kafkaprotocol.APIKeyOffsetFetch, 1, offsetFetchResp)
	require.NoError(t, err)
	offsetFetchResp = r.(*kafkaprotocol.OffsetFetchResponse)

	require.Equal(t, 1, len(offsetFetchResp.Topics))
	require.Equal(t, 1, len(offsetFetchResp.Topics[0].Partitions))
	partOffset := offsetFetchResp.Topics[0].Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, int(partOffset.ErrorCode))
}
