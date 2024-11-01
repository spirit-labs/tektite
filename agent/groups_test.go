package agent

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
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

	cl, err := NewKafkaApiClient()
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

	leaveGroupReq := &kafkaprotocol.LeaveGroupRequest{
		GroupId:  common.StrPtr(groupID),
		MemberId: common.StrPtr("member-1"),
	}
	leaveGroupResp := &kafkaprotocol.LeaveGroupResponse{}
	r, err = conn.SendRequest(leaveGroupReq, kafkaprotocol.ApiKeyLeaveGroup, 0, leaveGroupResp)
	require.NoError(t, err)
	leaveGroupResp = r.(*kafkaprotocol.LeaveGroupResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(leaveGroupResp.ErrorCode))
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

	cl, err := NewKafkaApiClient()
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

	cl, err := NewKafkaApiClient()
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

	cl, err := NewKafkaApiClient()
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

	cl, err := NewKafkaApiClient()
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

	cl, err := NewKafkaApiClient()
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
