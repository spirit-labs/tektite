package agent

import (
	"testing"
	"time"

	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
)

func setupAgentWithoutTopics(t *testing.T, cfg Conf) (*Agent, *dev.InMemStore, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports := transport.NewLocalTransports()
	agent, tearDown := setupAgentWithArgs(t, cfg, objStore, inMemMemberships, localTransports)
	return agent, objStore, tearDown
}

func TestCreateDeleteTopics(t *testing.T) {
	cfg := NewConf()
	agent, _, tearDown := setupAgentWithoutTopics(t, cfg)
	defer tearDown(t)

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	topicName := "test-topic-1"
	configName := "retention.ms"
	configValue := "86400000" // 1 day

	//Create
	req := kafkaprotocol.CreateTopicsRequest{
		Topics: []kafkaprotocol.CreateTopicsRequestCreatableTopic{
			{
				Name:              &topicName,
				NumPartitions:     23,
				ReplicationFactor: 3,
				Configs: []kafkaprotocol.CreateTopicsRequestCreatableTopicConfig{
					{
						Name:  &configName,
						Value: &configValue,
					},
				},
			},
		},
	}

	var resp kafkaprotocol.CreateTopicsResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyCreateTopics, 5, &resp)
	require.NoError(t, err)

	createResp, ok := r.(*kafkaprotocol.CreateTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(createResp.Topics))
	require.Equal(t, topicName, common.SafeDerefStringPtr(createResp.Topics[0].Name))
	require.Equal(t, int16(0), createResp.Topics[0].ErrorCode)
	require.Equal(t, int32(23), createResp.Topics[0].NumPartitions)
	require.Equal(t, int16(3), createResp.Topics[0].ReplicationFactor)
	require.Equal(t, 1, len(createResp.Topics[0].Configs))
	require.Equal(t, configName, common.SafeDerefStringPtr(createResp.Topics[0].Configs[0].Name))
	require.Equal(t, configValue, common.SafeDerefStringPtr(createResp.Topics[0].Configs[0].Value))

	info, topicExists, err := agent.topicMetaCache.GetTopicInfo(topicName)
	require.True(t, topicExists)
	require.NoError(t, err)
	require.Equal(t, topicName, info.Name)
	require.Equal(t, 23, info.PartitionCount)
	require.Equal(t, 24*time.Hour, info.RetentionTime)

	//Delete
	req2 := kafkaprotocol.DeleteTopicsRequest{
		TopicNames: []*string{
			&topicName,
		},
	}
	var resp2 kafkaprotocol.DeleteTopicsResponse
	r2, err := conn.SendRequest(&req2, kafkaprotocol.APIKeyDeleteTopics, 5, &resp2)
	require.NoError(t, err)
	deleteResp, ok := r2.(*kafkaprotocol.DeleteTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(deleteResp.Responses))
	require.Equal(t, topicName, *deleteResp.Responses[0].Name)
	require.Equal(t, int16(0), deleteResp.Responses[0].ErrorCode)
	_, topicExists2, _ := agent.topicMetaCache.GetTopicInfo(topicName)
	require.False(t, topicExists2)
}

func TestCreateDuplicateTopic(t *testing.T) {
	cfg := NewConf()
	agent, _, tearDown := setupAgentWithoutTopics(t, cfg)
	defer tearDown(t)

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	topicName := "test-topic-1"

	//Create
	req := kafkaprotocol.CreateTopicsRequest{
		Topics: []kafkaprotocol.CreateTopicsRequestCreatableTopic{
			{
				Name:              &topicName,
				NumPartitions:     23,
				ReplicationFactor: 3,
			},
		},
	}

	var resp kafkaprotocol.CreateTopicsResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyCreateTopics, 5, &resp)
	require.NoError(t, err)

	createResp, ok := r.(*kafkaprotocol.CreateTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(createResp.Topics))
	require.Equal(t, topicName, common.SafeDerefStringPtr(createResp.Topics[0].Name))
	require.Equal(t, int16(0), createResp.Topics[0].ErrorCode)
	require.Equal(t, int32(23), createResp.Topics[0].NumPartitions)
	require.Equal(t, int16(3), createResp.Topics[0].ReplicationFactor)

	info, topicExists, err := agent.topicMetaCache.GetTopicInfo(topicName)
	require.True(t, topicExists)
	require.NoError(t, err)
	require.Equal(t, topicName, info.Name)
	require.Equal(t, 23, info.PartitionCount)
	require.Equal(t, 7*24*time.Hour, info.RetentionTime)

	//Duplicate create
	req = kafkaprotocol.CreateTopicsRequest{
		Topics: []kafkaprotocol.CreateTopicsRequestCreatableTopic{
			{
				Name:              &topicName,
				NumPartitions:     23,
				ReplicationFactor: 3,
			},
		},
	}

	var resp2 kafkaprotocol.CreateTopicsResponse
	r2, err := conn.SendRequest(&req, kafkaprotocol.APIKeyCreateTopics, 5, &resp2)
	require.NoError(t, err)

	createResp2, ok := r2.(*kafkaprotocol.CreateTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(createResp2.Topics))
	require.Equal(t, topicName, common.SafeDerefStringPtr(createResp2.Topics[0].Name))
	require.Equal(t, int16(kafkaprotocol.ErrorCodeTopicAlreadyExists), createResp2.Topics[0].ErrorCode)
	require.Equal(t, int32(23), createResp2.Topics[0].NumPartitions)
	require.Equal(t, int16(3), createResp2.Topics[0].ReplicationFactor)
}

func TestDeleteNonExistentTopic(t *testing.T) {
	cfg := NewConf()
	agent, _, tearDown := setupAgentWithoutTopics(t, cfg)
	defer tearDown(t)

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	topicName := "test-topic-1"

	req := kafkaprotocol.DeleteTopicsRequest{
		TopicNames: []*string{
			&topicName,
		},
	}
	var resp kafkaprotocol.DeleteTopicsResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyDeleteTopics, 5, &resp)
	require.NoError(t, err)
	deleteResp, ok := r.(*kafkaprotocol.DeleteTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(deleteResp.Responses))
	require.Equal(t, topicName, *deleteResp.Responses[0].Name)
	require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), deleteResp.Responses[0].ErrorCode)
	_, topicExists2, _ := agent.topicMetaCache.GetTopicInfo(topicName)
	require.False(t, topicExists2)
}
