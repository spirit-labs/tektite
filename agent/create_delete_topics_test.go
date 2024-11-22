package agent

import (
	"testing"

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
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyCreateTopics, -1, &resp)
	require.NoError(t, err)
	produceResp, ok := r.(*kafkaprotocol.CreateTopicsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(produceResp.Topics))
}
