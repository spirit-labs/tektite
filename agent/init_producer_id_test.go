package agent

import (
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInitProducerID(t *testing.T) {
	t.Parallel()
	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)
	conn, err := cl.NewConnection(agents[0].Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	numRequests := 100
	for i := 0; i < numRequests; i++ {
		req := &kafkaprotocol.InitProducerIdRequest{}
		resp := &kafkaprotocol.InitProducerIdResponse{}
		r, err := conn.SendRequest(req, kafkaprotocol.APIKeyInitProducerId, 0, resp)
		require.NoError(t, err)
		resp = r.(*kafkaprotocol.InitProducerIdResponse)
		require.Equal(t, i, int(resp.ProducerId))
	}

}
