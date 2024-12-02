package agent

import (
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestApiVersionsRequestV0(t *testing.T) {
	req := &kafkaprotocol.ApiVersionsRequest{}
	testApiVersionsRequest(t, 0, req)
}

func TestApiVersionsRequestV3(t *testing.T) {
	req := &kafkaprotocol.ApiVersionsRequest{
		ClientSoftwareName:    common.StrPtr("foo"),
		ClientSoftwareVersion: common.StrPtr("v1.0"),
	}
	testApiVersionsRequest(t, 3, req)
}

func testApiVersionsRequest(t *testing.T, apiVersion int16, req *kafkaprotocol.ApiVersionsRequest) {

	cfg := NewConf()
	agents, tearDown := setupAgents(t, cfg, 1, func(i int) string {
		return "az1"
	})
	defer tearDown(t)

	cl, err := apiclient.NewKafkaApiClientWithClientID("")
	require.NoError(t, err)
	conn, err := cl.NewConnection(agents[0].Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	resp := &kafkaprotocol.ApiVersionsResponse{}
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyAPIVersions, apiVersion, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.ApiVersionsResponse)

	require.Equal(t, kafkaprotocol.SupportedAPIVersions, resp.ApiKeys)
}
