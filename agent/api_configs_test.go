package agent

import (
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDescribeAlterConfigs(t *testing.T) {

	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, nil, cfg)
	defer tearDown(t)

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)
	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	describeConfigsReq := kafkaprotocol.DescribeConfigsRequest{
		Resources: []kafkaprotocol.DescribeConfigsRequestDescribeConfigsResource{
			{
				ResourceType: 1,
				ResourceName: common.StrPtr("cluster"),
				ConfigurationKeys: []*string{
					common.StrPtr("foo.bar"),
				},
			},
			{
				ResourceType: 2,
				ResourceName: common.StrPtr("broker"),
				ConfigurationKeys: []*string{
					common.StrPtr("quux.flam"),
				},
			},
		},
		IncludeSynonyms:      true,
		IncludeDocumentation: true,
	}
	describeConfigsResp := &kafkaprotocol.DescribeConfigsResponse{}
	r, err := conn.SendRequest(&describeConfigsReq, kafkaprotocol.ApiKeyDescribeConfigs, 1, describeConfigsResp)
	require.NoError(t, err)
	describeConfigsResp, ok := r.(*kafkaprotocol.DescribeConfigsResponse)
	require.True(t, ok)

	require.Equal(t, 2, len(describeConfigsResp.Results))
	resourceResp1 := describeConfigsResp.Results[0]
	require.Equal(t, 1, int(resourceResp1.ResourceType))
	require.Equal(t, "cluster", common.SafeDerefStringPtr(resourceResp1.ResourceName))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resourceResp1.ErrorCode))
	require.Equal(t, 0, len(resourceResp1.Configs))

	resourceResp2 := describeConfigsResp.Results[1]
	require.Equal(t, 2, int(resourceResp2.ResourceType))
	require.Equal(t, "broker", common.SafeDerefStringPtr(resourceResp2.ResourceName))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resourceResp2.ErrorCode))
	require.Equal(t, 0, len(resourceResp2.Configs))

	alterConfigsReq := kafkaprotocol.AlterConfigsRequest{
		Resources: []kafkaprotocol.AlterConfigsRequestAlterConfigsResource{
			{
				ResourceType: 1,
				ResourceName: common.StrPtr("cluster"),
				Configs: []kafkaprotocol.AlterConfigsRequestAlterableConfig{
					{
						Name:  common.StrPtr("foo.bar"),
						Value: common.StrPtr("wibble"),
					},
				},
			},
		},
	}
	alterConfigsResp := &kafkaprotocol.AlterConfigsResponse{}
	r, err = conn.SendRequest(&alterConfigsReq, kafkaprotocol.ApiKeyAlterConfigs, 0, alterConfigsResp)
	require.NoError(t, err)
	alterConfigsResp, ok = r.(*kafkaprotocol.AlterConfigsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(alterConfigsResp.Responses))
	resp1 := alterConfigsResp.Responses[0]
	require.Equal(t, kafkaprotocol.ErrorCodeInvalidConfig, int(resp1.ErrorCode))
}
