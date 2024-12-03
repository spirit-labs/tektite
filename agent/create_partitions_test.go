package agent

import (
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreatePartitions(t *testing.T) {

	topicName := "test-topic-1"
	topicID := 1000
	partitionID := 7
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 10,
		},
	}
	cfg := NewConf()
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
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

	// First send a batch successfully
	batch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	req := kafkaprotocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(partitionID),
						Records: [][]byte{
							batch1,
						},
					},
				},
			},
		},
	}

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp := produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	verifyBatchesWritten(t, topicID, partitionID, 0, [][]byte{batch1}, controllerCl,
		agent.Conf().PusherConf.DataBucketName, objStore)

	// Now send with invalid partition
	batch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	req = kafkaprotocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(10),
						Records: [][]byte{
							batch2,
						},
					},
				},
			},
		},
	}

	resp = kafkaprotocol.ProduceResponse{}
	r, err = conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok = r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp = produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	// Now change number of partitions
	createPartitionsReq := kafkaprotocol.CreatePartitionsRequest{
		Topics: []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			{
				Name:  common.StrPtr(topicName),
				Count: 100,
			},
		},
	}
	createPartitionsResp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err = conn.SendRequest(&createPartitionsReq, kafkaprotocol.ApiKeyCreatePartitions, 0, createPartitionsResp)
	createPartitionsResp, ok = r.(*kafkaprotocol.CreatePartitionsResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(createPartitionsResp.Results))
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), createPartitionsResp.Results[0].ErrorCode)
	require.Nil(t, createPartitionsResp.Results[0].ErrorMessage)

	// Now send previous request again should succeed

	resp = kafkaprotocol.ProduceResponse{}
	r, err = conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok = r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)
	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp = produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	verifyBatchesWritten(t, topicID, 10, 0, [][]byte{batch2}, controllerCl,
		agent.Conf().PusherConf.DataBucketName, objStore)
}
