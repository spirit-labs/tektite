package agent

import (
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreatePartitions(t *testing.T) {

	numTopics := 10
	initialPartitionCount := 10
	finalPartitionCount := 100
	var topicInfos []topicmeta.TopicInfo
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%d", i)
		topicInfos = append(topicInfos, topicmeta.TopicInfo{
			Name:           topicName,
			PartitionCount: initialPartitionCount,
			RetentionTime:  -1,
		})
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

	// send with invalid partition
	for _, info := range topicInfos {
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
		req := kafkaprotocol.ProduceRequest{
			Acks:      -1,
			TimeoutMs: 1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: common.StrPtr(info.Name),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index: int32(finalPartitionCount - 1),
							Records: [][]byte{
								batch,
							},
						},
					},
				},
			},
		}
		resp := kafkaprotocol.ProduceResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)
		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), partResp.ErrorCode)
		require.Equal(t, (*string)(nil), partResp.ErrorMessage)
	}

	var topics []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic
	for _, info := range topicInfos {
		// Now change number of partitions
		topics = append(topics, kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			Name:  common.StrPtr(info.Name),
			Count: int32(finalPartitionCount),
		})
	}

	createPartitionsReq := kafkaprotocol.CreatePartitionsRequest{
		Topics: topics,
	}
	createPartitionsResp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err := conn.SendRequest(&createPartitionsReq, kafkaprotocol.ApiKeyCreatePartitions, 0, createPartitionsResp)
	require.NoError(t, err)
	createPartitionsResp, ok := r.(*kafkaprotocol.CreatePartitionsResponse)
	require.True(t, ok)
	require.Equal(t, numTopics, len(createPartitionsResp.Results))
	for _, topicResp := range createPartitionsResp.Results {
		require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), topicResp.ErrorCode)
		require.Nil(t, topicResp.ErrorMessage)
	}

	// Now produce again, should succeed
	for i, info := range topicInfos {
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
		req := kafkaprotocol.ProduceRequest{
			Acks:      -1,
			TimeoutMs: 1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: common.StrPtr(info.Name),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index: int32(finalPartitionCount - 1),
							Records: [][]byte{
								batch,
							},
						},
					},
				},
			},
		}
		resp := kafkaprotocol.ProduceResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)
		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)

		verifyBatchesWritten(t, 1000+i, finalPartitionCount-1, 0, [][]byte{batch}, controllerCl,
			agent.Conf().PusherConf.DataBucketName, objStore)
	}
}

func TestCreatePartitionsValidateOnly(t *testing.T) {

	numTopics := 10
	initialPartitionCount := 10
	finalPartitionCount := 100
	var topicInfos []topicmeta.TopicInfo
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%d", i)
		topicInfos = append(topicInfos, topicmeta.TopicInfo{
			Name:           topicName,
			PartitionCount: initialPartitionCount,
			RetentionTime:  -1,
		})
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
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

	// send with invalid partition
	for _, info := range topicInfos {
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
		req := kafkaprotocol.ProduceRequest{
			Acks:      -1,
			TimeoutMs: 1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: common.StrPtr(info.Name),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index: int32(finalPartitionCount - 1),
							Records: [][]byte{
								batch,
							},
						},
					},
				},
			},
		}
		resp := kafkaprotocol.ProduceResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)
		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), partResp.ErrorCode)
		require.Equal(t, (*string)(nil), partResp.ErrorMessage)
	}

	var topics []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic
	for _, info := range topicInfos {
		// This time with validateOnly = true
		topics = append(topics, kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			Name:  common.StrPtr(info.Name),
			Count: int32(finalPartitionCount),
		})
	}
	createPartitionsReq := kafkaprotocol.CreatePartitionsRequest{
		ValidateOnly: true,
		Topics:       topics,
	}
	createPartitionsResp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err := conn.SendRequest(&createPartitionsReq, kafkaprotocol.ApiKeyCreatePartitions, 0, createPartitionsResp)
	require.NoError(t, err)
	createPartitionsResp, ok := r.(*kafkaprotocol.CreatePartitionsResponse)
	require.True(t, ok)
	require.Equal(t, numTopics, len(createPartitionsResp.Results))
	for _, topicResp := range createPartitionsResp.Results {
		require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), topicResp.ErrorCode)
		require.Nil(t, topicResp.ErrorMessage)
	}

	// Now produce again, should fail still as we only validated
	for _, info := range topicInfos {
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
		req := kafkaprotocol.ProduceRequest{
			Acks:      -1,
			TimeoutMs: 1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: common.StrPtr(info.Name),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index: int32(finalPartitionCount - 1),
							Records: [][]byte{
								batch,
							},
						},
					},
				},
			},
		}
		resp := kafkaprotocol.ProduceResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)
		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), partResp.ErrorCode)
	}
}

func TestCreatePartitionsInvalidTopics(t *testing.T) {
	testCreatePartitionsInvalidTopics(t, false)
	testCreatePartitionsInvalidTopics(t, true)
}

func testCreatePartitionsInvalidTopics(t *testing.T, validOnly bool) {
	numTopics := 10
	var topicNames []string
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%d", i)
		topicNames = append(topicNames, topicName)
	}
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

	var topics []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic
	for _, topicName := range topicNames {
		topics = append(topics, kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			Name:  common.StrPtr(topicName),
			Count: 23,
		})
	}
	createPartitionsReq := kafkaprotocol.CreatePartitionsRequest{
		ValidateOnly: validOnly,
		Topics:       topics,
	}
	createPartitionsResp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err := conn.SendRequest(&createPartitionsReq, kafkaprotocol.ApiKeyCreatePartitions, 0, createPartitionsResp)
	require.NoError(t, err)
	createPartitionsResp, ok := r.(*kafkaprotocol.CreatePartitionsResponse)
	require.True(t, ok)
	require.Equal(t, numTopics, len(createPartitionsResp.Results))
	for _, topicResp := range createPartitionsResp.Results {
		require.Equal(t, int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition), topicResp.ErrorCode)
	}

}

func TestCreatePartitionsReducePartitions(t *testing.T) {
	testCreatePartitionsReducePartitions(t, false)
	testCreatePartitionsReducePartitions(t, true)
}

func testCreatePartitionsReducePartitions(t *testing.T, validOnly bool) {
	numTopics := 10
	initialPartitionCount := 10
	var topicInfos []topicmeta.TopicInfo
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%d", i)
		topicInfos = append(topicInfos, topicmeta.TopicInfo{
			Name:           topicName,
			PartitionCount: initialPartitionCount,
			RetentionTime:  -1,
		})
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
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

	var topics []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic
	for _, info := range topicInfos {
		topics = append(topics, kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			Name:  common.StrPtr(info.Name),
			Count: int32(initialPartitionCount - 1), // reduce the count
		})
	}
	createPartitionsReq := kafkaprotocol.CreatePartitionsRequest{
		ValidateOnly: validOnly,
		Topics:       topics,
	}
	createPartitionsResp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err := conn.SendRequest(&createPartitionsReq, kafkaprotocol.ApiKeyCreatePartitions, 0, createPartitionsResp)
	require.NoError(t, err)
	createPartitionsResp, ok := r.(*kafkaprotocol.CreatePartitionsResponse)
	require.True(t, ok)
	require.Equal(t, numTopics, len(createPartitionsResp.Results))
	for _, topicResp := range createPartitionsResp.Results {
		require.Equal(t, int16(kafkaprotocol.ErrorCodeInvalidPartitions), topicResp.ErrorCode)
	}

}
