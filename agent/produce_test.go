package agent

import (
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/streammeta"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

func TestProduceSimple(t *testing.T) {
	topicName := "test-topic-1"
	topicID := 1234
	partitionID := 12
	topicInfos := map[string]streammeta.TopicInfo{
		topicName: {
			TopicID:        topicID,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(partitionID),
						Records: [][]byte{
							batch,
						},
					},
				},
			},
		},
	}

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp := produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()

	verifyBatchesWritten(t, topicID, partitionID, 0, [][]byte{batch}, controllerCl,
		agent.Conf().PusherConf.DataBucketName, objStore)
}

func TestProduceMultipleTopicsAndPartitions(t *testing.T) {

	numTopics := 10
	numPartitionsPerTopic := 10

	topicInfos := map[string]streammeta.TopicInfo{}
	for i := 0; i < numTopics; i++ {
		topicInfos[fmt.Sprintf("topic-%02d", i)] = streammeta.TopicInfo{
			TopicID:        1000 + i,
			PartitionCount: numPartitionsPerTopic,
		}
	}

	cfg := NewConf()
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	var batches [][]byte
	var topicData []kafkaprotocol.ProduceRequestTopicProduceData
	for i := 0; i < numTopics; i++ {
		partitionData := []kafkaprotocol.ProduceRequestPartitionProduceData{}
		for j := 0; j < numPartitionsPerTopic; j++ {
			batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
			batches = append(batches, batch)
			partitionData = append(partitionData, kafkaprotocol.ProduceRequestPartitionProduceData{
				Index:   int32(j),
				Records: [][]byte{batch},
			})
		}
		topicData = append(topicData, kafkaprotocol.ProduceRequestTopicProduceData{
			Name:          strPtr(fmt.Sprintf("topic-%02d", i)),
			PartitionData: partitionData,
		})
	}

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData:       topicData,
	}

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, numTopics, len(produceResp.Responses))
	for _, produceResp := range produceResp.Responses {
		require.Equal(t, numPartitionsPerTopic, len(produceResp.PartitionResponses))
		for _, partitionResp := range produceResp.PartitionResponses {
			require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partitionResp.ErrorCode)
			require.Equal(t, (*string)(nil), partitionResp.ErrorMessage)
		}
	}

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()

	pos := 0
	for i := 0; i < numTopics; i++ {
		for j := 0; j < numPartitionsPerTopic; j++ {
			batch := batches[pos]
			pos++
			verifyBatchesWritten(t, i+1000, j, 0, [][]byte{batch}, controllerCl,
				agent.Conf().PusherConf.DataBucketName, objStore)
		}
	}
}

func TestProduceMultipleBatches(t *testing.T) {
	topicName := "test-topic-1"
	topicID := 1234
	partitionID := 12
	topicInfos := map[string]streammeta.TopicInfo{
		topicName: {
			TopicID:        topicID,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	cfg.PusherConf.WriteTimeout = 1 * time.Millisecond
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	numBatches := 100
	recordsPerBatch := 100

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	var batches [][]byte
	offsetStart := 0
	for i := 0; i < numBatches; i++ {
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(offsetStart, recordsPerBatch)
		offsetStart += recordsPerBatch
		batches = append(batches, batch)
		req := kafkaprotocol.ProduceRequest{
			TransactionalId: nil,
			Acks:            -1,
			TimeoutMs:       1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: strPtr(topicName),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index: int32(partitionID),
							Records: [][]byte{
								batch,
							},
						},
					},
				},
			},
		}

		var resp kafkaprotocol.ProduceResponse
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)
		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
		require.Equal(t, (*string)(nil), partResp.ErrorMessage)
	}

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()
	verifyBatchesWritten(t, topicID, partitionID, 0, batches, controllerCl,
		agent.Conf().PusherConf.DataBucketName, objStore)
}

func TestProduceSimpleWithReload(t *testing.T) {
	topicName := "test-topic-1"
	topicID := 1234
	partitionID := 12
	topicInfos := map[string]streammeta.TopicInfo{
		topicName: {
			TopicID:        topicID,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	batch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr(topicName),
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

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp := produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	err = agent.Stop()
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)

	// Restart agent, and send another batch

	agent, tearDown = setupAgentWithObjStore(t, topicInfos, cfg, objStore)
	defer tearDown(t)

	batch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(100, 100)
	req = kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(partitionID),
						Records: [][]byte{
							batch2,
						},
					},
				},
			},
		},
	}

	cl, err = NewKafkaApiClient()
	require.NoError(t, err)

	conn, err = cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)

	resp = kafkaprotocol.ProduceResponse{}
	r, err = conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok = r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp = produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()

	verifyBatchesWritten(t, topicID, partitionID, 0, [][]byte{batch1, batch2}, controllerCl,
		agent.Conf().PusherConf.DataBucketName, objStore)
}

func setupAgentWithObjStore(t *testing.T, topicInfos map[string]streammeta.TopicInfo, cfg Conf,
	objStore objstore.Client) (*Agent, func(t *testing.T)) {
	kafkaAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.KafkaListenerConfig.Address = kafkaAddress
	topicProvider := &testTopicProvider{
		topicInfos: topicInfos,
	}
	localTransports := transport.NewLocalTransports()
	transportServer, err := localTransports.NewLocalServer("test-address")
	require.NoError(t, err)
	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	agent, err := NewAgentWithFactories(cfg, objStore, topicProvider, localTransports.CreateConnection, transportServer, inMemMemberships.NewMembership)
	require.NoError(t, err)
	err = agent.Start()
	require.NoError(t, err)
	tearDown := func(t *testing.T) {
		err := agent.Stop()
		require.NoError(t, err)
		inMemMemberships.Stop()
	}
	return agent, tearDown
}

func setupAgent(t *testing.T, topicInfos map[string]streammeta.TopicInfo, cfg Conf) (*Agent, *dev.InMemStore, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	agent, tearDown := setupAgentWithObjStore(t, topicInfos, cfg, objStore)
	return agent, objStore, tearDown
}

func verifyBatchesWritten(t *testing.T, topicID int, partitionID int, offsetStart int, batches [][]byte,
	controllerCl control.Client, dataBucketName string, objStore objstore.Client) {
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	prefix, err := partHashes.GetPartitionHash(topicID, partitionID)
	require.NoError(t, err)

	keyStart := encoding.KeyEncodeInt(common.ByteSliceCopy(prefix), int64(offsetStart))
	keyEnd := common.IncBigEndianBytes(prefix)

	ids, err := controllerCl.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)

	tg := &tableGetter{
		bucketName: dataBucketName,
		objStore:   objStore,
	}
	var iters []iteration.Iterator
	for _, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			info := nonOverLapIDs[0]
			iter, err := sst.NewLazySSTableIterator(info.ID, tg, keyStart, keyEnd)
			require.NoError(t, err)
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tg, keyStart, keyEnd)
				require.NoError(t, err)
				itersInChain[j] = iter
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	mi, err := iteration.NewMergingIterator(iters, false, math.MaxUint64)
	require.NoError(t, err)

	expectedOffset := offsetStart
	for _, expectedBatch := range batches {
		ok, kv, err := mi.Next()
		require.NoError(t, err)
		require.True(t, ok)
		expectedKey := encoding.KeyEncodeInt(common.ByteSliceCopy(prefix), int64(expectedOffset))
		expectedKey = encoding.EncodeVersion(expectedKey, 0)
		require.Equal(t, kv.Key, expectedKey)
		recordBatch := kv.Value
		require.Equal(t, expectedBatch, recordBatch)
		numRecords := pusher.NumRecords(recordBatch)
		expectedOffset += numRecords
	}
	ok, _, err := mi.Next()
	require.NoError(t, err)
	require.False(t, ok)
}

type tableGetter struct {
	bucketName string
	objStore   objstore.Client
}

func (n *tableGetter) GetSSTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	buff, err := n.objStore.Get(context.Background(), n.bucketName, string(tableID))
	if err != nil {
		return nil, err
	}
	var table sst.SSTable
	table.Deserialize(buff, 0)
	return &table, nil
}

type testTopicProvider struct {
	topicInfos map[string]streammeta.TopicInfo
}

func (t *testTopicProvider) GetTopicInfo(topicName string) (streammeta.TopicInfo, bool) {
	info, ok := t.topicInfos[topicName]
	return info, ok
}

func (t *testTopicProvider) GetAllTopics() ([]streammeta.TopicInfo, error) {
	var allTopics []streammeta.TopicInfo
	for _, info := range t.topicInfos {
		allTopics = append(allTopics, info)
	}
	return allTopics, nil
}
