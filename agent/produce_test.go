package agent

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
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
	topicID := 1001
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           "footopic",
			PartitionCount: 23,
		},
		{
			Name:           topicName,
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
				Name: common.StrPtr(topicName),
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

	var topicInfos []topicmeta.TopicInfo
	for i := 0; i < numTopics; i++ {
		topicInfos = append(topicInfos, topicmeta.TopicInfo{
			Name:           fmt.Sprintf("topic-%02d", i),
			PartitionCount: numPartitionsPerTopic,
		})
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
			Name:          common.StrPtr(fmt.Sprintf("topic-%02d", i)),
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
	topicID := 1001
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           "footopic",
			PartitionCount: 23,
		},
		{
			Name:           topicName,
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
					Name: common.StrPtr(topicName),
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
	topicID := 1001
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           "footopic",
			PartitionCount: 23,
		},
		{
			Name:           topicName,
			ID:             topicID,
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

	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports := transport.NewLocalTransports()
	agent, tearDown = setupAgentWithArgs(t, cfg, objStore, inMemMemberships, localTransports)
	defer tearDown(t)

	batch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(100, 100)
	req = kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
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

func setupAgentWithArgs(t *testing.T, cfg Conf, objStore objstore.Client, inMemMemberships *InMemClusterMemberships, localTransports *transport.LocalTransports) (*Agent, func(t *testing.T)) {
	kafkaAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.KafkaListenerConfig.Address = kafkaAddress
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	agent, err := NewAgentWithFactories(cfg, objStore, localTransports.CreateConnection, transportServer, inMemMemberships.NewMembership)
	require.NoError(t, err)
	err = agent.Start()
	require.NoError(t, err)
	waitForDeliveredClusterVersion(t, agent)
	tearDown := func(t *testing.T) {
		err := agent.Stop()
		require.NoError(t, err)
	}
	return agent, tearDown
}

func setupAgent(t *testing.T, topicInfos []topicmeta.TopicInfo, cfg Conf) (*Agent, *dev.InMemStore, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports := transport.NewLocalTransports()
	agent, tearDown := setupAgentWithArgs(t, cfg, objStore, inMemMemberships, localTransports)
	setupTopics(t, agent, topicInfos)
	return agent, objStore, tearDown
}

func setupTopics(t *testing.T, agent *Agent, infos []topicmeta.TopicInfo) {
	controller := agent.controller
	var cl control.Client
outer:
	for _, info := range infos {
		for {
			var err error
			if cl == nil {
				cl, err = controller.Client()
			}
			if err == nil {
				err = cl.CreateTopic(info)
				if err == nil {
					continue outer
				}
			}
			if !common.IsUnavailableError(err) {
				require.NoError(t, err)
				return
			}
			// retry
			if cl != nil {
				err = cl.Close()
				require.NoError(t, err)
				cl = nil
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func verifyBatchesWritten(t *testing.T, topicID int, partitionID int, offsetStart int, batches [][]byte,
	controllerCl control.Client, dataBucketName string, objStore objstore.Client) {
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	prefix, err := partHashes.GetPartitionHash(topicID, partitionID)
	require.NoError(t, err)
	keyStart := append(common.ByteSliceCopy(prefix), common.EntryTypeTopicData)
	keyStart = encoding.KeyEncodeInt(keyStart, int64(offsetStart))
	keyStart = append(keyStart, common.EntryTypeTopicData)
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
			iter, err := sst.NewLazySSTableIterator(info.ID, tg.GetSSTable, keyStart, keyEnd)
			require.NoError(t, err)
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tg.GetSSTable, keyStart, keyEnd)
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
		expectedKey := append(common.ByteSliceCopy(prefix), common.EntryTypeTopicData)
		expectedKey = encoding.KeyEncodeInt(expectedKey, int64(expectedOffset))
		expectedKey = encoding.EncodeVersion(expectedKey, 0)
		require.Equal(t, kv.Key, expectedKey)
		recordBatch := kv.Value
		require.Equal(t, expectedBatch, recordBatch)
		numRecords := kafkaencoding.NumRecords(recordBatch)
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
