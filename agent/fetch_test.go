package agent

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchSimpleV2(t *testing.T) {
	testFetchSimple(t, 2)
}

func TestFetchSimpleV3(t *testing.T) {
	testFetchSimple(t, 3)
}

func testFetchSimple(t *testing.T, apiVersion int16) {
	topicName := "test-topic-1"
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:                topicName,
			PartitionCount:      100,
			MaxMessageSizeBytes: math.MaxInt,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	address := agent.Conf().KafkaListenerConfig.Address
	batch := produceBatch(t, topicName, partitionID, address)

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	fetchOffset := 0

	fetchReq := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  math.MaxInt32,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(partitionID),
						FetchOffset:       int64(fetchOffset),
						PartitionMaxBytes: math.MaxInt32,
					},
				},
			},
		},
	}
	if apiVersion == 2 {
		fetchReq.MaxBytes = 0 // not supported in V2
	}

	fetchResp := kafkaprotocol.FetchResponse{}

	r, err := conn.SendRequest(&fetchReq, kafkaprotocol.APIKeyFetch, apiVersion, &fetchResp)
	res, ok := r.(*kafkaprotocol.FetchResponse)
	require.True(t, ok)

	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(res.ErrorCode))
	require.Equal(t, 1, len(res.Responses))
	topicResp := res.Responses[0]
	require.Equal(t, topicName, *topicResp.Topic)
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
	require.Equal(t, 100, int(partResp.HighWatermark))
	require.Equal(t, batch, partResp.Records)
}

func TestFetchSingleSenderAndFetcherShortWriteTimeout(t *testing.T) {
	testFetch(t, 3, 1*time.Millisecond, 100, 1, 1)
}

func TestFetchMultipleSendersAndFetchersShortWriteTimeout(t *testing.T) {
	testFetch(t, 3, 1*time.Millisecond, 100, 5, 5)
}

func TestFetchMultipleSendersAndFetchersLongWriteTimeout(t *testing.T) {
	testFetch(t, 3, 100*time.Millisecond, 100, 5, 5)
}

func testFetch(t *testing.T, numAgents int, writeTimeout time.Duration, numBatchesPerProduceRunner int,
	numProducers int, numFetchers int) {

	topicName := "test-topic-1"
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:                topicName,
			PartitionCount:      100,
			RetentionTime:       -1,
			MaxMessageSizeBytes: math.MaxInt,
		},
	}
	cfg := NewConf()
	cfg.PusherConf.WriteTimeout = writeTimeout
	cfg.FetchCacheConf.MaxSizeBytes = 16 * 1024 * 1024
	var agents []*Agent
	var tearDowns []func(*testing.T)
	objStore := dev.NewInMemStore(0)
	memberships := NewInMemClusterMemberships()
	memberships.Start()
	defer memberships.Stop()
	localTransports := transport.NewLocalTransports()
	for i := 0; i < numAgents; i++ {
		agent, tearDown := setupAgentWithArgs(t, cfg, objStore, memberships, localTransports)
		agents = append(agents, agent)
		tearDowns = append(tearDowns, tearDown)
	}
	setupTopics(t, agents[0], topicInfos)

	defer func() {
		for _, tearDown := range tearDowns {
			tearDown(t)
		}
	}()

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)
	var connections []*apiclient.KafkaApiConnection
	for _, agent := range agents {
		conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
		require.NoError(t, err)
		connections = append(connections, conn)
	}
	defer func() {
		for _, conn := range connections {
			err := conn.Close()
			require.NoError(t, err)
		}
	}()

	var fetchRunners []*fetchRunner

	for i := 0; i < numFetchers; i++ {
		runner := &fetchRunner{
			topicName:   topicName,
			partitionID: partitionID,
			numBatches:  numBatchesPerProduceRunner * numProducers,
			maxBytes:    1000,
			maxWaitMs:   500,
			connections: connections,
		}
		fetchRunners = append(fetchRunners, runner)
		runner.start()
	}

	var offsetSeq int64

	var sendRunners []*sendRunner
	for i := 0; i < numProducers; i++ {
		runner := &sendRunner{
			id:                 i,
			topicName:          topicName,
			partitionID:        partitionID,
			numBatches:         numBatchesPerProduceRunner,
			connections:        connections,
			offset:             0,
			maxRecordsPerBatch: 100,
			offsetSeq:          &offsetSeq,
		}
		sendRunners = append(sendRunners, runner)
		runner.start()
	}

	var sentBatches [][]byte

	for _, runner := range sendRunners {
		runner.waitComplete()
		sentBatches = append(sentBatches, runner.getBatches()...)
	}

	for _, runner := range fetchRunners {
		runner.waitComplete()
		receivedBatches := runner.getBatches()
		require.Equal(t, numProducers*numBatchesPerProduceRunner, len(receivedBatches))
		require.Equal(t, len(sentBatches), len(receivedBatches))
		for _, sentBatch := range sentBatches {
			found := false
			for j := 0; j < len(receivedBatches); j++ {
				// We don't compare the first 8 bytes as that is base offset and is set on the server
				if bytes.Equal(sentBatch[8:], receivedBatches[j][8:]) {
					found = true
					break
				}
			}
			require.True(t, found)
		}
	}
}

type sendRunner struct {
	lock               sync.Mutex
	id                 int
	topicName          string
	partitionID        int
	numBatches         int
	stopWg             sync.WaitGroup
	connections        []*apiclient.KafkaApiConnection
	batches            [][]byte
	offset             int64
	maxRecordsPerBatch int
	offsetSeq          *int64
}

func (p *sendRunner) start() {
	p.stopWg.Add(1)
	go p.loop()
}

func (p *sendRunner) waitComplete() {
	p.stopWg.Wait()
}

func (p *sendRunner) getBatches() [][]byte {
	p.lock.Lock()
	defer p.lock.Unlock()
	copied := make([][]byte, len(p.batches))
	copy(copied, p.batches)
	return copied
}

func (p *sendRunner) loop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer p.stopWg.Done()
	for len(p.batches) < p.numBatches {
		p.produce()
	}
}

func (p *sendRunner) produce() {
	numRecords := rand.Intn(p.maxRecordsPerBatch) + 1
	offset := atomic.AddInt64(p.offsetSeq, int64(numRecords)) - int64(numRecords)
	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(int(offset), numRecords)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(p.topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   int32(p.partitionID),
						Records: batch,
					},
				},
			},
		},
	}

	resp := p.sendProduce(&req)
	partResp := resp.Responses[0].PartitionResponses[0]
	if partResp.ErrorCode != kafkaprotocol.ErrorCodeNone {
		panic(fmt.Sprintf("produce returned error code %d", partResp.ErrorCode))
	}
	p.batches = append(p.batches, batch)
}

func (f *sendRunner) sendProduce(req *kafkaprotocol.ProduceRequest) *kafkaprotocol.ProduceResponse {
	index := rand.Intn(len(f.connections))
	conn := f.connections[index]
	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyProduce, 3, &resp)
	if err != nil {
		panic(fmt.Sprintf("failed to send produce request: %v", err))
	}
	res := r.(*kafkaprotocol.ProduceResponse)
	return res
}

type fetchRunner struct {
	lock        sync.Mutex
	topicName   string
	partitionID int
	numBatches  int
	maxBytes    int
	maxWaitMs   int
	stopWg      sync.WaitGroup
	connections []*apiclient.KafkaApiConnection
	fetchOffset int64
	batches     [][]byte
}

func (f *fetchRunner) start() {
	f.stopWg.Add(1)
	go f.loop()
}

func (f *fetchRunner) waitComplete() {
	f.stopWg.Wait()
}

func (f *fetchRunner) getBatches() [][]byte {
	f.lock.Lock()
	defer f.lock.Unlock()
	copied := make([][]byte, len(f.batches))
	copy(copied, f.batches)
	return copied
}

func (f *fetchRunner) loop() {
	f.lock.Lock()
	defer f.lock.Unlock()
	defer f.stopWg.Done()
	for len(f.batches) < f.numBatches {
		f.fetch()
	}
}

func (f *fetchRunner) fetch() {
	maxBytes := int32(1 + rand.Intn(f.maxBytes))
	maxWait := int32(rand.Intn(f.maxWaitMs))
	minBytes := maxBytes / 2
	fetchReq := kafkaprotocol.FetchRequest{
		MaxWaitMs: maxWait,
		MinBytes:  minBytes,
		MaxBytes:  maxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(f.topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(f.partitionID),
						FetchOffset:       f.fetchOffset,
						PartitionMaxBytes: math.MaxInt32,
					},
				},
			},
		},
	}
	resp := f.sendFetch(&fetchReq)
	partResp := resp.Responses[0].Partitions[0]
	if partResp.ErrorCode != kafkaprotocol.ErrorCodeNone {
		panic(fmt.Sprintf("fetch got error %d", partResp.ErrorCode))
	}
	if len(partResp.Records) > 0 {
		batches := extractBatches(partResp.Records)
		f.batches = append(f.batches, batches...)
		for _, batch := range batches {
			baseOffset := kafkaencoding.BaseOffset(batch)
			if f.fetchOffset != baseOffset {
				panic(fmt.Sprintf("fetch got offset %d, want %d", baseOffset, f.fetchOffset))
			}
			numRecords := kafkaencoding.NumRecords(batch)
			f.fetchOffset += int64(numRecords)
			if partResp.HighWatermark <= f.fetchOffset-1 {
				panic(fmt.Sprintf("received last offset %d but high watermark is %d", f.fetchOffset-1, partResp.HighWatermark))
			}
		}
	}
}

func extractBatches(buff []byte) [][]byte {
	// Multiple record batches are concatenated together
	var batches [][]byte
	for {
		batchLen := binary.BigEndian.Uint32(buff[8:])
		batch := buff[:int(batchLen)+12] // 12: First two fields are not included in size
		batches = append(batches, batch)
		if int(batchLen)+12 == len(buff) {
			break
		}
		buff = buff[int(batchLen)+12:]
	}
	return batches
}

func (f *fetchRunner) sendFetch(req *kafkaprotocol.FetchRequest) *kafkaprotocol.FetchResponse {
	index := rand.Intn(len(f.connections))
	conn := f.connections[index]
	var resp kafkaprotocol.FetchResponse
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyFetch, 3, &resp)
	if err != nil {
		panic(fmt.Sprintf("failed to send fetch request: %v", err))
	}
	res := r.(*kafkaprotocol.FetchResponse)
	return res
}

func produceBatch(t *testing.T, topicName string, partitionID int, address string) []byte {
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
						Index:   int32(partitionID),
						Records: batch,
					},
				},
			},
		},
	}
	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)
	conn, err := cl.NewConnection(address)
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
	return batch
}

func waitForDeliveredClusterVersion(t *testing.T, agents ...*Agent) {
	for _, agent := range agents {
		testutils.WaitUntil(t, func() (bool, error) {
			return agent.DeliveredClusterVersion() > 0, nil
		})
	}
}
