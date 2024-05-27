package kafkaserver

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestProduce(t *testing.T) {
	topic := "my_topic"

	serverPort := testutils.PortProvider.GetPort(t)

	serverAddress := fmt.Sprintf("localhost:%d", serverPort)

	server, processor := createServer(t, topic, serverPort)

	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()

	sendMessages(t, topic, serverAddress, 100)

	batch := processor.getBatch()
	require.NotNil(t, batch)
}

func sendMessages(t *testing.T, topic string, serverAddress string, numMessages int) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": serverAddress,
		"acks":              "all",
		"debug":             "all",
	})
	require.NoError(t, err)
	defer producer.Close()
	for i := 0; i < numMessages; i++ {
		deliveryChan := make(chan kafka.Event, 1)
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value},
			deliveryChan,
		)
		e := <-deliveryChan
		m := e.(*kafka.Message)
		require.NoError(t, m.TopicPartition.Error)
	}
}

func createServer(t *testing.T, topic string, serverPort int) (*Server, *testProcessor) {

	meta := &testMetadataProvider{}
	meta.brokerInfos = []BrokerInfo{
		{
			NodeID: 0,
			Host:   "localhost",
			Port:   serverPort,
		},
	}
	meta.topicInfos = map[string]*TopicInfo{
		topic: {
			Name:           topic,
			ProduceEnabled: true,
			ProduceInfoProvider: &testProduceInfoProvider{
				receiverID:     10,
				lastOffset:     1001,
				lastAppendTime: 1000000,
			},
			Partitions: []PartitionInfo{
				{
					ID:           0,
					LeaderNodeID: 0,
					ReplicaNodeIDs: []int{
						0,
					},
				},
			}},
	}

	processor := &testProcessor{}
	procProvider := &testProcessorProvider{processor: processor}

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.KafkaServerEnabled = true
	cfg.KafkaServerAddresses = []string{fmt.Sprintf("localhost:%d", serverPort)}

	st := store2.TestStore()

	gc, err := NewGroupCoordinator(cfg, procProvider, &testStreamMgr{}, meta, st, &testBatchForwarder{})
	require.NoError(t, err)
	server := NewServer(cfg, meta, procProvider, gc, st, &testStreamMgr{})
	err = server.Activate()
	require.NoError(t, err)
	return server, processor
}

type testBatchForwarder struct {
}

func (t testBatchForwarder) ForwardBatch(*proc.ProcessBatch, bool, func(error)) {
}

type testStreamMgr struct {
}

func (t testStreamMgr) RegisterSystemSlab(string, int, int, int, *opers.OperatorSchema, []string, bool) error {
	return nil
}

func (t testStreamMgr) RegisterChangeListener(func(streamName string, deployed bool)) {
}

type testMetadataProvider struct {
	controllerNodeID int
	brokerInfos      []BrokerInfo
	topicInfos       map[string]*TopicInfo
}

func (t *testMetadataProvider) GetAllTopics() []*TopicInfo {
	var topicInfos []*TopicInfo
	for _, topicInfo := range t.topicInfos {
		topicInfos = append(topicInfos, topicInfo)
	}
	return topicInfos
}

func (t *testMetadataProvider) ControllerNodeID() int {
	return t.controllerNodeID
}

func (t *testMetadataProvider) BrokerInfos() []BrokerInfo {
	return t.brokerInfos
}

func (t *testMetadataProvider) GetTopicInfo(topicName string) (TopicInfo, bool) {
	topicInfo, ok := t.topicInfos[topicName]
	if !ok {
		return TopicInfo{}, false
	}
	return *topicInfo, true
}

type testProcessorProvider struct {
	processor        *testProcessor
	partitionNodeMap map[int]int
}

func (t *testProcessorProvider) GetProcessor(int) (proc.Processor, bool) {
	return nil, false
}

func (t *testProcessorProvider) NodeForPartition(partitionID int, _ string, _ int) int {
	return t.partitionNodeMap[partitionID]
}

func (t *testProcessorProvider) GetProcessorForPartition(string, int) (proc.Processor, bool) {
	return t.processor, true
}

type testProcessor struct {
	id    int
	lock  sync.Mutex
	batch *proc.ProcessBatch
}

func (t *testProcessor) GetCurrentVersion() int {
	return 0
}

func (t *testProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}

func (t *testProcessor) WriteCache() *proc.WriteCache {
	return nil
}

func (t *testProcessor) ID() int {
	return t.id
}

func (t *testProcessor) IngestBatch(processBatch *proc.ProcessBatch, completionFunc func(error)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.batch = processBatch
	completionFunc(nil)
}

func (t *testProcessor) getBatch() *proc.ProcessBatch {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.batch
}

func (t *testProcessor) IngestBatchSync(*proc.ProcessBatch) error {
	return nil
}

func (t *testProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (t *testProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (t *testProcessor) SetLeader() {
}

func (t *testProcessor) IsLeader() bool {
	return true
}

func (t *testProcessor) CheckInProcessorLoop() {
}

func (t *testProcessor) Stop() {
}

func (t *testProcessor) InvalidateCachedReceiverInfo() {
}

func (t *testProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (t *testProcessor) SetNotIdleNotifier(func()) {
}

func (t *testProcessor) IsIdle(int) bool {
	return false
}

func (t *testProcessor) IsStopped() bool {
	return false
}

func (t *testProcessor) SetReplicator(proc.Replicator) {
}

func (t *testProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (t *testProcessor) SubmitAction(func() error) bool {
	return false
}

func (t *testProcessor) CloseVersion(int, []int) {
}

type testProduceInfoProvider struct {
	receiverID     int
	lastOffset     int64
	lastAppendTime int64
}

func (t *testProduceInfoProvider) IngestBatch(recordBatchBytes []byte, processor proc.Processor, partitionID int, complFunc func(err error)) {
	bytesColBuilder := evbatch.NewBytesColBuilder()
	bytesColBuilder.Append(recordBatchBytes)
	evBatch := evbatch.NewBatch(opers.RecordBatchSchema, bytesColBuilder.Build())
	processBatch := proc.NewProcessBatch(processor.ID(), evBatch,
		t.receiverID, partitionID, -1)
	processor.IngestBatch(processBatch, complFunc)
}

func (t *testProduceInfoProvider) ReceiverID() int {
	return t.receiverID
}

func (t *testProduceInfoProvider) GetLastProducedInfo(int) (int64, int64) {
	return t.lastOffset, t.lastAppendTime
}
