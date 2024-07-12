//go:build integration

package integration

import (
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/server"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

func genTopicName() string {
	return fmt.Sprintf("bridge_test_%s", uuid.New().String())
}

func TestBridgeKafkaInitiallyUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start Kafka then immediately pause it, so we can get address
	kHolder := startKafka(t)
	defer kHolder.stop()
	topicName := genTopicName()
	createTopic(t, topicName, 10, kHolder.address)
	kHolder.pauseResumeKafka(t, true)

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	clusterName := uuid.NewString()
	s, objStore := startStandaloneServerWithObjStore(t, clusterName)
	defer func() {
		err := s.Stop()
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}()
	cli, err := tekclient.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cli.Close()

	err = cli.ExecuteStatement(`local_topic := (kafka in partitions = 10) -> (store stream)`)
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
egest_stream := local_topic -> (bridge to %s props = ("bootstrap.servers" = "%s"))`, topicName, kHolder.address))
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
	ingest_stream := (bridge from %s partitions = 10 props = ("bootstrap.servers" = "%s" "auto.offset.reset" = "earliest")) -> (store stream)`, topicName, kHolder.address))
	require.NoError(t, err)

	tektiteKafkaAddress := s.GetConfig().KafkaServerListenerConfig.Addresses[0]

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": tektiteKafkaAddress,
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	start := time.Now().UTC()

	_, err = sendMessages(2, 10, 0, "local_topic", producer)
	require.NoError(t, err)

	// Messages should have been stored
	// Now unpause

	kHolder.pauseResumeKafka(t, false)

	// Rows should arrive via the bridge from

	waitForRows(t, "ingest_stream", 20, cli, start)

	// Now send some more messages
	_, err = sendMessages(2, 10, 20, "local_topic", producer)
	require.NoError(t, err)

	waitForRows(t, "ingest_stream", 40, cli, start)
}

func TestBridgeSimulateNetworkFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kHolder := startKafka(t)
	defer kHolder.stop()
	topicName := genTopicName()
	createTopic(t, topicName, 10, kHolder.address)

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	clusterName := uuid.NewString()
	s, objStore := startStandaloneServerWithObjStore(t, clusterName)
	defer func() {
		err := s.Stop()
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}()
	cli, err := tekclient.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cli.Close()

	err = cli.ExecuteStatement(`local_topic := (kafka in partitions = 10) -> (store stream)`)
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
egest_stream := local_topic -> (bridge to %s props = ("bootstrap.servers" = "%s"))`, topicName, kHolder.address))
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
	ingest_stream := (bridge from %s partitions = 10 props = ("bootstrap.servers" = "%s" "auto.offset.reset" = "earliest")) -> (store stream)`, topicName, kHolder.address))
	require.NoError(t, err)

	tektiteKafkaAddress := s.GetConfig().KafkaServerListenerConfig.Addresses[0]

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": tektiteKafkaAddress,
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	start := time.Now().UTC()

	log.Debug("sending messages")

	_, err = sendMessages(2, 10, 0, "local_topic", producer)
	require.NoError(t, err)

	log.Debug("sent messages")

	// Rows should arrive via the bridge from

	waitForRows(t, "ingest_stream", 20, cli, start)

	log.Debug("got initial rows")

	// Now pause the Kafka container - simulates a temporary network failure
	kHolder.pauseResumeKafka(t, true)

	log.Debug("sending more messages")

	// Now send some more messages
	_, err = sendMessages(2, 10, 20, "local_topic", producer)
	require.NoError(t, err)

	log.Debug("sent more messages")

	// Wait a little bit
	time.Sleep(2 * time.Second)

	// Unpause the container
	kHolder.pauseResumeKafka(t, false)

	log.Debug("resumed container")

	// Rows should be received - we can get duplicates because if a batch is sent while the kafka container is frozen,
	// the write can be written to the wire but no response is received in time, and the bridge times out.
	// later the kafka container unfreezes and the original write is processed. but the bridge doesn't know it
	// succeeded so will retry it and then there will be duplicates in the remote Kafka. This is not an issue
	// specific to the Tektite bridge - in general with any RPC you do not know whether it succeeded or failed in
	// case of time-out of response. Usually you will then retry as you prefer duplicates to lost messages
	// i.e. you prefer ("at least once", as opposed to "at most once", delivery guarantee).
	waitForRowsIgnoreDups(t, "ingest_stream", 40, cli, start, true)
}

//
//func TestInLoop(t *testing.T) {
//	for i := 0; i < 100000; i++ {
//		log.Infof("iteration %d", i)
//		TestRestartBridgeMessagesStored(t)
//	}
//}

func TestRestartBridgeMessagesStored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	log.Infof("starting test %s", t.Name())

	kHolder := startKafka(t)
	defer kHolder.stop()
	topicName := genTopicName()
	createTopic(t, topicName, 10, kHolder.address)

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	clusterName := uuid.NewString()
	s, objStore := startStandaloneServerWithObjStore(t, clusterName)
	defer func() {
		err := s.Stop()
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}()
	cli, err := tekclient.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cli.Close()

	err = cli.ExecuteStatement(`local_topic := (kafka in partitions = 10) -> (store stream)`)
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
egest_stream := local_topic -> (bridge to %s props = ("bootstrap.servers" = "%s"))`, topicName, kHolder.address))
	require.NoError(t, err)

	err = cli.ExecuteStatement(fmt.Sprintf(`
	ingest_stream := (bridge from %s partitions = 10 props = ("bootstrap.servers" = "%s" "auto.offset.reset" = "earliest")) -> (store stream)`, topicName, kHolder.address))
	require.NoError(t, err)

	tektiteKafkaAddress := s.GetConfig().KafkaServerListenerConfig.Addresses[0]

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": tektiteKafkaAddress,
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	start := time.Now().UTC()

	// pause the kafka container so messages don't get bridged out
	kHolder.pauseResumeKafka(t, true)

	_, err = sendMessages(2, 10, 0, "local_topic", producer)
	require.NoError(t, err)

	log.Infof("%s sent initial messages", t.Name())

	log.Infof("%s shutting down server", t.Name())

	// shutdown the server
	cfg := s.GetConfig()
	err = shutdown.PerformShutdown(&cfg, false)
	require.NoError(t, err)

	log.Infof("%s server is shut down", t.Name())

	log.Infof("%s restarting server", t.Name())

	// restart

	s = startStandaloneServer(t, cfg.DevObjectStoreAddresses[0], clusterName)
	log.Infof("%s restarted server", t.Name())

	// unpause container
	kHolder.pauseResumeKafka(t, false)

	log.Infof("%s unpaused kafka - now waiting for rows", t.Name())

	cli, err = tekclient.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cli.Close()

	// rows should appear
	waitForRowsIgnoreDups(t, "ingest_stream", 20, cli, start, true)
}

func sendMessages(numBatches int, batchSize int, startIndex int, topicName string, producer *kafkago.Producer) ([]*kafkago.Message, error) {
	var msgs []*kafkago.Message
	msgID := startIndex
	for i := 0; i < numBatches; i++ {
		deliveryChan := make(chan kafkago.Event, batchSize)
		ts := time.Now().UTC()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("key%05d", msgID))
			value := []byte(fmt.Sprintf("value%05d", msgID))
			err := producer.Produce(&kafkago.Message{
				TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
				Key:            key,
				Value:          value,
				Timestamp:      ts,
			},
				deliveryChan,
			)
			if err != nil {
				return nil, err
			}
			msgID++
		}
		for j := 0; j < batchSize; j++ {
			e := <-deliveryChan
			m := e.(*kafkago.Message)
			if m.TopicPartition.Error != nil {
				return nil, m.TopicPartition.Error
			}
			log.Debugf("sent message %d", i)
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

func startStandaloneServerWithObjStore(t *testing.T, clusterName string) (*server.Server, *dev.Store) {
	objStoreAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	objStore := dev.NewDevStore(objStoreAddress)
	err = objStore.Start()
	require.NoError(t, err)
	s := startStandaloneServer(t, objStoreAddress, clusterName)
	return s, objStore
}

func startStandaloneServer(t *testing.T, objStoreAddress string, clusterName string) *server.Server {
	cfg := conf.Config{}
	cfg.LogScope = t.Name()
	cfg.ApplyDefaults()
	cfg.ClusterName = clusterName
	cfg.ProcessorCount = 16
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	cfg.CompactionWorkersEnabled = true
	remotingAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.ClusterAddresses = []string{remotingAddress}
	cfg.HttpApiEnabled = true
	apiAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	cfg.HttpApiAddresses = []string{apiAddress}
	cfg.HttpApiEnabled = true
	cfg.HttpApiTlsConfig = tlsConf
	cfg.KafkaServerEnabled = true
	kafkaAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.KafkaServerListenerConfig.Addresses = []string{kafkaAddress}
	cfg.ClientType = conf.KafkaClientTypeConfluent
	cfg.ObjectStoreType = conf.DevObjectStoreType
	cfg.DevObjectStoreAddresses = []string{objStoreAddress}
	cfg.ClusterManagerKeyPrefix = uuid.NewString() // test needs unique etcd namespace
	s, err := server.NewServer(cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	return s
}

type kafkaHolder struct {
	stopped bool
	kc      *kafka.KafkaContainer
	address string
}

func (k *kafkaHolder) stop() {
	if k.stopped {
		return
	}
	if err := k.kc.Stop(context.Background(), nil); err != nil {
		panic(err)
	}
	k.stopped = true
}

func (k *kafkaHolder) pauseResumeKafka(t *testing.T, pause bool) {
	containerID := k.kc.GetContainerID()
	dockerCli, err := client.NewClientWithOpts(client.FromEnv)
	require.NoError(t, err)
	defer func() {
		err := dockerCli.Close()
		require.NoError(t, err)
	}()
	if pause {
		err = dockerCli.ContainerPause(context.Background(), containerID)
		require.NoError(t, err)
	} else {
		err = dockerCli.ContainerUnpause(context.Background(), containerID)
		require.NoError(t, err)
	}
}

func startKafka(t *testing.T) *kafkaHolder {
	ctx := context.Background()
	// Start a container
	kc, err := kafka.RunContainer(ctx,
		kafka.WithClusterID(fmt.Sprintf("test-cluster-%s", uuid.NewString())),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
	)
	require.NoError(t, err)
	// Get the address exposed by the container
	brokers, err := kc.Brokers(context.Background())
	require.NoError(t, err)
	containerKafkaAddress := brokers[0]
	return &kafkaHolder{
		kc:      kc,
		address: containerKafkaAddress,
	}
}

func createTopic(t *testing.T, topicName string, partitions int, serverAddress string) {
	cfg := &kafkago.ConfigMap{
		"bootstrap.servers": serverAddress,
	}
	adminClient, err := kafkago.NewAdminClient(cfg)
	require.NoError(t, err)
	defer adminClient.Close()

	topicSpec := kafkago.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	results, err := adminClient.CreateTopics(context.Background(), []kafkago.TopicSpecification{topicSpec})
	require.NoError(t, err)
	require.Equal(t, kafkago.ErrNoError, results[0].Error.Code())
}
