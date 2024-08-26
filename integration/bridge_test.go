//go:build integration

package integration

import (
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/stretchr/testify/require"
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

	clientTLSConfig := client.TLSConfig{
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
	cli, err := client.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
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

	waitForRowsIgnoreDups(t, "ingest_stream", 20, cli, start, true)

	// Now send some more messages
	_, err = sendMessages(2, 10, 20, "local_topic", producer)
	require.NoError(t, err)

	waitForRowsIgnoreDups(t, "ingest_stream", 40, cli, start, true)
}

func TestBridgeSimulateNetworkFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kHolder := startKafka(t)
	defer kHolder.stop()
	topicName := genTopicName()
	createTopic(t, topicName, 10, kHolder.address)

	clientTLSConfig := client.TLSConfig{
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
	cli, err := client.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
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

func TestRestartBridgeMessagesStored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	log.Debugf("starting test %s", t.Name())

	kHolder := startKafka(t)
	defer kHolder.stop()
	topicName := genTopicName()
	createTopic(t, topicName, 10, kHolder.address)

	clientTLSConfig := client.TLSConfig{
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
	cli, err := client.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
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

	log.Debugf("%s sent initial messages", t.Name())

	log.Debugf("%s shutting down server", t.Name())

	// shutdown the server
	cfg := s.GetConfig()
	err = shutdown.PerformShutdown(&cfg, false)
	require.NoError(t, err)

	log.Debugf("%s server is shut down", t.Name())

	log.Debugf("%s restarting server", t.Name())

	// restart

	s = startStandaloneServer(t, cfg.DevObjectStoreAddresses[0], clusterName)
	log.Debugf("%s restarted server", t.Name())

	// unpause container
	kHolder.pauseResumeKafka(t, false)

	log.Debugf("%s unpaused kafka - now waiting for rows", t.Name())

	cli, err = client.NewClient(s.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cli.Close()

	// rows should appear
	waitForRowsIgnoreDups(t, "ingest_stream", 20, cli, start, true)
}
