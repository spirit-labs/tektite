package integ

import (
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	miniocl "github.com/spirit-labs/tektite/objstore/minio"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"strconv"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"

	clientKeyPath  = "testdata/selfsignedclientkey.pem"
	clientCertPath = "testdata/selfsignedclientcert.pem"
)

func TestProduceConsumeFranzNoTls(t *testing.T) {
	testProduceConsume(t, NewFranzProducer, NewFranzConsumer, false, false)
}

func TestProduceConsumeFranzServerTls(t *testing.T) {
	testProduceConsume(t, NewFranzProducer, NewFranzConsumer, true, false)
}

func TestProduceConsumeFranzServerAndClientTls(t *testing.T) {
	testProduceConsume(t, NewFranzProducer, NewFranzConsumer, true, true)
}

func TestProduceConsumeKafkaGoNoTls(t *testing.T) {
	testProduceConsume(t, NewKafkaGoProducer, NewKafkaGoConsumer, false, false)
}

func TestProduceConsumeKafkaGoServerTls(t *testing.T) {
	testProduceConsume(t, NewKafkaGoProducer, NewKafkaGoConsumer, true, false)
}

func TestProduceConsumeKafkaGoServerAndClientTls(t *testing.T) {
	testProduceConsume(t, NewKafkaGoProducer, NewKafkaGoConsumer, true, true)
}

func testProduceConsume(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory,
	serverTls bool, clientTls bool) {

	numAgents := 5
	topicName := fmt.Sprintf("test-topic-%s", uuid.New().String())
	agents, tearDown := startAgents(t, numAgents, serverTls, clientTls, compress.CompressionTypeNone, compress.CompressionTypeLz4)
	defer tearDown(t)

	createTopic(t, topicName, 100, agents[0].kafkaListenAddress, serverTls, clientTls)

	address := agents[0].kafkaListenAddress

	producer := createProducer(t, producerFactory, address, serverTls, clientTls, compress.CompressionTypeNone)
	defer func() {
		err := producer.Close()
		require.NoError(t, err)
	}()

	numMessages := 10
	var msgs []kafka.Message
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		msgs = append(msgs, kafka.Message{
			Key:       key,
			Value:     value,
			TimeStamp: time.Now(),
		})
	}
	err := producer.Produce(TopicProduce{
		TopicName: topicName,
		Messages:  msgs,
	})
	require.NoError(t, err)

	consumer := createConsumer(t, consumerFactory, address, topicName, "test_group", serverTls, clientTls)
	require.NoError(t, err)
	defer func() {
		err := consumer.Close()
		require.NoError(t, err)
	}()

	received, err := FetchMessages(numMessages, 5*time.Second, consumer)
	require.NoError(t, err)

	SortMessagesByKey(received)

	require.Equal(t, len(msgs), len(received))

	for i, msg := range msgs {
		require.Equal(t, msg.Key, received[i].Key)
		require.Equal(t, msg.Value, received[i].Value)
	}
}

func startMinioAndCreateBuckets(t *testing.T) (miniocl.Conf, *minio.MinioContainer) {
	minioCfg, minioContainer := startMinio(t)
	// create the buckets
	client := miniocl.NewMinioClient(minioCfg)
	err := client.Start()
	require.NoError(t, err)
	err = client.MakeBucket(context.Background(), "test-cluster-data")
	require.NoError(t, err)
	err = client.MakeBucket(context.Background(), "test-cluster-controller-sm")
	require.NoError(t, err)
	return minioCfg, minioContainer
}

func startAgents(t *testing.T, numAgents int, serverTls bool, clientAuth bool, fetchCompression compress.CompressionType,
	storageCompression compress.CompressionType) ([]*AgentProcess, func(*testing.T)) {
	return startAgentsWithExtraCommandLine(t, numAgents, serverTls, clientAuth, fetchCompression, storageCompression, "")
}

func startAgentsWithExtraCommandLine(t *testing.T, numAgents int, serverTls bool, clientAuth bool, fetchCompression compress.CompressionType,
	storageCompression compress.CompressionType, extraCommandLine string) ([]*AgentProcess, func(*testing.T)) {
	minioCfg, minioContainer := startMinioAndCreateBuckets(t)

	mgr := NewManager()

	tlsConf := ""
	if serverTls {
		tlsConf = fmt.Sprintf(" --kafka-tls-enabled=true --kafka-tls-server-cert-file=%s --kafka-tls-server-private-key-file=%s", serverCertPath, serverKeyPath)
		tlsConf += fmt.Sprintf(" --internal-tls-enabled=true --internal-tls-server-cert-file=%s --internal-tls-server-private-key-file=%s", serverCertPath, serverKeyPath)
		if clientAuth {
			tlsConf += fmt.Sprintf(" --kafka-tls-client-cert-file=%s --kafka-tls-client-auth-type=require-and-verify-client-cert", clientCertPath)
			tlsConf += fmt.Sprintf(" --internal-tls-client-cert-file=%s --internal-tls-client-auth-type=require-and-verify-client-cert", clientCertPath)
			tlsConf += fmt.Sprintf(" --internal-client-tls-enabled=true --internal-client-tls-server-cert-file=%s "+
				"--internal-client-tls-client-private-key-file=%s --internal-client-tls-client-cert-file=%s", serverCertPath, clientKeyPath, clientCertPath)
		} else {
			tlsConf += fmt.Sprintf(" --internal-client-tls-enabled=true --internal-client-tls-server-cert-file=%s", serverCertPath)
		}
	}

	var agents []*AgentProcess
	for i := 0; i < numAgents; i++ {
		commandLine := fmt.Sprintf("--obj-store-username=minioadmin --obj-store-password=minioadmin --obj-store-url=%s ", minioCfg.Endpoint) +
			"--cluster-name=test-cluster --location=az1 --kafka-listen-address=localhost:0 --internal-listen-address=localhost:0 " +
			"--membership-update-interval-ms=100 --membership-eviction-interval-ms=2000 " +
			"--consumer-group-initial-join-delay-ms=100 " +
			fmt.Sprintf("--storage-compression-type=%s ", storageCompression.String()) +
			fmt.Sprintf("--fetch-compression-type=%s ", fetchCompression.String()) +
			`--log-level=info`
		commandLine += tlsConf
		if extraCommandLine != "" {
			commandLine += " " + extraCommandLine
		}
		log.Debugf("command line: %s", commandLine)
		agent, err := mgr.StartAgent(commandLine, false)
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	return agents, func(t *testing.T) {
		for _, agent := range agents {
			if err := agent.Stop(); err != nil {
				log.Errorf("failed to stop agent cleanly: %v", err)
			}
		}
		err := minioContainer.Terminate(context.Background())
		require.NoError(t, err)
	}
}

func createTopic(t *testing.T, topicName string, partitions int, address string, serverTls bool, clientTls bool) {
	adminClient, err := newAdminProducer(address, serverTls, clientTls)
	require.NoError(t, err)
	defer adminClient.Close()
	_, err = adminClient.CreateTopics(context.Background(), []kafkago.TopicSpecification{
		{
			Topic:         topicName,
			NumPartitions: partitions,
		},
	})
	require.NoError(t, err)
}

func startMinio(t *testing.T) (miniocl.Conf, *minio.MinioContainer) {
	ctx := context.Background()
	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-26T15-33-07Z",
		minio.WithUsername("minioadmin"), minio.WithPassword("minioadmin"))
	require.NoError(t, err)
	cfg := miniocl.Conf{}
	ip, err := minioContainer.Host(ctx)
	require.NoError(t, err)
	port, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	cfg.Username = "minioadmin"
	cfg.Password = "minioadmin"
	cfg.Endpoint = fmt.Sprintf("%s:%d", ip, port.Int())
	return cfg, minioContainer
}

func createProducer(t *testing.T, factory ProducerFactory, bootstrapAddress string, serverTls bool, clientTls bool,
	compressionType compress.CompressionType) Producer {
	clientKey := ""
	clientCert := ""
	if clientTls {
		clientKey = clientKeyPath
		clientCert = clientCertPath
	}
	producer, err := factory(bootstrapAddress, serverTls, serverCertPath, clientCert, clientKey, compressionType, "az1")
	require.NoError(t, err)
	return producer
}

func createConsumer(t *testing.T, factory ConsumerFactory, address string, topicName string, groupID string,
	serverTls bool, clientTls bool) Consumer {
	clientKey := ""
	clientCert := ""
	if clientTls {
		clientKey = clientKeyPath
		clientCert = clientCertPath
	}
	consumer, err := factory(address, groupID, serverTls, serverCertPath, clientCert, clientKey, "az1")
	require.NoError(t, err)
	err = consumer.Subscribe(topicName)
	require.NoError(t, err)
	return consumer
}

func newAdminProducer(address string, serverTls bool, clientTls bool) (*kafkago.AdminClient, error) {
	clientKey := ""
	clientCert := ""
	if clientTls {
		clientKey = clientKeyPath
		clientCert = clientCertPath
	}
	cm := kafkago.ConfigMap{
		"partitioner":        "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers":  address,
		"acks":               "all",
		"enable.idempotence": strconv.FormatBool(true),
		"client.id":          "tek_az=az1",
	}
	if serverTls {
		cm = configureForTls(cm, serverCertPath, clientCert, clientKey)
	}
	return kafkago.NewAdminClient(&cm)
}
