package integ

import (
	"bytes"
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	miniocl "github.com/spirit-labs/tektite/objstore/minio"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestProduceConsume(t *testing.T) {

	minioCfg, minioContainer := startMinio(t)
	defer func() {
		ctx := context.Background()
		err := minioContainer.Terminate(ctx)
		require.NoError(t, err)
	}()
	// create the buckets
	client := miniocl.NewMinioClient(minioCfg)
	err := client.Start()
	require.NoError(t, err)
	err = client.MakeBucket(context.Background(), "test-cluster-data")
	require.NoError(t, err)
	err = client.MakeBucket(context.Background(), "test-cluster-controller-sm")
	require.NoError(t, err)

	mgr := NewManager()

	topicName := fmt.Sprintf("test-topic-%s", uuid.New().String())

	numAgents := 5
	var agents []*AgentProcess
	for i := 0; i < numAgents; i++ {
		commandLine := fmt.Sprintf("--obj-store-username=minioadmin --obj-store-password=minioadmin --obj-store-url=%s ", minioCfg.Endpoint) +
			"--cluster-name=test-cluster --location=az1 --kafka-listen-address=localhost:0 --internal-listen-address=localhost:0 " +
			"--membership-update-interval-ms=100 --membership-eviction-interval-ms=2000 " +
			"--consumer-group-initial-join-delay-ms=500 " +
			`--log-level=info ` +
			fmt.Sprintf("--topic-name=%s", topicName) //+
		agent, err := mgr.StartAgent(commandLine, false)
		require.NoError(t, err)
		agents = append(agents, agent)
	}

	address := agents[0].kafkaListenAddress
	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":        "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers":  address,
		"acks":               "all",
		"enable.idempotence": strconv.FormatBool(true),
	})
	require.NoError(t, err)

	numMessages := 10
	deliveryChan := make(chan kafkago.Event, numMessages)

	var msgs []*kafkago.Message
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		err := producer.Produce(&kafkago.Message{
			TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
			Key:            key,
			Value:          value},
			deliveryChan,
		)
		require.NoError(t, err)
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		if m.TopicPartition.Error != nil {
			require.NoError(t, m.TopicPartition.Error)
		}
		msgs = append(msgs, m)
	}

	cm := &kafkago.ConfigMap{
		"bootstrap.servers":  address,
		"group.id":           "test_group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		//"debug":              "all",
	}
	consumer, err := kafkago.NewConsumer(cm)
	require.NoError(t, err)

	err = consumer.Subscribe(topicName, nil)
	require.NoError(t, err)

	var received []*kafkago.Message
	start := time.Now()
	for len(received) < numMessages {
		msg, err := consumer.ReadMessage(5 * time.Second)
		if err != nil {
			if err.(kafkago.Error).Code() == kafkago.ErrTimedOut {
				require.True(t, time.Now().Sub(start) <= 1*time.Hour, "timed out waiting to consume messages")
				continue
			}
			require.NoError(t, err)
		}
		require.NotNil(t, msg)
		received = append(received, msg)
	}

	// sort by key
	sort.SliceStable(received, func(i, j int) bool {
		return bytes.Compare(received[i].Key, received[j].Key) < 0
	})

	require.Equal(t, len(msgs), len(received))

	for i, msg := range msgs {
		require.Equal(t, msg.Key, received[i].Key)
		require.Equal(t, msg.Value, received[i].Value)
	}

	producer.Close()

	err = consumer.Close()
	require.NoError(t, err)

	for _, agent := range agents {
		err := agent.Stop()
		require.NoError(t, err)
	}
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
