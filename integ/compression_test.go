package integ

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestProducerCompressionFranz(t *testing.T) {
	testProducerCompression(t, NewFranzProducer, NewFranzConsumer)
}

func TestFetchCompressionFranz(t *testing.T) {
	testFetchCompression(t, NewFranzProducer, NewFranzConsumer)
}

func TestStorageCompressionFranz(t *testing.T) {
	testStorageCompression(t, NewFranzProducer, NewFranzConsumer)
}

func TestProducerCompressionKafkaGo(t *testing.T) {
	testProducerCompression(t, NewKafkaGoProducer, NewKafkaGoConsumer)
}

func TestFetchCompressionKafkaGo(t *testing.T) {
	testFetchCompression(t, NewKafkaGoProducer, NewKafkaGoConsumer)
}

func TestStorageCompressionKafkaGo(t *testing.T) {
	testStorageCompression(t, NewKafkaGoProducer, NewKafkaGoConsumer)
}

func testProducerCompression(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory) {
	fetchCompression := compress.CompressionType(rand.Intn(int(compress.CompressionTypeZstd) + 1))
	storageCompression := compress.CompressionType(rand.Intn(2) + int(compress.CompressionTypeLz4))
	for producerCompression := compress.CompressionTypeNone; producerCompression <= compress.CompressionTypeZstd; producerCompression++ {
		testName := fmt.Sprintf("%s-ProducerCompression-%s-FetchCompression-%s-StorageCompression-%s",
			t.Name(), producerCompression, fetchCompression, storageCompression)
		t.Run(testName, func(t *testing.T) {
			testCompression(t, producerFactory, consumerFactory, producerCompression, fetchCompression, storageCompression)
		})
	}
}

func testFetchCompression(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory) {
	producerCompression := compress.CompressionType(rand.Intn(int(compress.CompressionTypeZstd) + 1))
	storageCompression := compress.CompressionType(rand.Intn(2) + int(compress.CompressionTypeLz4))
	for fetchCompression := compress.CompressionTypeNone; fetchCompression <= compress.CompressionTypeZstd; fetchCompression++ {
		testName := fmt.Sprintf("%s-ProducerCompression-%s-FetchCompression-%s-StorageCompression-%s",
			t.Name(), producerCompression, fetchCompression, storageCompression)
		t.Run(testName, func(t *testing.T) {
			testCompression(t, producerFactory, consumerFactory, producerCompression, fetchCompression, storageCompression)
		})
	}
}

func testStorageCompression(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory) {
	producerCompression := compress.CompressionType(rand.Intn(int(compress.CompressionTypeZstd) + 1))
	fetchCompression := compress.CompressionType(rand.Intn(int(compress.CompressionTypeZstd) + 1))
	for storageCompression := compress.CompressionTypeLz4; storageCompression <= compress.CompressionTypeZstd; storageCompression++ {
		testName := fmt.Sprintf("%s-ProducerCompression-%s-FetchCompression-%s-StorageCompression-%s",
			t.Name(), producerCompression, fetchCompression, storageCompression)
		t.Run(testName, func(t *testing.T) {
			testCompression(t, producerFactory, consumerFactory, producerCompression, fetchCompression, storageCompression)
		})
	}
}

func testCompression(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory,
	producerCompression compress.CompressionType, fetchCompression compress.CompressionType,
	storageCompression compress.CompressionType) {

	topicName := fmt.Sprintf("test-topic-%s", uuid.New().String())
	agents, tearDown := startAgents(t, 1, false, false, fetchCompression, storageCompression)
	defer tearDown(t)

	createTopic(t, topicName, 100, agents[0].kafkaListenAddress, false, false)

	address := agents[0].kafkaListenAddress

	producer := createProducer(t, producerFactory, address, false, false, producerCompression)
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
	err := producer.Produce(topicName, msgs)
	require.NoError(t, err)

	consumer := createConsumer(t, consumerFactory, address, topicName, "test_group", false, false)
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
