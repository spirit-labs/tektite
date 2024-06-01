package integration

import (
	"errors"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"testing"
	"time"
)

type clientType int

const (
	clientTypeGo   = 1
	clientTypeJava = 2
)

func TestProducerEndpointGoClient(t *testing.T) {
	t.Parallel()
	testProducerEndpoint(t, clientTypeGo)
}

func TestProducerEndpointJavaClient(t *testing.T) {
	t.Parallel()
	testProducerEndpoint(t, clientTypeJava)
}

func testProducerEndpoint(t *testing.T, ct clientType) {

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startClusterWithConfigSetter(t, 3, nil, func(cfg *conf.Config) {
		cfg.KafkaServerEnabled = types.AddressOf(true)
		var kafkaListenAddresses []string
		for i := 0; i < 3; i++ {
			kafkaListenAddresses = append(kafkaListenAddresses, fmt.Sprintf("localhost:%d", testutils.PortProvider.GetPort(t)))
		}
		cfg.KafkaServerAddresses = kafkaListenAddresses
	})
	defer tearDown(t)
	client, err := tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	// We create three "topics" - one with one partition, one with num partitions < processor count and one
	// with num partitions > processor count

	err = client.ExecuteStatement(`topic1 :=  (kafka in partitions=1) -> (store stream)`)
	require.NoError(t, err)

	err = client.ExecuteStatement(`topic2 :=  (kafka in partitions=10) -> (store stream)`)
	require.NoError(t, err)
	err = client.ExecuteStatement(`topic3 :=  (kafka in partitions=200) -> (store stream)`)
	require.NoError(t, err)

	// And create a table with a raw partition key (no partition operator)
	err = client.ExecuteStatement(`table1 :=  (kafka in partitions=100) -> (store table by key)`)
	require.NoError(t, err)

	// And create a table with repartitioning
	err = client.ExecuteStatement(
		`table2 :=  (kafka in partitions=100) ->
				(project to_upper(to_string(key)) as skey, val) ->
				(partition by skey partitions=20) ->
                (store table by skey)`)
	require.NoError(t, err)

	startTime := time.Now().UTC()

	serverAddress := servers[0].GetConfig().KafkaServerAddresses[0]

	switch ct {
	case clientTypeGo:
		err = executeProducerClientActionsGoClient(serverAddress)
	case clientTypeJava:
		err = executeProducerClientActionsJavaClient(serverAddress)
	default:
		panic("unexpected client type")
	}
	if err != nil {
		log.Errorf("failed to execute client actions %v", err)
	}
	require.NoError(t, err)

	waitForRows(t, "topic1", 10, client, startTime)
	waitForRows(t, "topic2", 10, client, startTime)
	waitForRows(t, "topic3", 10, client, startTime)
	waitForNumRows(t, "table1", 10, client)
	waitForNumRows(t, "table2", 10, client)

	// Test some point lookups - this verifies that the producer uses the same partition function as we do on
	// the server

	// Test point lookups with raw partition key
	for i := 0; i < 10; i++ {
		qr, err := client.ExecuteQuery(fmt.Sprintf(`(get to_bytes("key%05d") from table1)`, i))
		require.NoError(t, err)
		require.Equal(t, 1, qr.RowCount())
		value := string(qr.Row(0).BytesVal(3))
		require.Equal(t, fmt.Sprintf("value%05d", i), value)
	}

	// Test point lookups with re-partitioned data
	for i := 0; i < 10; i++ {
		qr, err := client.ExecuteQuery(fmt.Sprintf(`(get "KEY%05d" from table2)`, i))
		require.NoError(t, err)
		require.Equal(t, 1, qr.RowCount())
		value := string(qr.Row(0).BytesVal(2))
		require.Equal(t, fmt.Sprintf("value%05d", i), value)
	}
}

func waitForRows(t *testing.T, tableName string, numMessages int, client tekclient.Client, startTime time.Time) {
	waitForRowsIgnoreDups(t, tableName, numMessages, client, startTime, false)
}

func waitForRowsIgnoreDups(t *testing.T, tableName string, numMessages int, client tekclient.Client, startTime time.Time, ignoreDups bool) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		log.Debug("************ waiting for rows")
		qr, err := client.ExecuteQuery(fmt.Sprintf("(scan all from %s) -> (sort by key)", tableName))
		if err != nil {
			return false, err
		}
		log.Debugf("got num rows: %d", qr.RowCount())
		var lastKey string
		numUniqueRows := 0
		for i := 0; i < qr.RowCount(); i++ {
			row := qr.Row(i)
			expectedKey := fmt.Sprintf("key%05d", numUniqueRows)
			expectedValue := fmt.Sprintf("value%05d", numUniqueRows)
			eventTime := row.TimestampVal(1)
			key := string(row.BytesVal(2))
			if ignoreDups && key == lastKey {
				continue
			}
			value := string(row.BytesVal(4))
			if expectedKey != key {
				log.Errorf("expected key: %s got key: %s", expectedKey, key)
				return false, nil
			}
			require.Equal(t, expectedValue, value)
			now := time.Now().UTC()
			require.GreaterOrEqual(t, eventTime.Val, startTime.UnixMilli())
			require.Less(t, eventTime.Val, now.UnixMilli()+1000)
			numUniqueRows++
			lastKey = key
		}
		log.Debugf("waited and got %d rows", qr.RowCount())
		return numUniqueRows == numMessages, nil
	}, 30*time.Second, 1*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
}

func waitForNumRows(t *testing.T, topicName string, numMessages int, client tekclient.Client) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		qr, err := client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", topicName))
		if err != nil {
			return false, err
		}
		return qr.RowCount() == numMessages, nil
	}, 10*time.Second, 1*time.Second)
	require.True(t, ok)
	require.NoError(t, err)
}

// Confluent client
// ================

func executeProducerClientActionsGoClient(serverAddress string) error {
	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": serverAddress,
		"acks":              "all"})
	if err != nil {
		return err
	}
	defer producer.Close()

	// First we send messages to topic1 - this has one partition, so offsets should come back in contiguous order
	msgs, err := sendMessagesGoClient(10, "topic1", producer)
	if err != nil {
		return err
	}
	partitionID := -1
	for i, msg := range msgs {
		offset := msg.TopicPartition.Offset
		if i != int(offset) {
			return errors.New("unexpected offset")
		}
		if partitionID != -1 {
			if partitionID != int(msg.TopicPartition.Partition) {
				return errors.New("too many partitions")
			}
		} else {
			partitionID = int(msg.TopicPartition.Partition)
		}
	}

	// And to topic2
	_, err = sendMessagesGoClient(10, "topic2", producer)
	if err != nil {
		return err
	}

	// And to topic3
	msgs, err = sendMessagesGoClient(10, "topic3", producer)
	if err != nil {
		return err
	}

	// And to table1
	_, err = sendMessagesGoClient(10, "table1", producer)
	if err != nil {
		return err
	}

	// And to table2
	_, err = sendMessagesGoClient(10, "table2", producer)
	if err != nil {
		return err
	}

	return nil
}

func sendMessagesGoClient(numMessages int, topicName string, producer *kafkago.Producer) ([]*kafkago.Message, error) {
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
		if err != nil {
			return nil, err
		}
		// Note, we send messages one by one and wait for receipt on each one. Yes, this is slow, but it means it creates
		// lots of batches on the server which exercises the replication and failover/replay logic better
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		if m.TopicPartition.Error != nil {
			return nil, m.TopicPartition.Error
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func executeProducerClientActionsJavaClient(serverAddress string) error {
	return executeJavaClientActions("producer_endpoint", serverAddress)
}

func executeJavaClientActions(testName string, address string) error {

	cmd := exec.Command("java", "-jar", "java/kafkatest/target/test-runner-1.0.0.jar", "-testname", testName,
		"-address", address)

	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return err
	}

	// Wait for the command to complete
	return cmd.Wait()
}
