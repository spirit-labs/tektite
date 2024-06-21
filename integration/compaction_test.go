//go:build integration

package integration

import (
	crand "crypto/rand"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

func TestCompaction(t *testing.T) {
	t.Parallel()

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startClusterWithConfigSetter(t, 3, nil, func(cfg *conf.Config) {
		cfg.KafkaServerEnabled = true
		var kafkaListenAddresses []string
		for i := 0; i < 3; i++ {
			address, err := common.AddressWithPort("localhost")
			require.NoError(t, err)
			kafkaListenAddresses = append(kafkaListenAddresses, address)
		}
		cfg.KafkaServerListenerConfig.Addresses = kafkaListenAddresses
		cfg.MemtableMaxSizeBytes = 100 * 1024
		cfg.MemtableMaxReplaceInterval = 5 * time.Second
		cfg.CompactionWorkersEnabled = true
		cfg.SSTableDeleteCheckInterval = 250 * time.Millisecond
		cfg.SSTableDeleteDelay = 1 * time.Second
		cfg.CompactionMaxSSTableSize = 100 * 1024
		cfg.CompactionPollerTimeout = 1 * time.Second
		cfg.SSTableRegisterRetryDelay = 500 * time.Millisecond
	})
	defer tearDown(t)
	client, err := tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	// Create a table with a raw partition key (no partition operator)
	err = client.ExecuteStatement(`table1 := (kafka in partitions=32) -> (store table by key)`)
	require.NoError(t, err)

	serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": serverAddress,
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	topicName := "table1"

	numMessages := 120000
	msgBatchSize := 10000
	numBatches := numMessages / msgBatchSize
	numKeys := 100000
	valuePrefixSize := 100
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	valuePrefix := make([]byte, valuePrefixSize)
	_, err = crand.Read(valuePrefix)
	require.NoError(t, err)
	sValuePrefix := string(valuePrefix)

	kvMap := make(map[int]int, numKeys)

	msgCount := 0
	for i := 0; i < numBatches; i++ {
		deliveryChan := make(chan kafkago.Event, msgBatchSize)
		for j := 0; j < msgBatchSize; j++ {
			k := random.Intn(numKeys)
			v := random.Intn(numKeys)
			key := []byte(fmt.Sprintf("foo---key%07d-----bar", k))
			value := []byte(fmt.Sprintf("%s-val%07d", sValuePrefix, v))
			kvMap[k] = v
			err := producer.Produce(&kafkago.Message{
				TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
				Key:            key,
				Value:          value},
				deliveryChan,
			)
			require.NoError(t, err)
		}
		for j := 0; j < msgBatchSize; j++ {
			e := <-deliveryChan
			m := e.(*kafkago.Message)
			if m.TopicPartition.Error != nil {
				require.NoError(t, m.TopicPartition.Error)
			}
			msgCount++
		}
	}

	// Wait until all local data has been flushed
	for _, server := range servers {
		st := server.GetStore()
		lcv := st.GetLastCompletedVersion()
		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			return st.GetFlushedVersion() >= lcv, nil
		}, 45*time.Second, 100*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Now we wait for all in-progress jobs to complete
	for _, server := range servers {
		lm := server.GetLevelManager()
		if lm != nil {
			ok, err := testutils.WaitUntilWithError(func() (bool, error) {
				stats := lm.GetCompactionStats()
				return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, err)
			require.True(t, ok)
		}
	}
}
