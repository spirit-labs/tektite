package cl_test

import (
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spirit-labs/tektite/agent"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

/*
Tests that messages are produced to agents in the same AZ when AZ is provided on client id
*/

func TestProduceWithSingleAz(t *testing.T) {
	testProduceWithAz(t, "az-1", 6, func(i int) string {
		return "az-1"
	})
}

func TestProduceWithMultipleAzs(t *testing.T) {
	testProduceWithAz(t, "az-1", 6, func(i int) string {
		return fmt.Sprintf("az-%d", i%3)
	})
}

func testProduceWithAz(t *testing.T, clientAZ string, numAgents int, azSetter func(int) string) {
	agents, tearDown := startAgents(t, numAgents, azSetter)
	defer tearDown(t)

	address := agents[0].Conf().KafkaListenerConfig.Address
	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":        "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers":  address,
		"acks":               "all",
		"enable.idempotence": strconv.FormatBool(true),
		"client.id":          fmt.Sprintf("tek_az=%s", clientAZ),
	})
	require.NoError(t, err)
	defer producer.Close()

	topicName := "test-topic1"
	createTopicWithRetry(t, topicName, 10, agents[0])

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

	totBatches := 0
	for _, ag := range agents {
		batchCount := ag.TablePusher().GetStats().ProducedBatchCount
		agAz := ag.Conf().FetchCacheConf.AzInfo
		if agAz != clientAZ {
			require.Equal(t, 0, int(batchCount))
		} else {
			totBatches += int(batchCount)
		}
	}
	// Likely to be more but possible they could be combined into a single batch due to timing
	require.GreaterOrEqual(t, totBatches, 1)
}

func startAgents(t *testing.T, numAgents int, azPicker func(int) string) ([]*agent.Agent, func(t *testing.T)) {
	objStore := createObjStore()
	var agents []*agent.Agent
	for i := 0; i < numAgents; i++ {
		cfg := agent.NewConf()
		cfg.ClusterMembershipConfig.UpdateInterval = 10 * time.Millisecond // for faster start
		cfg.PusherConf.WriteTimeout = 10 * time.Millisecond                // for fast flush and produce
		cfg.PusherConf.EnforceProduceOnLeader = true
		az := azPicker(i)
		cfg.FetchCacheConf.AzInfo = az
		cfg.ControllerConf.AzInfo = az
		kafkaAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		clusterAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		cfg.KafkaListenerConfig.Address = kafkaAddress
		cfg.ClusterListenerConfig.Address = clusterAddress
		ag, err := agent.NewAgent(cfg, objStore)
		require.NoError(t, err)
		err = ag.Start()
		require.NoError(t, err)
		agents = append(agents, ag)
	}
	waitForMembers(t, numAgents, agents...)
	return agents, func(t *testing.T) {
		for _, ag := range agents {
			err := ag.Stop()
			require.NoError(t, err)
		}
	}
}

func createObjStore() objstore.Client {
	return dev.NewInMemStore(0)
}

func createTopicWithRetry(t *testing.T, topicName string, partitions int, agent *agent.Agent) {
	for {
		if err := createTopic(t, topicName, partitions, agent); err != nil {
			if common.IsUnavailableError(err) {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			require.NoError(t, err)
		} else {
			return
		}
	}
}

func createTopic(t *testing.T, topicName string, partitions int, agent *agent.Agent) error {
	controller := agent.Controller()
	cl, err := controller.Client()
	if err != nil {
		return err
	}
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()
	return cl.CreateTopic(topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: partitions,
	})
}

func createTopicUsingAdminClient(t *testing.T, topicName string, partitions int, producer *kafkago.Producer) {
	admin, err := kafkago.NewAdminClientFromProducer(producer)
	require.NoError(t, err)
	results, err := admin.CreateTopics(
		context.Background(),
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafkago.TopicSpecification{{
			Topic:             topicName,
			NumPartitions:     partitions,
			ReplicationFactor: 3}},
	)
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}
	for _, res := range results {
		require.NoError(t, res.Error)
	}
	admin.Close()
}

func waitForMembers(t *testing.T, numMembers int, agents ...*agent.Agent) {
	for _, agent := range agents {
		testutils.WaitUntil(t, func() (bool, error) {
			return len(agent.Controller().GetClusterState().Members) == numMembers, nil
		})
	}
}
