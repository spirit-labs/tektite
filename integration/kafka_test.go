//go:build integration

package integration

import (
	"context"
	"fmt"
	segment "github.com/segmentio/kafka-go"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestKafkaTopicNotFound(t *testing.T) {
	clientTLSConfig := client.TLSConfig{
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
	})
	defer tearDown(t)
	cl, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()
	// Execute a send on a different GR before topic is created - the client will retry, waiting for it to be created
	// before sending. this exercises logic in the server for returning metadata with error for non existent topic
	serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]
	var wg sync.WaitGroup
	wg.Add(1)
	startTime := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := segment.DialLeader(ctx, "tcp", serverAddress, "topic1", 0)
		if err != nil {
			panic(err)
		}
		//goland:noinspection ALL
		defer cancel()
		msg := segment.Message{
			Key:   []byte("key00000"),
			Value: []byte("value00000"),
			Time:  time.Now(),
		}
		_, err = conn.WriteMessages(msg)
		if err != nil {
			panic(err)
		}
		err = conn.Close()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	err = cl.ExecuteStatement(`topic1 :=  (kafka in partitions=10) -> (store stream)`)
	require.NoError(t, err)
	wg.Wait()
	waitForRows(t, "topic1", 1, cl, startTime)
}

func TestKafkaProduceMultipleTopics(t *testing.T) {
	clientTLSConfig := client.TLSConfig{
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
	})
	defer tearDown(t)
	cl, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()

	err = cl.ExecuteStatement(`topic1 :=  (kafka in partitions=10) -> (store stream)`)
	require.NoError(t, err)
	err = cl.ExecuteStatement(`topic2 :=  (kafka in partitions=10) -> (store stream)`)
	require.NoError(t, err)
	err = cl.ExecuteStatement(`topic3 :=  (kafka in partitions=10) -> (store stream)`)
	require.NoError(t, err)

	serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]
	startTime := time.Now()

	w := &segment.Writer{
		Addr: segment.TCP(serverAddress),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		Balancer: &segment.LeastBytes{},
	}
	numMsgs := 10
	// Produce to multiple topics in same batch
	var msgs []segment.Message
	msgs = append(msgs, createSegmentMessages("topic1", numMsgs)...)
	msgs = append(msgs, createSegmentMessages("topic2", numMsgs)...)
	msgs = append(msgs, createSegmentMessages("topic3", numMsgs)...)

	err = w.WriteMessages(context.Background(), msgs...)
	require.NoError(t, err)

	waitForRows(t, "topic1", numMsgs, cl, startTime)
	waitForRows(t, "topic2", numMsgs, cl, startTime)
	waitForRows(t, "topic3", numMsgs, cl, startTime)
}

func createSegmentMessages(topic string, num int) []segment.Message {
	var msgs []segment.Message
	for i := 0; i < num; i++ {
		msgs = append(msgs, segment.Message{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key%05d", i)),
			Value: []byte(fmt.Sprintf("value%05d", i)),
		})
	}
	return msgs
}
