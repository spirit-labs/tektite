// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package integration

import (
	kafkago "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestConsumerEndpointGoClient(t *testing.T) {
	t.Parallel()
	testConsumerEndpoint(t, clientTypeGo)
}

func TestConsumerEndpointJavaClient(t *testing.T) {
	t.Parallel()
	testConsumerEndpoint(t, clientTypeJava)
}

func testConsumerEndpoint(t *testing.T, ct clientType) {

	numMessages := 100

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
		cfg.KafkaServerAddresses = kafkaListenAddresses
	})
	defer tearDown(t)
	client, err := tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	err = client.ExecuteStatement(`topic1 :=  (kafka in partitions=1) -> (kafka out)`)
	require.NoError(t, err)

	err = client.ExecuteStatement(`topic2 :=  (kafka in partitions=10) -> (kafka out)`)
	require.NoError(t, err)

	startTime := time.Now().UTC()

	serverAddress := servers[0].GetConfig().KafkaServerAddresses[0]

	switch ct {
	case clientTypeGo:
		err = executeConsumerClientActionsGoClient(t, serverAddress, numMessages)
	case clientTypeJava:
		err = executeConsumerClientActionsJavaClient(serverAddress)
	default:
		panic("unexpected client type")
	}
	if err != nil {
		log.Errorf("failed to execute client actions %v", err)
	}
	require.NoError(t, err)

	waitForRows(t, "topic1", numMessages, client, startTime)
}

// Confluent client
// ================

func executeConsumerClientActionsGoClient(t *testing.T, serverAddress string, numMessages int) error {
	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": serverAddress,
		"acks":              "all"})
	if err != nil {
		return err
	}
	defer producer.Close()

	sendStart := time.UnixMilli(time.Now().UTC().UnixMilli()).UTC()

	_, err = sendMessagesGoClient(numMessages, "topic1", producer)
	if err != nil {
		return err
	}
	_, err = sendMessagesGoClient(numMessages, "topic2", producer)
	if err != nil {
		return err
	}
	sendEnd := time.UnixMilli(time.Now().UTC().UnixMilli()).UTC()

	cm := &kafkago.ConfigMap{
		"bootstrap.servers":  serverAddress,
		"group.id":           "test_group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"debug":              "all",
	}
	consumer1, err := kafkago.NewConsumer(cm)
	require.NoError(t, err)
	defer closeConsumer(t, consumer1)
	err = consumer1.Subscribe("topic1", nil)
	require.NoError(t, err)

	consumer2, err := kafkago.NewConsumer(cm)
	require.NoError(t, err)
	defer closeConsumer(t, consumer2)
	err = consumer2.Subscribe("topic2", nil)
	require.NoError(t, err)

	msgs1 := fetchMessages(t, consumer1, numMessages)
	// topic1 has one partition so messages should all be in offset order

	for i, msg := range msgs1 {
		require.Equal(t, 0, int(msg.TopicPartition.Partition))
		require.Equal(t, i, int(msg.TopicPartition.Offset))
		require.GreaterOrEqual(t, msg.Timestamp.UTC(), sendStart)
		require.LessOrEqual(t, msg.Timestamp.UTC(), sendEnd)
	}

	msgs2 := fetchMessages(t, consumer2, numMessages)
	for _, msg := range msgs2 {
		require.GreaterOrEqual(t, msg.Timestamp.UTC(), sendStart)
		require.LessOrEqual(t, msg.Timestamp.UTC(), sendEnd)
	}

	return nil
}

func closeConsumer(t *testing.T, consumer *kafkago.Consumer) {
	err := consumer.Close()
	require.NoError(t, err)
}

func fetchMessages(t *testing.T, consumer *kafkago.Consumer, numMessages int) []*kafkago.Message {
	var msgs []*kafkago.Message
	start := time.Now()
	for len(msgs) < numMessages {
		msg, err := consumer.ReadMessage(time.Second)
		if err != nil {
			if err.(kafkago.Error).Code() == kafkago.ErrTimedOut {
				require.True(t, time.Now().Sub(start) <= 1*time.Hour, "timed out waiting to consume messages")
				continue
			}
			require.NoError(t, err)
		}
		require.NotNil(t, msg)
		msgs = append(msgs, msg)
	}
	require.Equal(t, numMessages, len(msgs))
	return msgs
}

func executeConsumerClientActionsJavaClient(address string) error {
	return executeJavaClientActions("consumer_endpoint", address)
}
