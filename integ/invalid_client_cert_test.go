package integ

import (
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestInvalidClientCert(t *testing.T) {
	minioCfg, minioContainer := startMinioAndCreateBuckets(t)
	defer func() {
		err := minioContainer.Terminate(context.Background())
		require.NoError(t, err)
	}()

	topicName := "test-topic"
	commandLine := fmt.Sprintf("--obj-store-username=minioadmin --obj-store-password=minioadmin --obj-store-url=%s ", minioCfg.Endpoint) +
		"--cluster-name=test-cluster --location=az1 --kafka-listen-address=localhost:0 --internal-listen-address=localhost:0 " +
		"--membership-update-interval-ms=100 --membership-eviction-interval-ms=2000 " +
		"--consumer-group-initial-join-delay-ms=500 " +
		`--log-level=info ` +
		fmt.Sprintf("--topic-name=%s", topicName) +
		fmt.Sprintf(" --kafka-tls-enabled=true --kafka-tls-server-cert-file=%s --kafka-tls-server-private-key-file=%s",
			serverCertPath, serverKeyPath) +
		fmt.Sprintf(" --kafka-tls-client-cert-file=%s --kafka-tls-client-auth-type=require-and-verify-client-cert",
			clientCertPath)

	mgr := NewManager()
	agent, err := mgr.StartAgent(commandLine, false)
	require.NoError(t, err)

	err = agent.Start()
	require.NoError(t, err)
	defer func() {
		err := agent.Stop()
		require.NoError(t, err)
	}()

	// We, deliberately use the server cert and key for the client cert and key so it does not get authenticated
	producer, err := NewKafkaGoProducer(agent.kafkaListenAddress, true, serverCertPath, serverCertPath, serverKeyPath)
	require.NoError(t, err)
	defer func() {
		err := producer.Close()
		require.NoError(t, err)
	}()

	// With kafkago the tls error is, confusingly, provided on the events channel, not as a result of the producer creation
	kProducer := producer.(*KafkaGoProducer)
	foundErr := false
	for e := range kProducer.producer.Events() {
		err, ok := e.(kafkago.Error)
		require.True(t, ok)
		require.True(t, strings.Contains(err.Error(), "unknown ca"))
		foundErr = true
		break
	}
	require.True(t, foundErr)

}
