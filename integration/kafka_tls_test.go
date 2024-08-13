//go:build integration

package integration

import (
	kafkago "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/client"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKafkaTLS(t *testing.T) {
	serverTlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	testKafkaTLS(t, serverTlsConf, "", "", "")
}

func TestKafkaTLSWithClientCert(t *testing.T) {
	serverTlsConf := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         serverKeyPath,
		CertPath:        serverCertPath,
		ClientCertsPath: clientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	testKafkaTLS(t, serverTlsConf, clientKeyPath, clientCertPath, "tls")
}

func testKafkaTLS(t *testing.T, serverTlsConf conf.TLSConfig, clientKeyPath string, clientCertPath string, authType string) {
	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	clusterName := uuid.New().String()
	server, objStore := startStandaloneServerWithObjStoreAndConfigSetter(t, clusterName, func(config *conf.Config) {
		config.KafkaServerListenerConfig.AuthenticationType = authType
		config.KafkaServerListenerConfig.TLSConfig = serverTlsConf
	})
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}()

	cl, err := client.NewClient(server.GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()
	// Execute a send on a different GR before topic is created - the client will retry, waiting for it to be created
	// before sending. this exercises logic in the server for returning metadata with error for non existent topic
	serverAddress := server.GetConfig().KafkaServerListenerConfig.Addresses[0]

	err = cl.ExecuteStatement(`topic1 :=  (kafka in partitions=1) -> (store stream)`)
	require.NoError(t, err)

	startTime := time.Now()

	cfg := kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": serverAddress,
		"acks":              "all",
		"security.protocol": "ssl",
		"ssl.ca.location":   serverCertPath,
		"debug":             "all",
	}

	if clientCertPath != "" {
		cfg["ssl.certificate.location"] = clientCertPath
	}
	if clientKeyPath != "" {
		cfg["ssl.key.location"] = clientKeyPath
	}

	producer, err := kafkago.NewProducer(&cfg)

	require.NoError(t, err)
	defer producer.Close()

	_, err = sendMessagesGoClient(1, "topic1", producer)
	require.NoError(t, err)

	waitForRows(t, "topic1", 1, cl, startTime)

	if clientCertPath != "" {
		// Verify client is authenticated
		conns := server.GetKafkaServer().Connections()
		require.Equal(t, 1, len(conns))
		require.True(t, conns[0].AuthContext().Authorised)
		require.Equal(t, "O=acme aardvarks ltd.,L=San Francisco\\, street=Golden Gate Bridge\\, postalCode=94016,C=US", *conns[0].AuthContext().Principal)
	}
}
