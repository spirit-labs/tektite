//go:build integration

package integration

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	segment "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/client"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestKafkaAuthScram256(t *testing.T) {
	testKafkaAuthScram(t, auth.AuthenticationSaslScramSha256)
}

func testKafkaAuthScram(t *testing.T, authType string) {
	serverTlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
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

	username1 := "some-user1"
	password1 := "some-password1"

	username2 := "some-user2"
	password2 := "some-password2"

	scramManager := server.GetScramManager()
	putUserCred(t, scramManager, username1, password1, authType)
	putUserCred(t, scramManager, username2, password2, authType)

	// Test success
	tryConnect(t, username1, password1, true, server.GetConfig(), authType)
	tryConnect(t, username2, password2, true, server.GetConfig(), authType)

	// Test failure
	tryConnect(t, username1, "wrongpwd", false, server.GetConfig(), authType)
	tryConnect(t, username1, password2, false, server.GetConfig(), authType)

	tryConnect(t, username2, "wrongpwd", false, server.GetConfig(), authType)
	tryConnect(t, username2, password1, false, server.GetConfig(), authType)

	tryConnect(t, "wronguser", "wrongpwd", false, server.GetConfig(), authType)
}

func tryConnect(t *testing.T, username string, password string, shouldSucceeed bool, cfg conf.Config, authType string) {
	// We use the segmentio Kafka client as it returns errors on authentication failure unlike librdkafka which
	// retries in a loop

	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	cl, err := client.NewClient(cfg.HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()

	topicName := "topic-" + uuid.New().String()
	err = cl.ExecuteStatement(fmt.Sprintf(`%s := (kafka in partitions=1) -> (store stream)`, topicName))
	require.NoError(t, err)

	tlsc, err := clientTLSConfig.ToGoTlsConfig()
	require.NoError(t, err)
	var algo scram.Algorithm
	switch authType {
	case auth.AuthenticationSaslScramSha256:
		algo = scram.SHA256
	case auth.AuthenticationSaslScramSha512:
		algo = scram.SHA512
	default:
		panic("unknown auth type")
	}
	mechanism, err := scram.Mechanism(algo, username, password)
	require.NoError(t, err)
	dialer := &segment.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsc,
		SASLMechanism: mechanism,
	}
	conn, err := dialer.DialLeader(context.Background(), "tcp", cfg.KafkaServerListenerConfig.Addresses[0], topicName, 0)
	if shouldSucceeed {
		require.NoError(t, err)
	} else {
		var kerr segment.Error
		require.True(t, errors.As(err, &kerr))
		errMsg := kerr.Error()
		require.True(t, strings.HasPrefix(errMsg, "[58] SASL Authentication Failed"))
	}

	if err == nil {
		defer func() {
			err := conn.Close()
			require.NoError(t, err)
		}()

		startTime := time.Now()

		_, err = conn.WriteMessages(segment.Message{
			Key:   []byte("key00000"),
			Value: []byte("value00000"),
			Time:  time.Now(),
		})
		require.NoError(t, err)

		waitForRows(t, topicName, 1, cl, startTime)
	}
}
