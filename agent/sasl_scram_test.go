package agent

import (
	"context"
	"errors"
	"github.com/google/uuid"
	segment "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
)

func TestKafkaAuthSaslScram(t *testing.T) {

	authType := auth.AuthenticationSaslScramSha512

	cfg := NewConf()
	cfg.KafkaListenerConfig.TLSConfig = conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}
	agents, tearDown := setupAgents(t, cfg, 1, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	agent := agents[0]

	username1 := "some-user1"
	password1 := "some-password1"

	username2 := "some-user2"
	password2 := "some-password2"

	putUserCred(t, agent, username1, password1, authType)
	putUserCred(t, agent, username2, password2, authType)

	// Test success
	tryConnect(t, username1, password1, true, authType, agent)
	tryConnect(t, username2, password2, true, authType, agent)

	// Test failure
	tryConnect(t, username1, "wrongpwd", false, authType, agent)
	tryConnect(t, username1, password2, false, authType, agent)

	tryConnect(t, username2, "wrongpwd", false, authType, agent)
	tryConnect(t, username2, password1, false, authType, agent)

	tryConnect(t, "wronguser", "wrongpwd", false, authType, agent)
}

func tryConnect(t *testing.T, username string, password string, shouldSucceeed bool, authType string, agent *Agent) {
	// We use the segmentio Kafka client as it returns errors on authentication failure unlike librdkafka which
	// retries in a loop

	topicName := "topic-" + uuid.New().String()

	cl, err := agent.Controller().Client()
	require.NoError(t, err)
	err = cl.CreateTopic(topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	})
	require.NoError(t, err)

	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
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
	address := agent.cfg.KafkaListenerConfig.Address
	conn, err := dialer.DialLeader(context.Background(), "tcp", address, topicName, 0)
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

		val := []byte("value00000")
		_, err = conn.WriteMessages(segment.Message{
			Key:   []byte("key00000"),
			Value: val,
			Time:  time.Now(),
		})
		require.NoError(t, err)

		err = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		require.NoError(t, err)

		batch := conn.ReadBatch(1, 10e6)
		defer func() {
			err = batch.Close()
			require.NoError(t, err)
		}()
		buffer := make([]byte, 10e3)
		n, err := batch.Read(buffer)
		require.NoError(t, err)
		buffer = buffer[:n]
		require.Equal(t, val, buffer)
	}
}

func putUserCred(t *testing.T, agent *Agent, username string, password string, authType string) {
	storedKey, serverKey, salt := auth.CreateUserScramCreds(password, authType)
	cl, err := agent.controller.Client()
	require.NoError(t, err)
	err = cl.PutUserCredentials(username, storedKey, serverKey, salt, 4096)
	require.NoError(t, err)
	err = cl.Close()
	require.NoError(t, err)
}
