package agent

import (
	"context"
	"errors"
	"github.com/google/uuid"
	segment "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	saslplain "github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
	clientKeyPath  = "testdata/selfsignedclientkey.pem"
	clientCertPath = "testdata/selfsignedclientcert.pem"
)

func TestKafkaAuthMtls(t *testing.T) {
	cfg := NewConf()
	cfg.KafkaListenerConfig.TLSConfig = conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
		ClientCertFile:       clientCertPath,
		ClientAuthType:       "require-and-verify-client-cert",
	}
	cfg.AuthType = kafkaserver2.AuthenticationTypeMTls
	agents, tearDown := setupAgents(t, cfg, 1, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	agent := agents[0]

	clientTLSConfig := conf.ClientTlsConf{
		Enabled:              true,
		ServerCertFile:       serverCertPath,
		ClientPrivateKeyFile: clientKeyPath,
		ClientCertFile:       clientCertPath,
	}
	tlsc, err := clientTLSConfig.ToGoTlsConf()
	require.NoError(t, err)
	dialer := &segment.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsc,
	}
	address := agent.cfg.KafkaListenerConfig.Address
	topicName := makeTopic(t, agent)
	conn, err := dialer.DialLeader(context.Background(), "tcp", address, topicName, 0)
	require.NoError(t, err)

	readAndWriteMessage(t, conn)

	verifyConnection(t, agent, true, "O=acme aardvarks ltd.,L=San Francisco\\, street=Golden Gate Bridge\\, postalCode=94016,C=US", conn.LocalAddr().String())
}

func TestKafkaAuthSaslPlain(t *testing.T) {

	cfg := NewConf()
	cfg.AuthType = kafkaserver2.AuthenticationTypeSaslPlain
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

	clientTLSConfig := conf.ClientTlsConf{
		Enabled:        true,
		ServerCertFile: serverCertPath,
	}

	username1 := "some-user1"
	password1 := "some-password1"

	username2 := "some-user2"
	password2 := "some-password2"

	scramType := auth.AuthenticationSaslScramSha512

	putUserCred(t, agent, username1, password1, scramType)
	putUserCred(t, agent, username2, password2, scramType)

	mechProvider := func(t *testing.T, username string, password string) sasl.Mechanism {
		return saslplain.Mechanism{
			Username: username,
			Password: password,
		}
	}

	// Test success
	tryConnect(t, username1, password1, true, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username2, password2, true, agent, clientTLSConfig, mechProvider)

	// Test failure
	tryConnect(t, username1, "wrongpwd", false, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username1, password2, false, agent, clientTLSConfig, mechProvider)

	tryConnect(t, username2, "wrongpwd", false, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username2, password1, false, agent, clientTLSConfig, mechProvider)

	tryConnect(t, "wronguser", "wrongpwd", false, agent, clientTLSConfig, mechProvider)

	// Now create a non SCRAM connection against the server
	// must fail as it will send a metadata request before any SASL handshake
	tlsc, err := clientTLSConfig.ToGoTlsConf()
	require.NoError(t, err)
	dialer := &segment.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsc,
	}
	topicName := makeTopic(t, agent)
	address := agent.cfg.KafkaListenerConfig.Address
	_, err = dialer.DialLeader(context.Background(), "tcp", address, topicName, 0)
	require.Error(t, err)
}

func TestKafkaAuthSaslScram(t *testing.T) {

	cfg := NewConf()
	cfg.AuthType = kafkaserver2.AuthenticationTypeSaslScram512
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

	clientTLSConfig := conf.ClientTlsConf{
		Enabled:        true,
		ServerCertFile: serverCertPath,
	}

	username1 := "some-user1"
	password1 := "some-password1"

	username2 := "some-user2"
	password2 := "some-password2"

	scramType := auth.AuthenticationSaslScramSha512

	putUserCred(t, agent, username1, password1, scramType)
	putUserCred(t, agent, username2, password2, scramType)

	mechProvider := func(t *testing.T, username string, password string) sasl.Mechanism {
		mechanism, err := scram.Mechanism(scram.SHA512, username, password)
		require.NoError(t, err)
		return mechanism
	}

	// Test success
	tryConnect(t, username1, password1, true, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username2, password2, true, agent, clientTLSConfig, mechProvider)

	// Test failure
	tryConnect(t, username1, "wrongpwd", false, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username1, password2, false, agent, clientTLSConfig, mechProvider)

	tryConnect(t, username2, "wrongpwd", false, agent, clientTLSConfig, mechProvider)
	tryConnect(t, username2, password1, false, agent, clientTLSConfig, mechProvider)

	tryConnect(t, "wronguser", "wrongpwd", false, agent, clientTLSConfig, mechProvider)

	// Now create a non SCRAM connection against the server
	// must fail as it will send a metadata request before any SASL handshake
	tlsc, err := clientTLSConfig.ToGoTlsConf()
	require.NoError(t, err)
	dialer := &segment.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsc,
	}
	topicName := makeTopic(t, agent)
	address := agent.cfg.KafkaListenerConfig.Address
	_, err = dialer.DialLeader(context.Background(), "tcp", address, topicName, 0)
	require.Error(t, err)
}

type saslMechanismProvider func(t *testing.T, username string, password string) sasl.Mechanism

func tryConnect(t *testing.T, username string, password string, shouldSucceeed bool, agent *Agent,
	clientTls conf.ClientTlsConf, saslMechProvider saslMechanismProvider) {
	// We use the segmentio Kafka client as it returns errors on authentication failure unlike librdkafka which
	// retries in a loop
	topic := makeTopic(t, agent)
	conn := tryCreateConnection(t, agent, clientTls, username, password, topic, shouldSucceeed, saslMechProvider)
	if shouldSucceeed {
		readAndWriteMessage(t, conn)
		verifyConnection(t, agent, true, username, conn.LocalAddr().String())
		err := conn.Close()
		require.NoError(t, err)
	}
}

func tryCreateConnection(t *testing.T, agent *Agent, clientTls conf.ClientTlsConf, username string, password string,
	topicName string, shouldSucceed bool, saslMechProvider saslMechanismProvider) *segment.Conn {
	tlsc, err := clientTls.ToGoTlsConf()
	require.NoError(t, err)
	mechanism := saslMechProvider(t, username, password)
	dialer := &segment.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsc,
		SASLMechanism: mechanism,
	}
	address := agent.cfg.KafkaListenerConfig.Address
	conn, err := dialer.DialLeader(context.Background(), "tcp", address, topicName, 0)
	if shouldSucceed {
		require.NoError(t, err)
	} else {
		var kerr segment.Error
		require.True(t, errors.As(err, &kerr))
		errMsg := kerr.Error()
		require.True(t, strings.HasPrefix(errMsg, "[58] SASL Authentication Failed"))
	}
	return conn
}

func verifyConnection(t *testing.T, agent *Agent, authenticated bool, principal string, clientAddress string) {
	infos := agent.kafkaServer.ConnectionInfos()
	require.Equal(t, 1, len(infos))
	info := infos[0]
	require.Equal(t, authenticated, info.Authenticated)
	require.Equal(t, principal, info.Principal)
	require.Equal(t, clientAddress, info.ClientAddress)
}

func readAndWriteMessage(t *testing.T, conn *segment.Conn) {
	val := []byte("value00000")
	_, err := conn.WriteMessages(segment.Message{
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

func makeTopic(t *testing.T, agent *Agent) string {
	topicName := "topic-" + uuid.New().String()
	cl, err := agent.Controller().Client()
	require.NoError(t, err)
	err = cl.CreateTopic(topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	})
	require.NoError(t, err)
	return topicName
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
