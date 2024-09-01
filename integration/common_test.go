package integration

import (
	"context"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	docker "github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/server"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafka/fake"
	"github.com/spirit-labs/tektite/lock"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"os"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"

	clientKeyPath  = "testdata/selfsignedclientkey.pem"
	clientCertPath = "testdata/selfsignedclientcert.pem"
)

var etcdAddress string

func TestMain(m *testing.M) {
	etcd, err := testutils.CreateEtcdContainer()
	if err != nil {
		panic(err)
	}
	etcdAddress = etcd.Address()
	defer etcd.Stop()
	m.Run()
}

func startStandaloneServerWithObjStore(t *testing.T, clusterName string) (*server.Server, *dev.Store) {
	return startStandaloneServerWithObjStoreAndConfigSetter(t, clusterName, nil)
}

func startStandaloneServerWithObjStoreAndConfigSetter(t *testing.T, clusterName string, configSetter func(config *conf.Config)) (*server.Server, *dev.Store) {
	objStoreAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	objStore := dev.NewDevStore(objStoreAddress)
	err = objStore.Start()
	require.NoError(t, err)
	s := startStandaloneServerWithConfigSetter(t, objStoreAddress, clusterName, configSetter)
	return s, objStore
}

func startStandaloneServer(t *testing.T, objStoreAddress string, clusterName string) *server.Server {
	return startStandaloneServerWithConfigSetter(t, objStoreAddress, clusterName, nil)
}

func startStandaloneServerWithConfigSetter(t *testing.T, objStoreAddress string, clusterName string, configSetter func(config *conf.Config)) *server.Server {
	cfg := conf.Config{}
	cfg.LogScope = t.Name()
	cfg.ApplyDefaults()
	cfg.ClusterName = clusterName
	cfg.ProcessorCount = 16
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	cfg.CompactionWorkersEnabled = true
	remotingAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.ClusterAddresses = []string{remotingAddress}
	cfg.HttpApiEnabled = true
	apiAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	cfg.HttpApiAddresses = []string{apiAddress}
	cfg.HttpApiEnabled = true
	cfg.HttpApiTlsConfig = tlsConf
	cfg.KafkaServerEnabled = true
	kafkaAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.KafkaServerListenerConfig.Addresses = []string{kafkaAddress}
	cfg.ClientType = conf.KafkaClientTypeConfluent
	cfg.ObjectStoreType = conf.DevObjectStoreType
	cfg.DevObjectStoreAddresses = []string{objStoreAddress}
	cfg.ClusterManagerKeyPrefix = uuid.NewString() // test needs unique etcd namespace
	if configSetter != nil {
		configSetter(&cfg)
	}
	s, err := server.NewServer(cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	return s
}

func startClusterFast(t *testing.T, numServers int, objStoreClient objstore.Client, cfgSetter func(cfg *conf.Config)) ([]*server.Server, func(t *testing.T)) {
	// use constant cluster manager (no etcd)
	// use trace tool to make it go fast - there should be no reason why this can't be super fast
	common.RequireDebugServer(t)

	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}

	var servers []*server.Server
	var remotingAddresses []string
	var httpServerListenAddresses []string
	for i := 0; i < numServers; i++ {
		remotingAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		remotingAddresses = append(remotingAddresses, remotingAddress)
		apiAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		httpServerListenAddresses = append(httpServerListenAddresses, apiAddress)
	}

	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.LogScope = t.Name()

	cfg.ClusterAddresses = remotingAddresses
	cfg.ClusterName = uuid.NewString() // must have unique namespace in etcd
	cfg.HttpApiEnabled = true
	cfg.HttpApiAddresses = httpServerListenAddresses
	cfg.HttpApiTlsConfig = tlsConf
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	cfg.MinSnapshotInterval = 100 * time.Millisecond
	cfg.CompactionWorkersEnabled = true
	cfg.ClusterManagerAddresses = []string{etcdAddress}
	cfg.VersionManagerLevelManagerRetryDelay = 5 * time.Millisecond
	cfg.LevelManagerRetryDelay = 5 * time.Millisecond
	cfg.CompactionPollerTimeout = 500 * time.Millisecond

	if cfgSetter != nil {
		cfgSetter(&cfg)
	}

	clustStateMgr := clustmgr.NewFixedStateManager(cfg.ProcessorCount+1, numServers)
	lockMgr := lock.NewInMemLockManager()

	fk := &fake.Kafka{}
	for i := 0; i < numServers; i++ {
		cfgCopy := cfg
		cfgCopy.NodeID = i
		s, err := server.NewServerWithArgs(cfgCopy, fake.NewFakeMessageClientFactory(fk), objStoreClient, clustStateMgr, lockMgr)
		require.NoError(t, err)
		servers = append(servers, s)
	}

	startServers(t, servers)

	return servers, func(t *testing.T) {
		cfg := servers[0].GetConfig()
		err := shutdown.PerformShutdown(&cfg, true)
		require.NoError(t, err)
	}
}

func startCluster(t *testing.T, numServers int, fk *fake.Kafka) ([]*server.Server, func(t *testing.T)) {
	return startClusterWithConfigSetter(t, numServers, fk, nil)
}

func startClusterWithConfigSetter(t *testing.T, numServers int, fk *fake.Kafka, configSetter func(cfg *conf.Config)) ([]*server.Server, func(t *testing.T)) {
	common.RequireDebugServer(t)
	objStoreAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	objStore := dev.NewDevStore(objStoreAddress)
	err = objStore.Start()
	require.NoError(t, err)
	servers, tearDown := startClusterWithObjStore(t, objStoreAddress, numServers, fk, configSetter)
	return servers, func(t *testing.T) {
		tearDown(t)
		err := objStore.Stop()
		require.NoError(t, err)
	}
}

func startClusterWithObjStore(t *testing.T, objStoreAddress string, numServers int, fk *fake.Kafka, configSetter func(cfg *conf.Config)) ([]*server.Server, func(t *testing.T)) {
	common.RequireDebugServer(t)

	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}

	var servers []*server.Server
	var remotingAddresses []string
	var httpServerListenAddresses []string
	for i := 0; i < numServers; i++ {
		remotingAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		remotingAddresses = append(remotingAddresses, remotingAddress)
		apiAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		httpServerListenAddresses = append(httpServerListenAddresses, apiAddress)
	}

	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.LogScope = t.Name()

	cfg.ClusterAddresses = remotingAddresses
	cfg.ClusterName = uuid.NewString() // must have unique namespace in etcd
	cfg.HttpApiEnabled = true
	cfg.HttpApiAddresses = httpServerListenAddresses
	cfg.HttpApiTlsConfig = tlsConf
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	cfg.MinSnapshotInterval = 100 * time.Millisecond
	cfg.CompactionWorkersEnabled = true
	cfg.ClusterManagerAddresses = []string{etcdAddress}

	// In real life don't want to set this so low otherwise cluster state will be calculated when just one node
	// is started with all leaders
	cfg.ClusterStateUpdateInterval = 10 * time.Millisecond

	// Set this low so store retries quickly to get prefix retentions on startup.
	cfg.LevelManagerRetryDelay = 10 * time.Millisecond

	cfg.DevObjectStoreAddresses = []string{objStoreAddress}
	cfg.ObjectStoreType = conf.DevObjectStoreType

	// Give each test a different etcd prefix, so they have separate namespaces
	cfg.ClusterManagerKeyPrefix = fmt.Sprintf("tektite-integration-tests-%s", t.Name())
	if configSetter != nil {
		configSetter(&cfg)
	}

	for i := 0; i < numServers; i++ {
		cfgCopy := cfg
		cfgCopy.NodeID = i
		s, err := server.NewServerWithClientFactory(cfgCopy, fake.NewFakeMessageClientFactory(fk))
		require.NoError(t, err)
		servers = append(servers, s)
	}

	startServers(t, servers)

	return servers, func(t *testing.T) {
		cfg := servers[0].GetConfig()
		err := shutdown.PerformShutdown(&cfg, true)
		require.NoError(t, err)
	}
}

func startServers(t *testing.T, servers []*server.Server) {
	// Start them in parallel
	var chans []chan error
	log.Debugf("%s: starting %d servers", t.Name(), len(servers))
	for _, s := range servers {
		ch := make(chan error, 1)
		chans = append(chans, ch)
		theServer := s
		go func() {
			log.Debugf("%s: starting server %d", t.Name(), theServer.GetConfig().NodeID)
			err := theServer.Start()
			if err != nil {
				log.Errorf("%s: starting server %d returned err %v", t.Name(), theServer.GetConfig().NodeID, err)
			} else {
				log.Errorf("%s: started server %d ok", t.Name(), theServer.GetConfig().NodeID)
			}
			ch <- err
		}()
	}

	for _, ch := range chans {
		err := <-ch
		if err != nil {
			log.Errorf("Got error in starting server %v", err)
		}
		require.NoError(t, err)
	}
}

type kafkaHolder struct {
	stopped bool
	kc      *kafka.KafkaContainer
	address string
}

func (k *kafkaHolder) stop() {
	if k.stopped {
		return
	}
	if err := k.kc.Stop(context.Background(), nil); err != nil {
		panic(err)
	}
	k.stopped = true
}

func (k *kafkaHolder) pauseResumeKafka(t *testing.T, pause bool) {
	containerID := k.kc.GetContainerID()
	dockerCli, err := docker.NewClientWithOpts(docker.FromEnv)
	require.NoError(t, err)
	defer func() {
		err := dockerCli.Close()
		require.NoError(t, err)
	}()
	if pause {
		err = dockerCli.ContainerPause(context.Background(), containerID)
		require.NoError(t, err)
	} else {
		err = dockerCli.ContainerUnpause(context.Background(), containerID)
		require.NoError(t, err)
	}
}

func startKafka(t *testing.T) *kafkaHolder {
	ctx := context.Background()

	if err := os.Setenv("DOCKER_API_VERSION", "1.44"); err != nil {
		panic(err)
	}

	// Start a container
	kc, err := kafka.RunContainer(ctx,
		kafka.WithClusterID(fmt.Sprintf("test-cluster-%s", uuid.NewString())),
		testcontainers.WithImage("confluentinc/confluent-local:7.7.0"),
	)
	require.NoError(t, err)
	// Get the address exposed by the container
	brokers, err := kc.Brokers(context.Background())
	require.NoError(t, err)
	containerKafkaAddress := brokers[0]
	return &kafkaHolder{
		kc:      kc,
		address: containerKafkaAddress,
	}
}

func createTopic(t *testing.T, topicName string, partitions int, serverAddress string) {
	cfg := &kafkago.ConfigMap{
		"bootstrap.servers": serverAddress,
	}
	adminClient, err := kafkago.NewAdminClient(cfg)
	require.NoError(t, err)
	defer adminClient.Close()

	topicSpec := kafkago.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	results, err := adminClient.CreateTopics(context.Background(), []kafkago.TopicSpecification{topicSpec})
	require.NoError(t, err)
	require.Equal(t, kafkago.ErrNoError, results[0].Error.Code())
}

func sendMessages(numBatches int, batchSize int, startIndex int, topicName string, producer *kafkago.Producer) ([]*kafkago.Message, error) {
	var msgs []*kafkago.Message
	msgID := startIndex
	for i := 0; i < numBatches; i++ {
		deliveryChan := make(chan kafkago.Event, batchSize)
		ts := time.Now().UTC()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("key%05d", msgID))
			value := []byte(fmt.Sprintf("value%05d", msgID))
			err := producer.Produce(&kafkago.Message{
				TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
				Key:            key,
				Value:          value,
				Timestamp:      ts,
			},
				deliveryChan,
			)
			if err != nil {
				return nil, err
			}
			msgID++
		}
		for j := 0; j < batchSize; j++ {
			e := <-deliveryChan
			m := e.(*kafkago.Message)
			if m.TopicPartition.Error != nil {
				return nil, m.TopicPartition.Error
			}
			log.Debugf("sent message %d", i)
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

func putUserCred(t *testing.T, sm *auth.ScramManager, username string, password string, authType string) {
	storedKey, serverKey, salt := auth.CreateUserScramCreds(password, authType)
	err := sm.PutUserCredentials(username, storedKey, serverKey, salt, auth.NumIters)
	require.NoError(t, err)
}
