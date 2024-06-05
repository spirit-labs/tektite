//go:build integration

package integration

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/kafka/fake"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/server"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
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
	cfg.ClusterAddresses = remotingAddresses
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

	// Start them in parallel
	var chans []chan error
	for _, s := range servers {
		ch := make(chan error, 1)
		chans = append(chans, ch)
		s := s
		go func() {
			err := s.Start()
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
	return servers, func(t *testing.T) {
		cfg := servers[0].GetConfig()
		err := shutdown.PerformShutdown(&cfg, true)
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}
}

func TestServer(t *testing.T) {
	fk := &fake.Kafka{}

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startCluster(t, 3, fk)
	defer tearDown(t)
	client, err := tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	// We create a stream, scan it, then delete it in a loop
	// This exercises the barrier logic and makes sure barriers still propagate and versions complete as streams are
	// deployed / undeployed, without getting stuck
	for i := 0; i < 10; i++ {
		streamName := fmt.Sprintf("test_stream-%d", i)

		topic, err := fk.CreateTopic("test_topic", 10)
		require.NoError(t, err)

		_, err = client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
		require.Error(t, err)
		require.Equal(t, fmt.Sprintf(`unknown table or stream '%s' (line 1 column 16):
(scan all from %s)
               ^`, streamName, streamName), err.Error())

		err = client.ExecuteStatement(fmt.Sprintf(`%s := 
		(bridge from test_topic
			partitions = 10
			props = ()
		) -> (partition by key partitions = 16) -> (store stream)`, streamName))
		require.NoError(t, err)

		qr, err := client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
		require.NoError(t, err)
		require.Equal(t, 0, qr.RowCount())

		numMessages := 10
		for i := 0; i < numMessages; i++ {
			// Generate some JSON messages
			var msg kafka.Message
			msg.Key = []byte(fmt.Sprintf("key%d", i))
			msg.Value = []byte(fmt.Sprintf("value%d", i))
			err := topic.Push(&msg)
			require.NoError(t, err)
		}

		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			qr, err = client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
			if err != nil {
				return false, err
			}
			return qr.RowCount() == numMessages, nil
		}, 10*time.Second, 1*time.Second)
		require.True(t, ok)
		require.NoError(t, err)

		err = client.ExecuteStatement(fmt.Sprintf(`delete(%s)`, streamName))
		require.NoError(t, err)

		err = fk.DeleteTopic("test_topic")
		require.NoError(t, err)
	}

}
