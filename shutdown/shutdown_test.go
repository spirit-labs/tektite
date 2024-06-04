package shutdown

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/kafka/fake"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/server"
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

func TestMain(m *testing.M) {
	common.EnableTestPorts()
	testutils.RequireEtcd()
	defer testutils.ReleaseEtcd()
	m.Run()
}

func startClusterWithObjStore(t *testing.T) ([]*server.Server, *dev.Store, func(t *testing.T)) {

	objStore, objStoreAddress := startObjStore(t)

	servers, tearDown := startCluster(t, objStoreAddress, nil)

	return servers, objStore, tearDown
}

func startObjStore(t *testing.T) (*dev.Store, string) {
	objStoreAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	objStore := dev.NewDevStore(objStoreAddress)
	err = objStore.Start()
	require.NoError(t, err)
	return objStore, objStoreAddress
}

func createConfig(t *testing.T, objStoreAddress string) conf.Config {
	numServers := 3

	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}

	var clusterAddresses []string
	var apiAddresses []string
	for i := 0; i < numServers; i++ {
		remotingAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		clusterAddresses = append(clusterAddresses, remotingAddress)

		apiAddress, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		apiAddresses = append(apiAddresses, apiAddress)
	}

	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.ClusterManagerKeyPrefix = t.Name()
	cfg.ClusterAddresses = clusterAddresses
	cfg.HttpApiEnabled = true
	cfg.HttpApiAddresses = apiAddresses
	cfg.HttpApiTlsConfig = tlsConf
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	cfg.MinSnapshotInterval = 100 * time.Millisecond
	cfg.MemtableMaxReplaceInterval = 1 * time.Second

	// In real life don't want to set this so low otherwise cluster state will be calculated when just one node
	// is started with all leaders
	cfg.ClusterStateUpdateInterval = 10 * time.Millisecond

	// Set this low so store retries quickly to get prefix retentions on startup.
	cfg.LevelManagerRetryDelay = 10 * time.Millisecond

	cfg.DevObjectStoreAddresses = []string{objStoreAddress}
	return cfg
}

func startCluster(t *testing.T, objStoreAddress string, fk *fake.Kafka) ([]*server.Server, func(t *testing.T)) {
	cfg := createConfig(t, objStoreAddress)
	return startClusterWithConfig(t, cfg, fk)
}

func startClusterWithConfig(t *testing.T, cfg conf.Config, fk *fake.Kafka) ([]*server.Server, func(t *testing.T)) {
	numServers := 3

	var servers []*server.Server

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
		theServer := s
		go func() {
			err := theServer.Start()
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
		for _, s := range servers {
			err := s.Stop()
			require.NoError(t, err)
		}
	}
}

func TestShutdownNoData(t *testing.T) {
	servers, objStore, tearDown := startClusterWithObjStore(t)
	defer func() {
		tearDown(t)
		err := objStore.Stop()
		require.NoError(t, err)
	}()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ClusterAddresses = servers[0].GetConfig().ClusterAddresses

	err := PerformShutdown(cfg, false)
	require.NoError(t, err)
}

func TestShutdownWithData(t *testing.T) {

	fk := &fake.Kafka{}
	objStore, objStoreAddress := startObjStore(t)
	servers, _ := startCluster(t, objStoreAddress, fk)
	defer func() {
		err := objStore.Stop()
		require.NoError(t, err)
	}()

	topic, err := fk.CreateTopic("test_topic", 10)
	require.NoError(t, err)

	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}

	client, err := tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)

	err = client.ExecuteStatement(`test_stream := (bridge from test_topic partitions = 10 props = ()) -> (store stream)`)
	require.NoError(t, err)

	qr, err := client.ExecuteQuery("(scan all from test_stream)")
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
		qr, err = client.ExecuteQuery("(scan all from test_stream)")
		if err != nil {
			return false, err
		}
		return qr.RowCount() == numMessages, nil
	}, 10*time.Second, 1*time.Second)
	require.True(t, ok)
	require.NoError(t, err)

	listenAddresses := servers[0].GetConfig().ClusterAddresses
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ClusterAddresses = listenAddresses

	err = PerformShutdown(cfg, false)
	require.NoError(t, err)

	client.Close()

	// Now we restart the cluster
	servers, tearDown := startCluster(t, objStoreAddress, fk)
	defer tearDown(t)

	client, err = tekclient.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	// Data should still be there
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		qr, err = client.ExecuteQuery("(scan all from test_stream)")
		if err != nil {
			return false, err
		}
		return qr.RowCount() == numMessages, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	listenAddresses = servers[0].GetConfig().ClusterAddresses
	cfg = &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ClusterAddresses = listenAddresses

	err = PerformShutdown(cfg, false)
	require.NoError(t, err)
}
