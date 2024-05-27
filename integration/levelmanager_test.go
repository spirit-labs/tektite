package integration

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/repli"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sequence"
	"github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

/*
TestLevelManagerCluster creates a cluster of processor managers with a levelManager on one of them, and uses the
external client and local client to access it.
*/
//goland:noinspection ALL
func TestLevelManagerCluster(t *testing.T) {
	t.Parallel()
	st := store.TestStore()
	err := st.Start()
	require.NoError(t, err)
	defer func() {
		err := st.Stop()
		require.NoError(t, err)
	}()

	cloudStore := dev.NewInMemStore(0)
	tcConf := conf.Config{}
	tcConf.ApplyDefaults()
	tabCache, err := tabcache.NewTableCache(cloudStore, &tcConf)
	require.NoError(t, err)
	seqMgr := sequence.NewInMemSequenceManager()

	numNodes := 3
	remotingAddresses := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		remotingAddresses[i] = fmt.Sprintf("localhost:%d", 7830+i)
	}

	var mgrs []proc.Manager
	var remotingServers []remoting.Server
	var clustStateMgrs []*testClustStateMgr
	var mapperServices []*levels.LevelManagerService
	var vmgrs []*vmgr.VersionManager

	for i := 0; i < numNodes; i++ {
		clustStateMgr := &testClustStateMgr{}
		clustStateMgrs = append(clustStateMgrs, clustStateMgr)
		cfg := &conf.Config{}
		cfg.ApplyDefaults()
		cfg.NodeID = i
		cfg.ClusterAddresses = remotingAddresses
		// There are no processors used for data-processing, however there will be one more than this (i.e. 1) for
		// the level manager
		cfg.ProcessorCount = 0
		cfg.LevelManagerEnabled = true

		handler := &testBatchHandler{}

		mgr := proc.NewProcessorManager(clustStateMgr, &testReceiverInfoProvider{}, st, cfg, repli.NewReplicator,
			func(processorID int) proc.BatchHandler {
				return handler
			}, nil, &testIngestNotifier{})

		levelManagerService := levels.NewLevelManagerService(mgr, cfg, cloudStore, tabCache,
			proc.NewLevelManagerCommandIngestor(mgr), mgr)

		mgr.SetLevelMgrProcessorInitialisedCallback(levelManagerService.ActivateLevelManager)
		err := mgr.Start()
		require.NoError(t, err)
		mgrs = append(mgrs, mgr)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := mgr.Stop()
			require.NoError(t, err)
		}()
		clustStateMgr.SetClusterStateHandler(mgr.HandleClusterState)

		remotingServer := remoting.NewServer(remotingAddresses[i], conf.TLSConfig{})
		err = remotingServer.Start()
		require.NoError(t, err)
		defer func() {
			err := remotingServer.Stop()
			require.NoError(t, err)
		}()
		remotingServers = append(remotingServers, remotingServer)
		teeHandler := &remoting.TeeBlockingClusterMessageHandler{}
		mgr.SetClusterMessageHandlers(remotingServer, teeHandler)
		remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageVersionsMessage, teeHandler)
		repli.SetClusterMessageHandlers(remotingServer, mgr)

		levelManagerService.SetClusterMessageHandlers(remotingServer)
		err = levelManagerService.Start()
		require.NoError(t, err)
		mgr.RegisterStateHandler(levelManagerService.HandleClusterState)
		mapperServices = append(mapperServices, levelManagerService)

		lMgrClient := proc.NewLevelManagerLocalClient(cfg)
		lMgrClient.SetProcessorManager(mgr)

		vMgr := vmgr.NewVersionManager(seqMgr, lMgrClient, cfg, remotingAddresses...)
		mgr.RegisterStateHandler(vMgr.HandleClusterState)
		vmgrs = append(vmgrs, vMgr)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := vMgr.Stop()
			require.NoError(t, err)
		}()

		vMgr.SetClusterMessageHandlers(remotingServer)

		levelManagerBatchHandler := proc.NewLevelManagerBatchHandler(levelManagerService)
		handler.levelManagerHandler = levelManagerBatchHandler
	}

	// deploy processor 0
	cs := clustmgr.ClusterState{
		Version: 23,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 1},
			},
		},
	}

	for _, sm := range clustStateMgrs {
		err := sm.sendClusterState(cs)
		require.NoError(t, err)
	}

	// Test local client
	cfg := &conf.Config{}
	cfg.ClusterAddresses = remotingAddresses
	cfg.ApplyDefaults()
	cfg.ProcessorCount = 0
	mgr := mgrs[rand.Intn(numNodes)]
	localClient := proc.NewLevelManagerLocalClient(cfg)
	localClient.SetProcessorManager(mgr)
	sendClientCommands(t, localClient)
	err = localClient.Stop()
	require.NoError(t, err)
	// External client
	externalClient := levels.NewExternalClient(remotingAddresses, conf.TLSConfig{}, 10*time.Millisecond)
	sendClientCommands(t, externalClient)
	err = externalClient.Stop()
	require.NoError(t, err)
}

func sendClientCommands(t *testing.T, client levels.Client) {
	regBatch := levels.RegistrationBatch{
		ClusterName:    "test_cluster",
		ClusterVersion: 123,
		Registrations: []levels.RegistrationEntry{
			{
				Level:    0,
				TableID:  []byte("some_table_id"),
				KeyStart: encoding.EncodeVersion([]byte("key01"), 0),
				KeyEnd:   encoding.EncodeVersion([]byte("key08"), 0),
			},
		},
	}
	_, err := common.CallWithRetryOnUnavailable[int](func() (int, error) {
		return 0, client.RegisterL0Tables(regBatch)
	}, func() bool {
		return false
	})
	require.NoError(t, err)

	tids, _, _, err := client.GetTableIDsForRange([]byte("key01"), []byte("key09"))
	require.NoError(t, err)
	require.NotNil(t, tids)

	require.Equal(t, 1, len(tids))
	nTids := tids[0]
	require.Equal(t, 1, len(nTids))
	receivedTid := nTids[0]
	require.Equal(t, "some_table_id", string(receivedTid))

	// Try and apply changes with an old version, should fail
	regBatch = levels.RegistrationBatch{
		ClusterName:    "test_cluster",
		ClusterVersion: 122,
		Registrations: []levels.RegistrationEntry{
			{
				Level:    0,
				TableID:  []byte("some_table_id"),
				KeyStart: encoding.EncodeVersion([]byte("key01"), 0),
				KeyEnd:   encoding.EncodeVersion([]byte("key08"), 0),
			},
		},
	}
	err = client.RegisterL0Tables(regBatch)
	require.Error(t, err)
	require.Equal(t, "registration batch version is too low", err.Error())

	pr := retention.PrefixRetention{
		Prefix:    []byte("some_prefix"),
		Retention: 12345,
	}
	err = client.RegisterPrefixRetentions([]retention.PrefixRetention{pr})
	require.NoError(t, err)

	prefixRetentions, err := client.GetPrefixRetentions()
	require.NoError(t, err)
	require.Equal(t, 1, len(prefixRetentions))
	pref := prefixRetentions[0]
	require.Equal(t, "some_prefix", string(pref.Prefix))
	require.Equal(t, 12345, int(pref.Retention))

	regBatch = levels.RegistrationBatch{
		ClusterName:    "test_cluster",
		ClusterVersion: 123,
		Registrations: []levels.RegistrationEntry{
			{
				Level:    1,
				TableID:  []byte("some_table_id2"),
				KeyStart: encoding.EncodeVersion([]byte("key20"), 0),
				KeyEnd:   encoding.EncodeVersion([]byte("key30"), 0),
			},
		},
		DeRegistrations: []levels.RegistrationEntry{
			{
				Level:    0,
				TableID:  []byte("some_table_id"),
				KeyStart: encoding.EncodeVersion([]byte("key01"), 0),
				KeyEnd:   encoding.EncodeVersion([]byte("key08"), 0),
			},
		},
	}
	err = client.ApplyChanges(regBatch)
	require.NoError(t, err)

	tids, _, _, err = client.GetTableIDsForRange([]byte("key20"), []byte("key25"))
	require.NoError(t, err)
	require.NotNil(t, tids)
	require.Equal(t, 1, len(tids))
}

type testBatchHandler struct {
	levelManagerHandler proc.BatchHandler
}

func (t *testBatchHandler) HandleProcessBatch(processor proc.Processor, processBatch *proc.ProcessBatch, reprocess bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	return t.levelManagerHandler.HandleProcessBatch(processor, processBatch, reprocess)
}

type testReceiverInfoProvider struct {
}

func (t *testReceiverInfoProvider) GetTerminalReceiverCount() int {
	return 0
}

func (t *testReceiverInfoProvider) GetForwardingProcessorCount(int) (int, bool) {
	return 1, true
}

func (t *testReceiverInfoProvider) GetInjectableReceivers(int) []int {
	return []int{common.LevelManagerReceiverID}
}

func (t *testReceiverInfoProvider) GetRequiredCompletions() int {
	return 1
}

type testIngestNotifier struct {
}

func (t *testIngestNotifier) StopIngest() error {
	return nil
}

func (t *testIngestNotifier) StartIngest(int) error {
	return nil
}
