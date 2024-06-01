package main

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
)

func TestParseConfigWithComments(t *testing.T) {
	hcl, err := os.ReadFile("testdata/config.hcl")
	require.NoError(t, err)
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = types.AddressOf(2)
	testRunner(t, hcl, cnfExpected, 2)
}

func testRunner(t *testing.T, b []byte, cnf conf.Config, nodeID int) {
	t.Helper()
	dataDir, err := os.MkdirTemp("", "runner-test")
	require.NoError(t, err)
	defer removeDataDir(dataDir)

	fName := filepath.Join(dataDir, "json1.conf")
	err = os.WriteFile(fName, b, fs.ModePerm)
	require.NoError(t, err)

	r := &runner{}
	args := []string{"--config", fName, "--node-id", fmt.Sprintf("%d", nodeID)}

	cfg, err := r.loadConfig(args)
	require.NoError(t, err)
	cfg.Server.Original = types.AddressOf("")

	require.NoError(t, r.run(&cfg.Server, false, nil))

	actualConfig := r.getServer().GetConfig()
	//require.Equal(t, cnf, actualConfig)
	// TODO: fix this test
	require.True(t, reflect.DeepEqual(cnf, actualConfig))
}

func removeDataDir(dataDir string) {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Errorf("failed to remove datadir %v", err)
	}
}

func createConfigWithAllFields() conf.Config {
	return conf.Config{
		ProcessingEnabled:        types.AddressOf(true),
		LevelManagerEnabled:      types.AddressOf(true),
		CompactionWorkersEnabled: types.AddressOf(true),

		ClusterName:                    types.AddressOf("the_cluster_name"),
		ProcessorCount:                 types.AddressOf(96),
		MaxProcessorBatchesInProgress:  types.AddressOf(1000),
		MemtableMaxSizeBytes:           (*conf.ParseableInt)(types.AddressOf(10000000)),
		MemtableMaxReplaceInterval:     types.AddressOf(37 * time.Second),
		MemtableFlushQueueMaxSize:      types.AddressOf(50),
		StoreWriteBlockedRetryInterval: types.AddressOf(777 * time.Millisecond),
		MinReplicas:                    types.AddressOf(3),
		MaxReplicas:                    types.AddressOf(5),
		TableFormat:                    types.AddressOf(common.DataFormatV1),
		MinSnapshotInterval:            types.AddressOf(13 * time.Second),
		IdleProcessorCheckInterval:     types.AddressOf(23 * time.Second),
		BatchFlushCheckInterval:        types.AddressOf(7 * time.Second),
		ConsumerRetryInterval:          types.AddressOf(30 * time.Second),
		MaxBackfillBatchSize:           types.AddressOf(998),
		ClientType:                     (*conf.KafkaClientType)(types.AddressOf(conf.KafkaClientTypeConfluent)),
		ForwardResendDelay:             types.AddressOf(876 * time.Millisecond),

		QueryMaxBatchRows: types.AddressOf(999),

		HttpApiPath: types.AddressOf("/wibble"),

		RegistryFormat:                 types.AddressOf(common.MetadataFormat(3)),
		MasterRegistryRecordID:         types.AddressOf("avocados"),
		MaxRegistrySegmentTableEntries: types.AddressOf(12345),
		LevelManagerFlushInterval:      types.AddressOf(10 * time.Second),
		SegmentCacheMaxSize:            types.AddressOf(777),

		ClusterManagerLockTimeout:  types.AddressOf(7 * time.Minute),
		ClusterManagerKeyPrefix:    types.AddressOf("test_keyprefix"),
		ClusterManagerAddresses:    []string{"etcd1", "etcd2"},
		ClusterEvictionTimeout:     types.AddressOf(13 * time.Second),
		ClusterStateUpdateInterval: types.AddressOf(1500 * time.Millisecond),
		EtcdCallTimeout:            types.AddressOf(7 * time.Second),

		ClusterAddresses: []string{"addr1", "addr2", "addr3", "addr4", "addr5"},

		HttpApiEnabled:   types.AddressOf(true),
		HttpApiAddresses: []string{"addr11-1", "addr12-1", "addr13-1", "addr14-1", "addr15-1"},
		HttpApiTlsConfig: &conf.TLSConfig{
			Enabled:         true,
			KeyPath:         "http-key-path",
			CertPath:        "http-cert-path",
			ClientCertsPath: "http-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		MetricsBind:    types.AddressOf("localhost:9102"),
		MetricsEnabled: types.AddressOf(false),

		KafkaServerEnabled:      types.AddressOf(true),
		KafkaServerAddresses:    []string{"kafka1:9301", "kafka2:9301", "kafka3:9301", "kafka4:9301", "kafka5:9301"},
		KafkaUseServerTimestamp: types.AddressOf(true),
		KafkaServerTLSConfig: &conf.TLSConfig{
			Enabled:         true,
			KeyPath:         "kafka-key-path",
			CertPath:        "kafka-cert-path",
			ClientCertsPath: "kafka-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		KafkaInitialJoinDelay:       types.AddressOf(2 * time.Second),
		KafkaMinSessionTimeout:      types.AddressOf(7 * time.Second),
		KafkaMaxSessionTimeout:      types.AddressOf(25 * time.Second),
		KafkaNewMemberJoinTimeout:   types.AddressOf(4 * time.Second),
		KafkaFetchCacheMaxSizeBytes: (*conf.ParseableInt)(types.AddressOf(7654321)),

		CommandCompactionInterval: types.AddressOf(3 * time.Second),

		DDProfilerTypes:           types.AddressOf("HEAP,CPU"),
		DDProfilerServiceName:     types.AddressOf("my-service"),
		DDProfilerEnvironmentName: types.AddressOf("playing"),
		DDProfilerPort:            types.AddressOf(1324),
		DDProfilerVersionName:     types.AddressOf("2.3"),
		DDProfilerHostEnvVarName:  types.AddressOf("FOO_IP"),

		ClusterTlsConfig: conf.TLSConfig{
			Enabled:         true,
			KeyPath:         "intra-cluster-key-path",
			CertPath:        "intra-cluster-cert-path",
			ClientCertsPath: "intra-cluster-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		LevelManagerRetryDelay:             types.AddressOf(750 * time.Millisecond),
		L0CompactionTrigger:                types.AddressOf(12),
		L0MaxTablesBeforeBlocking:          types.AddressOf(21),
		L1CompactionTrigger:                types.AddressOf(23),
		LevelMultiplier:                    types.AddressOf(3),
		CompactionPollerTimeout:            types.AddressOf(777 * time.Millisecond),
		CompactionJobTimeout:               types.AddressOf(7 * time.Minute),
		CompactionWorkerCount:              types.AddressOf(12),
		SSTableDeleteCheckInterval:         types.AddressOf(350 * time.Millisecond),
		SSTableDeleteDelay:                 types.AddressOf(1 * time.Hour),
		SSTableRegisterRetryDelay:          types.AddressOf(35 * time.Second),
		SSTablePushRetryDelay:              types.AddressOf(6 * time.Second),
		PrefixRetentionRemoveCheckInterval: types.AddressOf(17 * time.Second),
		PrefixRetentionRefreshInterval:     types.AddressOf(13 * time.Second),
		CompactionMaxSSTableSize:           types.AddressOf(54321),

		TableCacheMaxSizeBytes: (*conf.ParseableInt)(types.AddressOf(12345678)),

		SequencesObjectName: types.AddressOf("my_sequences"),
		SequencesRetryDelay: types.AddressOf(300 * time.Millisecond),

		DevObjectStoreAddresses: []string{"addr23"},
		ObjectStoreType:         types.AddressOf("dev"),

		VersionCompletedBroadcastInterval:  types.AddressOf(2 * time.Second),
		VersionManagerStoreFlushedInterval: types.AddressOf(23 * time.Second),

		WasmModuleInstances: types.AddressOf(23),
	}
}
