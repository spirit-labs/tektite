// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spirit-labs/tektite/conf"
	"github.com/stretchr/testify/require"
)

func TestParseConfigWithComments(t *testing.T) {
	hcl, err := os.ReadFile("testdata/config.hcl")
	require.NoError(t, err)
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = 2
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
	cfg.Server.Original = ""

	require.NoError(t, r.run(&cfg.Server, false, nil))

	actualConfig := r.getServer().GetConfig()
	require.Equal(t, cnf, actualConfig)
}

func removeDataDir(dataDir string) {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Errorf("failed to remove datadir %v", err)
	}
}

func createConfigWithAllFields() conf.Config {
	return conf.Config{
		ProcessingEnabled:        true,
		LevelManagerEnabled:      true,
		CompactionWorkersEnabled: true,

		ClusterName:                    "the_cluster_name",
		ProcessorCount:                 96,
		MaxProcessorBatchesInProgress:  1000,
		MemtableMaxSizeBytes:           10000000,
		MemtableMaxReplaceInterval:     37 * time.Second,
		MemtableFlushQueueMaxSize:      50,
		StoreWriteBlockedRetryInterval: 777 * time.Millisecond,
		MinReplicas:                    3,
		MaxReplicas:                    5,
		TableFormat:                    common.DataFormatV1,
		MinSnapshotInterval:            13 * time.Second,
		IdleProcessorCheckInterval:     23 * time.Second,
		BatchFlushCheckInterval:        7 * time.Second,
		ConsumerRetryInterval:          30 * time.Second,
		MaxBackfillBatchSize:           998,
		ClientType:                     conf.KafkaClientTypeConfluent,
		ForwardResendDelay:             876 * time.Millisecond,

		QueryMaxBatchRows: 999,

		HttpApiPath: "/wibble",

		RegistryFormat:                 3,
		MasterRegistryRecordID:         "avocados",
		MaxRegistrySegmentTableEntries: 12345,
		LevelManagerFlushInterval:      10 * time.Second,
		SegmentCacheMaxSize:            777,

		ClusterManagerLockTimeout:  7 * time.Minute,
		ClusterManagerKeyPrefix:    "test_keyprefix",
		ClusterManagerAddresses:    []string{"etcd1", "etcd2"},
		ClusterEvictionTimeout:     13 * time.Second,
		ClusterStateUpdateInterval: 1500 * time.Millisecond,
		EtcdCallTimeout:            7 * time.Second,

		ClusterAddresses: []string{"addr1", "addr2", "addr3", "addr4", "addr5"},

		HttpApiEnabled:   true,
		HttpApiAddresses: []string{"addr11-1", "addr12-1", "addr13-1", "addr14-1", "addr15-1"},
		HttpApiTlsConfig: conf.TLSConfig{
			Enabled:         true,
			KeyPath:         "http-key-path",
			CertPath:        "http-cert-path",
			ClientCertsPath: "http-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		MetricsBind:    "localhost:9102",
		MetricsEnabled: false,

		KafkaServerEnabled: true,
		KafkaServerListenerConfig: conf.ListenerConfig{
			Addresses: []string{"kafka1:9301", "kafka2:9301", "kafka3:9301", "kafka4:9301", "kafka5:9301"},
			TLSConfig: conf.TLSConfig{
				Enabled:         true,
				KeyPath:         "kafka-key-path",
				CertPath:        "kafka-cert-path",
				ClientCertsPath: "kafka-client-certs-path",
				ClientAuth:      "require-and-verify-client-cert",
			},
		},
		KafkaUseServerTimestamp:     true,
		KafkaInitialJoinDelay:       2 * time.Second,
		KafkaMinSessionTimeout:      7 * time.Second,
		KafkaMaxSessionTimeout:      25 * time.Second,
		KafkaNewMemberJoinTimeout:   4 * time.Second,
		KafkaFetchCacheMaxSizeBytes: 7654321,

		CommandCompactionInterval: 3 * time.Second,

		DDProfilerTypes:           "HEAP,CPU",
		DDProfilerServiceName:     "my-service",
		DDProfilerEnvironmentName: "playing",
		DDProfilerPort:            1324,
		DDProfilerVersionName:     "2.3",
		DDProfilerHostEnvVarName:  "FOO_IP",

		ClusterTlsConfig: conf.TLSConfig{
			Enabled:         true,
			KeyPath:         "intra-cluster-key-path",
			CertPath:        "intra-cluster-cert-path",
			ClientCertsPath: "intra-cluster-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		LevelManagerRetryDelay:             750 * time.Millisecond,
		L0CompactionTrigger:                12,
		L0MaxTablesBeforeBlocking:          21,
		L1CompactionTrigger:                23,
		LevelMultiplier:                    3,
		CompactionPollerTimeout:            777 * time.Millisecond,
		CompactionJobTimeout:               7 * time.Minute,
		CompactionWorkerCount:              12,
		SSTableDeleteCheckInterval:         350 * time.Millisecond,
		SSTableDeleteDelay:                 1 * time.Hour,
		SSTableRegisterRetryDelay:          35 * time.Second,
		SSTablePushRetryDelay:              6 * time.Second,
		PrefixRetentionRemoveCheckInterval: 17 * time.Second,
		PrefixRetentionRefreshInterval:     13 * time.Second,
		CompactionMaxSSTableSize:           54321,

		TableCacheMaxSizeBytes:  12345678,
		TableCacheSSTableMaxAge: 5 * time.Minute,

		SequencesObjectName: "my_sequences",
		SequencesRetryDelay: 300 * time.Millisecond,

		DevObjectStoreAddresses: []string{"addr23"},
		ObjectStoreType:         "dev",

		VersionCompletedBroadcastInterval:  2 * time.Second,
		VersionManagerStoreFlushedInterval: 23 * time.Second,

		WasmModuleInstances: 23,

		LogScope: "foo",
	}
}
