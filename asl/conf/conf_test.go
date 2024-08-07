package conf

import (
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func invalidNodeIDConf() Config {
	cnf := validConf()
	cnf.NodeID = -1
	return cnf
}

func invalidClusterNameConf() Config {
	cnf := validConf()
	cnf.ClusterName = ""
	return cnf
}

func invalidProcessorCountNoLevelManagerConf() Config {
	cnf := validConf()
	cnf.LevelManagerEnabled = false
	cnf.ProcessingEnabled = true
	cnf.ProcessorCount = 0
	return cnf
}

func invalidProcessorCountLevelManagerConf() Config {
	cnf := validConf()
	cnf.LevelManagerEnabled = true
	cnf.ProcessingEnabled = false
	cnf.ProcessorCount = -1
	return cnf
}

func invalidMaxBackfillBatchSize() Config {
	cnf := validConf()
	cnf.MaxBackfillBatchSize = 0
	return cnf
}

func invalidMaxProcessorQueueSizeConf() Config {
	cnf := validConf()
	cnf.MaxProcessorBatchesInProgress = 0
	return cnf
}

func invalidMinSnapshotInterval() Config {
	cnf := validConf()
	cnf.MinSnapshotInterval = 0
	return cnf
}

func invalidIdleProcessorCheckInterval() Config {
	cnf := validConf()
	cnf.IdleProcessorCheckInterval = 0
	return cnf
}

func invalidMemtableMaxSizeBytesConf() Config {
	cnf := validConf()
	cnf.MemtableMaxSizeBytes = 0
	return cnf
}

func invalidMemtableMaxReplaceTimeConf() Config {
	cnf := validConf()
	cnf.MemtableMaxReplaceInterval = 0
	return cnf
}

func invalidMemtableFlushQueueMaxSizeConf() Config {
	cnf := validConf()
	cnf.MemtableFlushQueueMaxSize = 0
	return cnf
}

func invalidMinReplicasConf() Config {
	cnf := validConf()
	cnf.MinReplicas = 0
	return cnf
}

func invalidMaxReplicasConf() Config {
	cnf := validConf()
	cnf.MinReplicas = 1
	cnf.MaxReplicas = 0
	return cnf
}

func invalidMaxLessThanMinReplicasConf() Config {
	cnf := validConf()
	cnf.MinReplicas = 3
	cnf.MaxReplicas = 2
	return cnf
}

func invalidTableFormatConf() Config {
	cnf := validConf()
	cnf.TableFormat = 0
	return cnf
}

func invalidHTTPAPIServerListenAddress() Config {
	cnf := validConf()
	cnf.HttpApiEnabled = true
	cnf.HttpApiAddresses = nil
	return cnf
}

func httpAPIServerTLSKeyPathNotSpecifiedConfig() Config {
	cnf := validConf()
	cnf.HttpApiTlsConfig.KeyPath = ""
	return cnf
}

func httpAPIServerTLSCertPathNotSpecifiedConfig() Config {
	cnf := validConf()
	cnf.HttpApiTlsConfig.CertPath = ""
	return cnf
}

func httpAPIServerTLSNotEnabled() Config {
	cnf := validConf()
	cnf.HttpApiTlsConfig.Enabled = false
	return cnf
}

func httpAPIServerNoClientCerts() Config {
	cnf := validConf()
	cnf.HttpApiTlsConfig.ClientCertsPath = ""
	return cnf
}

func adminConsoleNoAddresses() Config {
	cnf := validConf()
	cnf.AdminConsoleEnabled = true
	cnf.AdminConsoleAddresses = []string{}
	return cnf
}

func intraClusterTLSCertPathNotSpecifiedConfig() Config {
	cnf := validConf()
	cnf.ClusterTlsConfig.CertPath = ""
	return cnf
}

func intraClusterTLSKeyPathNotSpecifiedConfig() Config {
	cnf := validConf()
	cnf.ClusterTlsConfig.KeyPath = ""
	return cnf
}

func intraClusterTLSCAPathNotSpecifiedConfig() Config {
	cnf := validConf()
	cnf.ClusterTlsConfig.ClientCertsPath = ""
	return cnf
}

const (
	lifeCycleListenAddress = "localhost:8765"
	startupEndpointPath    = "/started"
	liveEndpointPath       = "/liveness"
	readyEndpointPath      = "/readiness"
)

func invalidLifecycleListenAddress() Config {
	cnf := validConf()
	cnf.LifeCycleEndpointEnabled = true
	cnf.LifeCycleAddress = ""
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidStartupEndpointPath() Config {
	cnf := validConf()
	cnf.LifeCycleEndpointEnabled = true
	cnf.LifeCycleAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = ""
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidLiveEndpointPath() Config {
	cnf := validConf()
	cnf.LifeCycleEndpointEnabled = true
	cnf.LifeCycleAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = ""
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidReadyEndpointPath() Config {
	cnf := validConf()
	cnf.LifeCycleEndpointEnabled = true
	cnf.LifeCycleAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = ""
	return cnf
}

func invalidSegmentCacheMaxSize() Config {
	cnf := validConf()
	cnf.SegmentCacheMaxSize = -1
	return cnf
}

func invalidLevelManagerFlushInterval() Config {
	cnf := validConf()
	cnf.LevelManagerFlushInterval = 0
	return cnf
}

func nodeIDOutOfRangeConf() Config {
	cnf := validConf()
	cnf.NodeID = len(cnf.ClusterAddresses)
	return cnf
}

func invalidLockTimeoutConf() Config {
	cnf := validConf()
	cnf.ClusterManagerLockTimeout = 0
	return cnf
}

func invalidClusterManagerKeyPrefix() Config {
	cnf := validConf()
	cnf.ClusterManagerKeyPrefix = ""
	return cnf
}

func invalidClusterAddresses() Config {
	cnf := validConf()
	cnf.ClusterManagerAddresses = []string{}
	return cnf
}

func invalidClusterEvictionTimeout() Config {
	cnf := validConf()
	cnf.ClusterEvictionTimeout = time.Millisecond
	return cnf
}

func invalidClusterStateUpdateInterval() Config {
	cnf := validConf()
	cnf.ClusterStateUpdateInterval = time.Microsecond
	return cnf
}

func invalidKafkaInitialJoinDelay() Config {
	cnf := validConf()
	cnf.KafkaInitialJoinDelay = -1
	return cnf
}

func invalidKafkaMinSessionTimeout() Config {
	cnf := validConf()
	cnf.KafkaMinSessionTimeout = -1
	return cnf
}

func invalidKafkaMaxSessionTimeout() Config {
	cnf := validConf()
	cnf.KafkaMaxSessionTimeout = -1
	return cnf
}

func kafkaMaxSessionTimeoutLowerThanMinSessionTimeout() Config {
	cnf := validConf()
	cnf.KafkaMaxSessionTimeout = 2 * time.Second
	cnf.KafkaMinSessionTimeout = 3 * time.Second
	return cnf
}

func invalidKafkaServerListenerZerosAddress() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"0.0.0.0:8880"}
	return cnf
}

func invalidKafkaServerListenerAddressPortOnly() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.9.1:8880", ":8880"}
	return cnf
}

func invalidKafkaServerListenerAddressPort() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.9.1:8xbc"}
	return cnf
}

func invalidKafkaServerListenerZerosAdvertisedAddress() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.0.1:8880"}
	cnf.KafkaServerListenerConfig.AdvertisedAddresses = []string{"0.0.0.0:8880"}
	return cnf
}

func invalidKafkaServerListenerAdvertisedAddressPortOnly() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.0.1:8880", "192.168.1.1:8880"}
	cnf.KafkaServerListenerConfig.AdvertisedAddresses = []string{"192.168.9.1:8880", ":8880"}
	return cnf
}

func invalidKafkaServerListenerAdvertisedAddressPort() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.9.1:8880"}
	cnf.KafkaServerListenerConfig.AdvertisedAddresses = []string{"192.168.9.1:8xbc"}
	return cnf
}

func invalidKafkaServerListenerAdvertisedAddressLength() Config {
	cnf := validConf()
	cnf.KafkaServerListenerConfig.Addresses = []string{"192.168.0.1:8880", "192.168.1.1:8880"}
	cnf.KafkaServerListenerConfig.AdvertisedAddresses = []string{"192.168.0.1:8880"}
	return cnf
}

func invalidTableCacheSSTableMaxAge() Config {
	cnf := validConf()
	cnf.TableCacheSSTableMaxAge = 0
	return cnf
}

func TestValidate(t *testing.T) {
	tcs := []struct {
		name string
		conf Config
		err  string
	}{
		{
			"Negative node-id",
			invalidNodeIDConf(),
			"invalid configuration: node-id must be >= 0",
		},
		{
			"node-id > length of cluster-addresses",
			nodeIDOutOfRangeConf(),
			"invalid configuration: node-id must be >= 0 and < length cluster-addresses",
		},

		{
			"No cluster-name",
			invalidClusterNameConf(),
			"invalid configuration: cluster-name must be specified",
		},
		{
			"Zero processor-count",
			invalidProcessorCountNoLevelManagerConf(),
			"invalid configuration: processor-count must be > 0",
		},
		{
			"Negative processor-count",
			invalidProcessorCountLevelManagerConf(),
			"invalid configuration: processor-count must be >= 0",
		},
		{
			"Zero mak-backfill-size", invalidMaxBackfillBatchSize(),
			"invalid configuration: max-backfill-batch-size must be > 0",
		},
		{
			"Zero max-processor-batches-in-progress",
			invalidMaxProcessorQueueSizeConf(),
			"invalid configuration: max-processor-batches-in-progress must be > 0",
		},
		{
			"Zero min-snapshot-interval",
			invalidMinSnapshotInterval(),
			"invalid configuration: min-snapshot-interval must be >= 1 millisecond",
		},
		{
			"Zero idle-processor-check-interval",
			invalidIdleProcessorCheckInterval(),
			"invalid configuration: idle-processor-check-interval must be >= 1 millisecond",
		},
		{
			"Zero memtable-flush-queue-max-size",
			invalidMemtableFlushQueueMaxSizeConf(),
			"invalid configuration: memtable-flush-queue-max-size must be > 0",
		},
		{
			"Zero memtable-max-replace-time",
			invalidMemtableMaxReplaceTimeConf(),
			"invalid configuration: memtable-max-replace-time must be >= 1 ms",
		},
		{
			"Zero memtable-max-size-bytes",
			invalidMemtableMaxSizeBytesConf(),
			"invalid configuration: memtable-max-size-bytes must be > 0",
		},
		{
			"Zero min-replicas",
			invalidMinReplicasConf(),
			"invalid configuration: min-replicas must be > 0",
		},
		{
			"Zero max-replicas",
			invalidMaxReplicasConf(),
			"invalid configuration: max-replicas must be > 0",
		},
		{
			"min-replicas > max-replicas",
			invalidMaxLessThanMinReplicasConf(),
			"invalid configuration: min-replicas must be <= max-replicas",
		},
		{
			"No table-format",
			invalidTableFormatConf(),
			"invalid configuration: table-format must be specified",
		},

		{
			"No http-api-addresses",
			invalidHTTPAPIServerListenAddress(),
			"invalid configuration: http-api-addresses must be specified",
		},
		{
			"No life-cycle-address",
			invalidLifecycleListenAddress(),
			"invalid configuration: life-cycle-address must be specified",
		},
		{
			"No startup-endpoint-path",
			invalidStartupEndpointPath(),
			"invalid configuration: startup-endpoint-path must be specified",
		},
		{
			"No live-endpoint-path",
			invalidLiveEndpointPath(),
			"invalid configuration: live-endpoint-path must be specified",
		},
		{
			"No ready-endpoint-path",
			invalidReadyEndpointPath(),
			"invalid configuration: ready-endpoint-path must be specified",
		},

		{
			"No http-api-tls-key-path",
			httpAPIServerTLSKeyPathNotSpecifiedConfig(),
			"invalid configuration: http-api-tls-key-path must be specified for HTTP API server",
		},
		{
			"No http-api-tls-cert-path",
			httpAPIServerTLSCertPathNotSpecifiedConfig(),
			"invalid configuration: http-api-tls-cert-path must be specified for HTTP API server",
		},
		{
			"http-api-tls-enabled is false and http-api-enabled is true",
			httpAPIServerTLSNotEnabled(),
			"invalid configuration: http-api-tls-enabled must be true if http-api-enabled is true",
		},
		{
			"No http-api-tls-client-certs-path and client auth is enabled",
			httpAPIServerNoClientCerts(),
			"invalid configuration: http-api-tls-client-certs-path must be provided if client auth is enabled",
		},

		{
			"No admin-console-addresses",
			adminConsoleNoAddresses(),
			"invalid configuration: admin-console-addresses must be specified",
		},

		{
			"No cluster-tls-key-path and cluster-tls-enabled is true",
			intraClusterTLSKeyPathNotSpecifiedConfig(),
			"invalid configuration: cluster-tls-key-path must be specified if cluster-tls-enabled is true",
		},
		{
			"No cluster-tls-cert-path and cluster-tls-enabled is true",
			intraClusterTLSCertPathNotSpecifiedConfig(),
			"invalid configuration: cluster-tls-cert-path must be specified if cluster-tls-enabled is true",
		},
		{
			"No cluster-tls-client-certs-path and cluster-tls-enabled is true",
			intraClusterTLSCAPathNotSpecifiedConfig(),
			"invalid configuration: cluster-tls-client-certs-path must be specified if cluster-tls-enabled is true",
		},

		{
			"Zero level-manager-flush-interval",
			invalidLevelManagerFlushInterval(),
			"invalid configuration: level-manager-flush-interval must be >= 1ms",
		},
		{
			"Negative segment-cache-max-size",
			invalidSegmentCacheMaxSize(),
			"invalid configuration: segment-cache-max-size must be >= 0",
		},

		{
			"Zero cluster-manager-lock-timeout",
			invalidLockTimeoutConf(),
			"invalid configuration: cluster-manager-lock-timeout must be >= 1ms",
		},
		{
			"No cluster-manager-key-prefix",
			invalidClusterManagerKeyPrefix(),
			"invalid configuration: cluster-manager-key-prefix must be specified",
		},
		{
			"No cluster-manager-addresses",
			invalidClusterAddresses(),
			"invalid configuration: cluster-manager-addresses must be specified",
		},
		{
			"cluster-eviction-timeout less than 1s",
			invalidClusterEvictionTimeout(),
			"invalid configuration: cluster-eviction-timeout must be >= 1s",
		},
		{
			"cluster-state-update-interval less than 1ms",
			invalidClusterStateUpdateInterval(),
			"invalid configuration: cluster-state-update-interval must be >= 1ms",
		},

		{
			"Negative kafka-initial-join-delay", invalidKafkaInitialJoinDelay(),
			"invalid configuration: kafka-initial-join-delay must be >= 0",
		},
		{
			"Negative kafka-min-session-timeout",
			invalidKafkaMinSessionTimeout(),
			"invalid configuration: kafka-min-session-timeout must be >= 0",
		},
		{
			"Negative kafka-max-session-timeout",
			invalidKafkaMaxSessionTimeout(),
			"invalid configuration: kafka-max-session-timeout must be >= 0",
		},
		{
			"kafka-max-session-timeout < kafka-min-session-timeout",
			kafkaMaxSessionTimeoutLowerThanMinSessionTimeout(),
			"invalid configuration: kafka-max-session-timeout must be > kafka-min-session-timeout",
		},
		{
			"0.0.0.0 kafka-server-listener-addresses",
			invalidKafkaServerListenerZerosAddress(),
			"invalid configuration: 0.0.0.0 is not a valid address for kafka-server-listener-addresses",
		},
		{
			":8880 kafka-server-listener-addresses",
			invalidKafkaServerListenerAddressPortOnly(),
			"invalid configuration: invalid address :8880 for kafka-server-listener-addresses. IP address is required",
		},
		{
			":8xbc kafka-server-listener-addresses",
			invalidKafkaServerListenerAddressPort(),
			"invalid configuration: invalid port 8xbc for kafka-server-listener-addresses. Port must be a number",
		},
		{
			"0.0.0.0 kafka-server-listener-advertised-addresses",
			invalidKafkaServerListenerZerosAdvertisedAddress(),
			"invalid configuration: 0.0.0.0 is not a valid address for kafka-server-listener-advertised-addresses",
		},
		{
			":8880 kafka-server-listener-advertised-addresses",
			invalidKafkaServerListenerAdvertisedAddressPortOnly(),
			"invalid configuration: invalid address :8880 for kafka-server-listener-advertised-addresses. IP address is required",
		},
		{
			":8xbc kafka-server-listener-advertised-addresses",
			invalidKafkaServerListenerAdvertisedAddressPort(),
			"invalid configuration: invalid port 8xbc for kafka-server-listener-advertised-addresses. Port must be a number",
		},
		{

			"kafka-server-listener-advertised-addresses different length than kafka-server-listener-addresses",
			invalidKafkaServerListenerAdvertisedAddressLength(),
			"invalid configuration: kafka-server-listener-advertised-addresses must be the same length as kafka-server-listener-addresses",
		},
		{
			"Zero table-cache-sstable-max-age",
			invalidTableCacheSSTableMaxAge(),
			"invalid configuration: table-cache-sstable-max-age must be >= 1ms",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.conf.Validate()
			require.Error(t, err)
			require.Equal(t, tc.err, err.Error())
			require.Error(t, err, "Didn't get error, expected: %s", tc.err)
			//goland:noinspection GoTypeAssertionOnErrors
			pe, ok := errwrap.Cause(err).(common.TektiteError)
			require.True(t, ok)
			require.Equal(t, common.InvalidConfiguration, pe.Code)
			require.Equal(t, tc.err, pe.Msg)
		})
	}
}

func validConf() Config {
	conf := Config{
		NodeID:           0,
		ClusterName:      "test_cluster",
		ClusterAddresses: []string{"addr4", "addr5", "addr6"},
		HttpApiEnabled:   true,
		HttpApiAddresses: []string{"addr10", "addr11", "addr12"},
		HttpApiTlsConfig: TLSConfig{
			Enabled:         true,
			KeyPath:         "http_key_path",
			CertPath:        "http_cert_path",
			ClientCertsPath: "http_client_certs_path",
			ClientAuth:      "http_client_auth",
		},
		ClusterTlsConfig: TLSConfig{
			Enabled:         true,
			KeyPath:         "intra_cluster_key_path",
			CertPath:        "intra_cluster_cert_path",
			ClientCertsPath: "intra_cluster_client_certs_path",
			ClientAuth:      "intra_cluster_client_auth",
		},
	}
	conf.ApplyDefaults()
	return conf
}
