package conf

import (
	"github.com/spirit-labs/tektite/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

type configPair struct {
	errMsg string
	conf   Config
}

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

func invalidMaxProcessorQueueSizeConf() Config {
	cnf := validConf()
	cnf.MaxProcessorBatchesInProgress = 0
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

var invalidConfigs = []configPair{
	{"invalid configuration: node-id must be >= 0", invalidNodeIDConf()},
	{"invalid configuration: node-id must be >= 0 and < length cluster-addresses", nodeIDOutOfRangeConf()},

	{"invalid configuration: cluster-name must be specified", invalidClusterNameConf()},
	{"invalid configuration: processor-count must be > 0", invalidProcessorCountNoLevelManagerConf()},
	{"invalid configuration: processor-count must be >= 0", invalidProcessorCountLevelManagerConf()},
	{"invalid configuration: max-processor-batches-in-progress must be > 0", invalidMaxProcessorQueueSizeConf()},
	{"invalid configuration: memtable-flush-queue-max-size must be > 0", invalidMemtableFlushQueueMaxSizeConf()},
	{"invalid configuration: memtable-max-replace-time must be >= 1 ms", invalidMemtableMaxReplaceTimeConf()},
	{"invalid configuration: memtable-max-size-bytes must be > 0", invalidMemtableMaxSizeBytesConf()},
	{"invalid configuration: min-replicas must be > 0", invalidMinReplicasConf()},
	{"invalid configuration: max-replicas must be > 0", invalidMaxReplicasConf()},
	{"invalid configuration: min-replicas must be <= max-replicas", invalidMaxLessThanMinReplicasConf()},
	{"invalid configuration: table-format must be specified", invalidTableFormatConf()},

	{"invalid configuration: http-api-addresses must be specified", invalidHTTPAPIServerListenAddress()},
	{"invalid configuration: life-cycle-address must be specified", invalidLifecycleListenAddress()},
	{"invalid configuration: startup-endpoint-path must be specified", invalidStartupEndpointPath()},
	{"invalid configuration: live-endpoint-path must be specified", invalidLiveEndpointPath()},
	{"invalid configuration: ready-endpoint-path must be specified", invalidReadyEndpointPath()},

	{"invalid configuration: http-api-tls-key-path must be specified for HTTP API server", httpAPIServerTLSKeyPathNotSpecifiedConfig()},
	{"invalid configuration: http-api-tls-cert-path must be specified for HTTP API server", httpAPIServerTLSCertPathNotSpecifiedConfig()},
	{"invalid configuration: http-api-tls-enabled must be true if http-api-enabled is true", httpAPIServerTLSNotEnabled()},
	{"invalid configuration: http-api-tls-client-certs-path must be provided if client auth is enabled", httpAPIServerNoClientCerts()},

	{"invalid configuration: cluster-tls-key-path must be specified if cluster-tls-enabled is true", intraClusterTLSKeyPathNotSpecifiedConfig()},
	{"invalid configuration: cluster-tls-cert-path must be specified if cluster-tls-enabled is true", intraClusterTLSCertPathNotSpecifiedConfig()},
	{"invalid configuration: cluster-tls-client-certs-path must be specified if cluster-tls-enabled is true", intraClusterTLSCAPathNotSpecifiedConfig()},

	{"invalid configuration: level-manager-flush-interval must be >= 1ms", invalidLevelManagerFlushInterval()},
	{"invalid configuration: segment-cache-max-size must be >= 0", invalidSegmentCacheMaxSize()},

	{"invalid configuration: cluster-manager-lock-timeout must be >= 1ms", invalidLockTimeoutConf()},
}

func TestValidate(t *testing.T) {
	for _, cp := range invalidConfigs {
		err := cp.conf.Validate()
		require.Error(t, err, "Didn't get error, expected: %s", cp.errMsg)
		//goland:noinspection GoTypeAssertionOnErrors
		pe, ok := errors.Cause(err).(errors.TektiteError)
		require.True(t, ok)
		require.Equal(t, errors.InvalidConfiguration, int(pe.Code))
		require.Equal(t, cp.errMsg, pe.Msg)
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
