package conf

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"net"
	"strconv"
	"time"
)

const (
	DefaultProcessorCount                 = 12
	DefaultMaxProcessorBatchesInProgress  = 1000
	DefaultMemtableMaxSizeBytes           = 16 * 1024 * 1024
	DefaultMemtableMaxReplaceInterval     = 30 * time.Second
	DefaultMemtableFlushQueueMaxSize      = 4
	DefaultStoreWriteBlockedRetryInterval = 250 * time.Millisecond
	DefaultMinReplicas                    = 2
	DefaultMaxReplicas                    = 3
	DefaultTableFormat                    = common.DataFormatV1
	DefaultMinSnapshotInterval            = 200 * time.Millisecond
	DefaultIdleProcessorCheckInterval     = 1 * time.Second
	DefaultBatchFlushCheckInterval        = 1 * time.Second
	DefaultConsumerRetryInterval          = 2 * time.Second
	DefaultQueryMaxBatchRows              = 1000
	DefaultMaxBackfillBatchSize           = 1000
	DefaultForwardResendDelay             = 250 * time.Millisecond

	DefaultHTTPAPIServerPath = "/tektite"

	DefaultLevelManagerFlushInterval      = 5 * time.Second
	DefaultMasterRecordRegistryID         = "tektite_master"
	DefaultMaxRegistrySegmentTableEntries = 50000
	DefaultRegistryFormat                 = common.MetadataFormatV1
	DefaultSegmentCacheMaxSize            = 100
	DefaultClusterName                    = "tektite_cluster"
	DefaultLevelManagerRetryDelay         = 250 * time.Millisecond
	// DefaultL0CompactionTrigger Note that default L0 and L1 compaction triggers are similar - this is because L0->L1 compaction compacts the whole
	// of L0 and key range can be large so it can merge with many/most of tables in L1. To prevent very large merge
	// We keep L1 max size approx. same as L0.
	DefaultL0CompactionTrigger                = 4
	DefaultL0MaxTablesBeforeBlocking          = 10
	DefaultL1CompactionTrigger                = 4
	DefaultLevelMultiplier                    = 10
	DefaultCompactionPollerTimeout            = 1 * time.Second
	DefaultCompactionJobTimeout               = 30 * time.Second
	DefaultSSTableDeleteDelay                 = 10 * time.Second
	DefaultSSTableDeleteCheckInterval         = 2 * time.Second
	DefaultSSTableRegisterRetryDelay          = 1 * time.Second
	DefaultPrefixRetentionRemoveCheckInterval = 30 * time.Second
	DefaultCompactionMaxSSTableSize           = 16 * 1024 * 1024

	DefaultCompactionWorkerCount          = 4
	DefaultPrefixRetentionRefreshInterval = 10 * time.Second

	DefaultEtcdCallTimeout = 5 * time.Second

	DefaultTableCacheMaxSizeBytes  = 128 * 1024 * 1024
	DefaultTableCacheSSTableMaxAge = 5 * time.Minute

	DefaultClusterManagerLockTimeout  = 2 * time.Minute
	DefaultClusterManagerKeyPrefix    = "tektite_clust_data/"
	DefaultClusterEvictionTimeout     = 5 * time.Second
	DefaultClusterStateUpdateInterval = 2 * time.Second

	DefaultSequencesObjectName = "tektite_sequences"
	DefaultSequencesRetryDelay = 250 * time.Millisecond

	DefaultVersionCompletedBroadcastInterval  = 500 * time.Millisecond
	DefaultVersionManagerStoreFlushedInterval = 1 * time.Second

	DefaultDevObjectStoreAddress = "127.0.0.1:6690"

	DefaultKafkaInitialJoinDelay       = 3 * time.Second
	DefaultKafkaMinSessionTimeout      = 6 * time.Second
	DefaultKafkaMaxSessionTimeout      = 30 * time.Minute
	DefaultKafkaNewMemberJoinTimeout   = 5 * time.Minute
	DefaultKafkaFetchCacheMaxSizeBytes = 128 * 1024 * 1024

	DefaultSSTablePushRetryDelay = 1 * time.Second

	DefaultCommandCompactionInterval = 5 * time.Minute

	DefaultWebUISampleInterval = 5 * time.Second

	DevObjectStoreType      = "dev"
	EmbeddedObjectStoreType = "embedded"
	MinioObjectStoreType    = "minio"

	DefaultWasmModuleInstances = 8

	DefaultLogScope = "_default_logscope_"
)

var DefaultClusterManagerAddresses = []string{"localhost:2379"}

type Config struct {
	ProcessingEnabled   bool
	LevelManagerEnabled bool

	NodeID           int
	ClusterAddresses []string  `name:"cluster-addresses"`
	ClusterTlsConfig TLSConfig `embed:"" prefix:"cluster-tls-"`

	// Level-manager config
	ExternalLevelManagerAddresses      []string  `name:"external-level-manager-addresses"`
	ExternalLevelManagerTlsConfig      TLSConfig `embed:"" prefix:"external-level-manager-tls-"`
	ClusterName                        string
	RegistryFormat                     common.MetadataFormat
	MasterRegistryRecordID             string
	MaxRegistrySegmentTableEntries     int
	LevelManagerFlushInterval          time.Duration
	SegmentCacheMaxSize                int
	L0CompactionTrigger                int `name:"l0-compaction-trigger"`
	L0MaxTablesBeforeBlocking          int `name:"l0-max-tables-before-blocking"`
	L1CompactionTrigger                int `name:"l1-compaction-trigger"`
	LevelMultiplier                    int
	CompactionPollerTimeout            time.Duration
	CompactionJobTimeout               time.Duration
	SSTableDeleteCheckInterval         time.Duration
	SSTableDeleteDelay                 time.Duration
	LevelManagerRetryDelay             time.Duration
	SSTableRegisterRetryDelay          time.Duration
	PrefixRetentionRemoveCheckInterval time.Duration
	CompactionMaxSSTableSize           int

	// Table-cache config
	TableCacheMaxSizeBytes  parseableInt
	TableCacheSSTableMaxAge time.Duration `name:"table-cache-sstable-max-age"`

	// Compaction worker config
	CompactionWorkersEnabled bool
	CompactionWorkerCount    int
	SSTablePushRetryDelay    time.Duration

	PrefixRetentionRefreshInterval time.Duration

	// Command manager config
	CommandCompactionInterval time.Duration

	// Cluster-manager config
	ClusterManagerLockTimeout  time.Duration
	ClusterManagerKeyPrefix    string
	ClusterManagerAddresses    []string
	ClusterEvictionTimeout     time.Duration
	ClusterStateUpdateInterval time.Duration
	EtcdCallTimeout            time.Duration

	// Sequence manager config
	SequencesObjectName string
	SequencesRetryDelay time.Duration

	// Object store config
	ObjectStoreType         string
	DevObjectStoreAddresses []string

	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	MinioBucketName string
	MinioSecure     bool

	// store/processor config
	ProcessorCount                 int
	MaxProcessorBatchesInProgress  int
	MemtableMaxSizeBytes           parseableInt
	MemtableMaxReplaceInterval     time.Duration
	MemtableFlushQueueMaxSize      int
	StoreWriteBlockedRetryInterval time.Duration
	TableFormat                    common.DataFormat
	MinReplicas                    int
	MaxReplicas                    int
	MinSnapshotInterval            time.Duration
	IdleProcessorCheckInterval     time.Duration
	BatchFlushCheckInterval        time.Duration
	ConsumerRetryInterval          time.Duration

	MaxBackfillBatchSize int
	ForwardResendDelay   time.Duration

	// query manager config
	QueryMaxBatchRows int

	// Http-API config
	HttpApiEnabled   bool      `name:"http-api-enabled"`
	HttpApiAddresses []string  `name:"http-api-addresses"`
	HttpApiTlsConfig TLSConfig `embed:"" prefix:"http-api-tls-"`
	HttpApiPath      string    `name:"http-api-path"`

	// Admin console config
	AdminConsoleEnabled        bool
	AdminConsoleAddresses      []string  `name:"admin-console-addresses"`
	AdminConsoleTLSConfig      TLSConfig `embed:"" prefix:"admin-console-tls-"`
	AdminConsoleSampleInterval time.Duration

	// Kafka protocol config
	KafkaServerEnabled          bool           `name:"kafka-server-enabled"`
	KafkaServerListenerConfig   ListenerConfig `embed:"" prefix:"kafka-server-listener-"`
	KafkaUseServerTimestamp     bool
	KafkaMinSessionTimeout      time.Duration
	KafkaMaxSessionTimeout      time.Duration
	KafkaInitialJoinDelay       time.Duration
	KafkaNewMemberJoinTimeout   time.Duration
	KafkaFetchCacheMaxSizeBytes parseableInt

	LifeCycleEndpointEnabled bool
	LifeCycleAddress         string
	StartupEndpointPath      string
	ReadyEndpointPath        string
	LiveEndpointPath         string
	MetricsBind              string `help:"Bind address for Prometheus metrics." default:"localhost:9102" env:"METRICS_BIND"`
	MetricsEnabled           bool

	// Version manager config
	VersionCompletedBroadcastInterval  time.Duration
	VersionManagerStoreFlushedInterval time.Duration

	// Wasm module manager config
	WasmModuleInstances int

	// Datadog profiling
	DDProfilerTypes           string
	DDProfilerHostEnvVarName  string
	DDProfilerPort            int
	DDProfilerServiceName     string
	DDProfilerEnvironmentName string
	DDProfilerVersionName     string

	SourceStatsEnabled bool

	// Profiling
	MemProfileEnabled bool
	CPUProfileEnabled bool

	// Testing only
	ClientType      KafkaClientType
	FailureDisabled bool

	// Debug server
	DebugServerEnabled   bool
	DebugServerAddresses []string

	Original string
	LogScope string
}

type KafkaClientType int

const (
	KafkaClientTypeConfluent = 1
)

type parseableInt int

// UnmarshalText Kong uses default Json Unmrashalling which unmarshalls numbers as float64 which can result in loss of precision
// or failure to parse - this ensures large int fields are parsed correctly
// the field needs to be quoted as a string in the config
func (p *parseableInt) UnmarshalText(text []byte) error {
	s := string(text)
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*p = parseableInt(i)
	return nil
}

type TLSConfig struct {
	Enabled         bool   `help:"Set to true to enable TLS. TLS must be enabled when using the HTTP 2 API" default:"false"`
	KeyPath         string `help:"Path to a PEM encoded file containing the server private key"`
	CertPath        string `help:"Path to a PEM encoded file containing the server certificate"`
	ClientCertsPath string `help:"Path to a PEM encoded file containing trusted client certificates and/or CA certificates. Only needed with TLS client authentication"`
	ClientAuth      string `help:"Client certificate authentication mode. One of: no-client-cert, request-client-cert, require-any-client-cert, verify-client-cert-if-given, require-and-verify-client-cert"`
}

type ListenerConfig struct {
	Addresses           []string
	AdvertisedAddresses []string
	TLSConfig           TLSConfig `embed:"" prefix:"tls-"`
	AuthenticationType  string
}

type ClientAuthMode string

const (
	ClientAuthModeUnspecified                = ""
	ClientAuthModeNoClientCert               = "no-client-cert"
	ClientAuthModeRequestClientCert          = "request-client-cert"
	ClientAuthModeRequireAnyClientCert       = "require-any-client-cert"
	ClientAuthModeVerifyClientCertIfGiven    = "verify-client-cert-if-given"
	ClientAuthModeRequireAndVerifyClientCert = "require-and-verify-client-cert"
)

func (c *Config) ApplyDefaults() {
	if c.HttpApiEnabled {
		c.HttpApiTlsConfig.Enabled = true
	}
	if c.ProcessorCount == 0 {
		c.ProcessorCount = DefaultProcessorCount
	}
	if c.MaxProcessorBatchesInProgress == 0 {
		c.MaxProcessorBatchesInProgress = DefaultMaxProcessorBatchesInProgress
	}
	if c.MemtableMaxSizeBytes == 0 {
		c.MemtableMaxSizeBytes = DefaultMemtableMaxSizeBytes
	}
	if c.MemtableMaxReplaceInterval == 0 {
		c.MemtableMaxReplaceInterval = DefaultMemtableMaxReplaceInterval
	}
	if c.MemtableFlushQueueMaxSize == 0 {
		c.MemtableFlushQueueMaxSize = DefaultMemtableFlushQueueMaxSize
	}
	if c.StoreWriteBlockedRetryInterval == 0 {
		c.StoreWriteBlockedRetryInterval = DefaultStoreWriteBlockedRetryInterval
	}
	if c.MinReplicas == 0 {
		c.MinReplicas = DefaultMinReplicas
	}
	if c.MaxReplicas == 0 {
		c.MaxReplicas = DefaultMaxReplicas
	}
	if c.TableFormat == 0 {
		c.TableFormat = DefaultTableFormat
	}
	if c.MinSnapshotInterval == 0 {
		c.MinSnapshotInterval = DefaultMinSnapshotInterval
	}
	if c.IdleProcessorCheckInterval == 0 {
		c.IdleProcessorCheckInterval = DefaultIdleProcessorCheckInterval
	}
	if c.BatchFlushCheckInterval == 0 {
		c.BatchFlushCheckInterval = DefaultBatchFlushCheckInterval
	}
	if c.ConsumerRetryInterval == 0 {
		c.ConsumerRetryInterval = DefaultConsumerRetryInterval
	}
	if c.MaxBackfillBatchSize == 0 {
		c.MaxBackfillBatchSize = DefaultMaxBackfillBatchSize
	}
	if c.ForwardResendDelay == 0 {
		c.ForwardResendDelay = DefaultForwardResendDelay
	}

	if c.HttpApiPath == "" {
		c.HttpApiPath = DefaultHTTPAPIServerPath
	}

	if c.LevelManagerFlushInterval == 0 {
		c.LevelManagerFlushInterval = DefaultLevelManagerFlushInterval
	}
	if c.MasterRegistryRecordID == "" {
		c.MasterRegistryRecordID = DefaultMasterRecordRegistryID
	}
	if c.MaxRegistrySegmentTableEntries == 0 {
		c.MaxRegistrySegmentTableEntries = DefaultMaxRegistrySegmentTableEntries
	}
	if c.RegistryFormat == 0 {
		c.RegistryFormat = DefaultRegistryFormat
	}
	if c.SegmentCacheMaxSize == 0 {
		c.SegmentCacheMaxSize = DefaultSegmentCacheMaxSize
	}
	if c.LevelManagerRetryDelay == 0 {
		c.LevelManagerRetryDelay = DefaultLevelManagerRetryDelay
	}
	if c.L0CompactionTrigger == 0 {
		c.L0CompactionTrigger = DefaultL0CompactionTrigger
	}
	if c.L0MaxTablesBeforeBlocking == 0 {
		c.L0MaxTablesBeforeBlocking = DefaultL0MaxTablesBeforeBlocking
	}
	if c.L1CompactionTrigger == 0 {
		c.L1CompactionTrigger = DefaultL1CompactionTrigger
	}
	if c.LevelMultiplier == 0 {
		c.LevelMultiplier = DefaultLevelMultiplier
	}
	if c.CompactionPollerTimeout == 0 {
		c.CompactionPollerTimeout = DefaultCompactionPollerTimeout
	}
	if c.CompactionJobTimeout == 0 {
		c.CompactionJobTimeout = DefaultCompactionJobTimeout
	}
	if c.SSTableDeleteDelay == 0 {
		c.SSTableDeleteDelay = DefaultSSTableDeleteDelay
	}
	if c.SSTableDeleteCheckInterval == 0 {
		c.SSTableDeleteCheckInterval = DefaultSSTableDeleteCheckInterval
	}
	if c.SSTableRegisterRetryDelay == 0 {
		c.SSTableRegisterRetryDelay = DefaultSSTableRegisterRetryDelay
	}
	if c.CompactionMaxSSTableSize == 0 {
		c.CompactionMaxSSTableSize = DefaultCompactionMaxSSTableSize
	}

	if c.PrefixRetentionRemoveCheckInterval == 0 {
		c.PrefixRetentionRemoveCheckInterval = DefaultPrefixRetentionRemoveCheckInterval
	}

	if c.CompactionWorkerCount == 0 {
		c.CompactionWorkerCount = DefaultCompactionWorkerCount
	}
	if c.PrefixRetentionRefreshInterval == 0 {
		c.PrefixRetentionRefreshInterval = DefaultPrefixRetentionRefreshInterval
	}

	if c.TableCacheMaxSizeBytes == 0 {
		c.TableCacheMaxSizeBytes = DefaultTableCacheMaxSizeBytes
	}

	if c.TableCacheSSTableMaxAge == 0 {
		c.TableCacheSSTableMaxAge = DefaultTableCacheSSTableMaxAge
	}

	if c.ClusterName == "" {
		c.ClusterName = DefaultClusterName
	}
	if c.SequencesObjectName == "" {
		c.SequencesObjectName = DefaultSequencesObjectName
	}
	if c.SequencesRetryDelay == 0 {
		c.SequencesRetryDelay = DefaultSequencesRetryDelay
	}
	if c.ClusterManagerLockTimeout == 0 {
		c.ClusterManagerLockTimeout = DefaultClusterManagerLockTimeout
	}
	if c.ClusterManagerKeyPrefix == "" {
		c.ClusterManagerKeyPrefix = DefaultClusterManagerKeyPrefix
	}
	if c.ClusterEvictionTimeout == 0 {
		c.ClusterEvictionTimeout = DefaultClusterEvictionTimeout
	}
	if c.ClusterStateUpdateInterval == 0 {
		c.ClusterStateUpdateInterval = DefaultClusterStateUpdateInterval
	}
	if len(c.ClusterManagerAddresses) == 0 {
		c.ClusterManagerAddresses = DefaultClusterManagerAddresses
	}

	if c.QueryMaxBatchRows == 0 {
		c.QueryMaxBatchRows = DefaultQueryMaxBatchRows
	}

	if c.VersionCompletedBroadcastInterval == 0 {
		c.VersionCompletedBroadcastInterval = DefaultVersionCompletedBroadcastInterval
	}
	if c.VersionManagerStoreFlushedInterval == 0 {
		c.VersionManagerStoreFlushedInterval = DefaultVersionManagerStoreFlushedInterval
	}

	if c.ClientType == 0 {
		c.ClientType = KafkaClientTypeConfluent
	}

	if c.KafkaInitialJoinDelay == 0 {
		c.KafkaInitialJoinDelay = DefaultKafkaInitialJoinDelay
	}
	if c.KafkaMinSessionTimeout == 0 {
		c.KafkaMinSessionTimeout = DefaultKafkaMinSessionTimeout
	}
	if c.KafkaMaxSessionTimeout == 0 {
		c.KafkaMaxSessionTimeout = DefaultKafkaMaxSessionTimeout
	}
	if c.KafkaNewMemberJoinTimeout == 0 {
		c.KafkaNewMemberJoinTimeout = DefaultKafkaNewMemberJoinTimeout
	}
	if c.KafkaFetchCacheMaxSizeBytes == 0 {
		c.KafkaFetchCacheMaxSizeBytes = DefaultKafkaFetchCacheMaxSizeBytes
	}

	if c.SSTablePushRetryDelay == 0 {
		c.SSTablePushRetryDelay = DefaultSSTablePushRetryDelay
	}

	if c.CommandCompactionInterval == 0 {
		c.CommandCompactionInterval = DefaultCommandCompactionInterval
	}

	if c.EtcdCallTimeout == 0 {
		c.EtcdCallTimeout = DefaultEtcdCallTimeout
	}

	if c.WasmModuleInstances == 0 {
		c.WasmModuleInstances = DefaultWasmModuleInstances
	}

	if c.LogScope == "" {
		c.LogScope = DefaultLogScope
	}
}

func invalidConfigurationError(errMsg string) error {
	return common.NewTektiteErrorf(common.InvalidConfiguration, "invalid configuration: %s", errMsg)
}

func (c *Config) Validate() error { //nolint:gocyclo
	if c.NodeID < 0 {
		return invalidConfigurationError("node-id must be >= 0")
	}
	if c.NodeID >= len(c.ClusterAddresses) {
		return invalidConfigurationError("node-id must be >= 0 and < length cluster-addresses")
	}
	if c.HttpApiEnabled {
		if len(c.HttpApiAddresses) == 0 {
			return invalidConfigurationError("http-api-addresses must be specified")
		}
		if !c.HttpApiTlsConfig.Enabled {
			return invalidConfigurationError("http-api-tls-enabled must be true if http-api-enabled is true")
		}
		if c.HttpApiTlsConfig.CertPath == "" {
			return invalidConfigurationError("http-api-tls-cert-path must be specified for HTTP API server")
		}
		if c.HttpApiTlsConfig.KeyPath == "" {
			return invalidConfigurationError("http-api-tls-key-path must be specified for HTTP API server")
		}
		if c.HttpApiTlsConfig.ClientAuth != ClientAuthModeNoClientCert && c.HttpApiTlsConfig.ClientAuth != "" && c.HttpApiTlsConfig.ClientCertsPath == "" {
			return invalidConfigurationError("http-api-tls-client-certs-path must be provided if client auth is enabled")
		}
	}
	if c.AdminConsoleEnabled {
		if len(c.AdminConsoleAddresses) == 0 {
			return invalidConfigurationError("admin-console-addresses must be specified")
		}
		if c.AdminConsoleSampleInterval == 0 {
			c.AdminConsoleSampleInterval = DefaultWebUISampleInterval
		}
	}
	if c.LifeCycleEndpointEnabled {
		if c.LifeCycleAddress == "" {
			return invalidConfigurationError("life-cycle-address must be specified")
		}
		if c.StartupEndpointPath == "" {
			return invalidConfigurationError("startup-endpoint-path must be specified")
		}
		if c.LiveEndpointPath == "" {
			return invalidConfigurationError("live-endpoint-path must be specified")
		}
		if c.ReadyEndpointPath == "" {
			return invalidConfigurationError("ready-endpoint-path must be specified")
		}
	}
	if c.ClusterTlsConfig.Enabled {
		if c.ClusterTlsConfig.CertPath == "" {
			return invalidConfigurationError("cluster-tls-cert-path must be specified if cluster-tls-enabled is true")
		}
		if c.ClusterTlsConfig.KeyPath == "" {
			return invalidConfigurationError("cluster-tls-key-path must be specified if cluster-tls-enabled is true")
		}
		if c.ClusterTlsConfig.ClientCertsPath == "" {
			return invalidConfigurationError("cluster-tls-client-certs-path must be specified if cluster-tls-enabled is true")
		}
	}
	if c.ClusterName == "" {
		return invalidConfigurationError("cluster-name must be specified")
	}
	if c.ProcessingEnabled && c.ProcessorCount < 1 {
		return invalidConfigurationError("processor-count must be > 0")
	}
	if c.ProcessorCount < 0 {
		return invalidConfigurationError("processor-count must be >= 0")
	}
	if c.MaxBackfillBatchSize < 1 {
		return invalidConfigurationError("max-backfill-batch-size must be > 0")
	}

	if c.MaxProcessorBatchesInProgress < 1 {
		return invalidConfigurationError("max-processor-batches-in-progress must be > 0")
	}
	if c.MinSnapshotInterval < 1*time.Millisecond {
		return invalidConfigurationError("min-snapshot-interval must be >= 1 millisecond")
	}
	if c.IdleProcessorCheckInterval < 1*time.Millisecond {
		return invalidConfigurationError("idle-processor-check-interval must be >= 1 millisecond")
	}
	if c.MemtableMaxSizeBytes < 1 {
		return invalidConfigurationError("memtable-max-size-bytes must be > 0")
	}
	if c.MemtableMaxReplaceInterval < 1*time.Millisecond {
		return invalidConfigurationError("memtable-max-replace-time must be >= 1 ms")
	}
	if c.MemtableFlushQueueMaxSize < 1 {
		return invalidConfigurationError("memtable-flush-queue-max-size must be > 0")
	}
	if c.MinReplicas < 1 {
		return invalidConfigurationError("min-replicas must be > 0")
	}
	if c.MaxReplicas < 1 {
		return invalidConfigurationError("max-replicas must be > 0")
	}
	if c.MinReplicas > c.MaxReplicas {
		return invalidConfigurationError("min-replicas must be <= max-replicas")
	}
	if c.TableFormat != common.DataFormatV1 {
		return invalidConfigurationError("table-format must be specified")
	}
	if c.LevelManagerFlushInterval < 1*time.Millisecond {
		return invalidConfigurationError("level-manager-flush-interval must be >= 1ms")
	}
	if c.SegmentCacheMaxSize < 0 {
		return invalidConfigurationError("segment-cache-max-size must be >= 0")
	}
	if c.DevObjectStoreAddresses == nil {
		c.DevObjectStoreAddresses = []string{DefaultDevObjectStoreAddress}
	}
	if c.ObjectStoreType == "" {
		c.ObjectStoreType = DevObjectStoreType
	}
	if c.ClusterManagerLockTimeout < 1*time.Millisecond {
		return invalidConfigurationError("cluster-manager-lock-timeout must be >= 1ms")
	}
	if c.ClusterManagerKeyPrefix == "" {
		return invalidConfigurationError("cluster-manager-key-prefix must be specified")
	}
	if len(c.ClusterManagerAddresses) == 0 {
		return invalidConfigurationError("cluster-manager-addresses must be specified")
	}
	if c.ClusterEvictionTimeout < 1*time.Second {
		return invalidConfigurationError("cluster-eviction-timeout must be >= 1s")
	}
	if c.ClusterStateUpdateInterval < 1*time.Millisecond {
		return invalidConfigurationError("cluster-state-update-interval must be >= 1ms")
	}

	if c.KafkaInitialJoinDelay < 0 {
		return invalidConfigurationError("kafka-initial-join-delay must be >= 0")
	}
	if c.KafkaMinSessionTimeout < 0 {
		return invalidConfigurationError("kafka-min-session-timeout must be >= 0")
	}
	if c.KafkaMaxSessionTimeout < 0 {
		return invalidConfigurationError("kafka-max-session-timeout must be >= 0")
	}
	if c.KafkaMaxSessionTimeout <= c.KafkaMinSessionTimeout {
		return invalidConfigurationError("kafka-max-session-timeout must be > kafka-min-session-timeout")
	}
	if len(c.KafkaServerListenerConfig.AdvertisedAddresses) > 0 && len(c.KafkaServerListenerConfig.AdvertisedAddresses) != len(c.KafkaServerListenerConfig.Addresses) {
		return invalidConfigurationError("kafka-server-listener-advertised-addresses must be the same length as kafka-server-listener-addresses")
	}
	if len(c.KafkaServerListenerConfig.Addresses) > 0 {
		for _, addr := range c.KafkaServerListenerConfig.Addresses {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return invalidConfigurationError(fmt.Sprintf("error when parsing address %s for kafka-server-listener-addresses. %s", addr, err))
			}
			if host == "0.0.0.0" {
				return invalidConfigurationError("0.0.0.0 is not a valid address for kafka-server-listener-addresses")
			} else if host == "" {
				return invalidConfigurationError(fmt.Sprintf("invalid address %s for kafka-server-listener-addresses. IP address is required", addr))
			} else if _, err := strconv.Atoi(port); err != nil {
				return invalidConfigurationError(fmt.Sprintf("invalid port %s for kafka-server-listener-addresses. Port must be a number", port))
			}
		}
	}
	if len(c.KafkaServerListenerConfig.AdvertisedAddresses) > 0 {
		for _, addr := range c.KafkaServerListenerConfig.AdvertisedAddresses {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return invalidConfigurationError(fmt.Sprintf("error when parsing address %s for kafka-server-listener-advertised-addresses. %s", addr, err))
			}
			if host == "0.0.0.0" {
				return invalidConfigurationError("0.0.0.0 is not a valid address for kafka-server-listener-advertised-addresses")
			} else if host == "" {
				return invalidConfigurationError(fmt.Sprintf("invalid address %s for kafka-server-listener-advertised-addresses. IP address is required", addr))
			} else if _, err := strconv.Atoi(port); err != nil {
				return invalidConfigurationError(fmt.Sprintf("invalid port %s for kafka-server-listener-advertised-addresses. Port must be a number", port))
			}
		}
	}
	if c.TableCacheSSTableMaxAge < 1*time.Millisecond {
		return invalidConfigurationError("table-cache-sstable-max-age must be >= 1ms")
	}
	return nil
}
