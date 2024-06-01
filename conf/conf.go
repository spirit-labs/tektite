package conf

import (
	"github.com/spirit-labs/tektite/common"
	"strconv"
	"time"

	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/types"
)

const (
	DefaultProcessorCount                 = 48
	DefaultMaxProcessorBatchesInProgress  = 1000
	DefaultMemtableMaxSizeBytes           = 16 * 1024 * 1024
	DefaultMemtableMaxReplaceInterval     = 30 * time.Second
	DefaultMemtableFlushQueueMaxSize      = 10
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

	DefaultTableCacheMaxSizeBytes = 128 * 1024 * 1024

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

	DefaultFailureDisabled = false

	DefaultMetricsEnabled = false
)

var DefaultClusterManagerAddresses = []string{"localhost:2379"}

type Config struct {
	ProcessingEnabled   *bool
	LevelManagerEnabled *bool

	NodeID           *int
	ClusterAddresses []string  `name:"cluster-addresses"`
	ClusterTlsConfig TLSConfig `embed:"" prefix:"cluster-tls-"`

	// Level-manager config
	ExternalLevelManagerAddresses      []string  `name:"external-level-manager-addresses"`
	ExternalLevelManagerTlsConfig      TLSConfig `embed:"" prefix:"external-level-manager-tls-"`
	ClusterName                        *string
	RegistryFormat                     *common.MetadataFormat
	MasterRegistryRecordID             *string
	MaxRegistrySegmentTableEntries     *int
	LevelManagerFlushInterval          *time.Duration
	SegmentCacheMaxSize                *int
	L0CompactionTrigger                *int `name:"l0-compaction-trigger"`
	L0MaxTablesBeforeBlocking          *int `name:"l0-max-tables-before-blocking"`
	L1CompactionTrigger                *int `name:"l1-compaction-trigger"`
	LevelMultiplier                    *int
	CompactionPollerTimeout            *time.Duration
	CompactionJobTimeout               *time.Duration
	SSTableDeleteCheckInterval         *time.Duration
	SSTableDeleteDelay                 *time.Duration
	LevelManagerRetryDelay             *time.Duration
	SSTableRegisterRetryDelay          *time.Duration
	PrefixRetentionRemoveCheckInterval *time.Duration
	CompactionMaxSSTableSize           *int

	// Table-cache config
	TableCacheMaxSizeBytes *ParseableInt

	// Compaction worker config
	CompactionWorkersEnabled *bool
	CompactionWorkerCount    *int
	SSTablePushRetryDelay    *time.Duration

	PrefixRetentionRefreshInterval *time.Duration

	// Command manager config
	CommandCompactionInterval *time.Duration

	// Cluster-manager config
	ClusterManagerLockTimeout  *time.Duration
	ClusterManagerKeyPrefix    *string
	ClusterManagerAddresses    []string
	ClusterEvictionTimeout     *time.Duration
	ClusterStateUpdateInterval *time.Duration
	EtcdCallTimeout            *time.Duration

	// Sequence manager config
	SequencesObjectName *string
	SequencesRetryDelay *time.Duration

	// Object store config
	ObjectStoreType         *string
	DevObjectStoreAddresses []string

	MinioEndpoint   *string
	MinioAccessKey  *string
	MinioSecretKey  *string
	MinioBucketName *string
	MinioSecure     *bool

	// store/processor config
	ProcessorCount                 *int
	MaxProcessorBatchesInProgress  *int
	MemtableMaxSizeBytes           *ParseableInt
	MemtableMaxReplaceInterval     *time.Duration
	MemtableFlushQueueMaxSize      *int
	StoreWriteBlockedRetryInterval *time.Duration
	TableFormat                    *common.DataFormat
	MinReplicas                    *int
	MaxReplicas                    *int
	MinSnapshotInterval            *time.Duration
	IdleProcessorCheckInterval     *time.Duration
	BatchFlushCheckInterval        *time.Duration
	ConsumerRetryInterval          *time.Duration

	MaxBackfillBatchSize *int
	ForwardResendDelay   *time.Duration

	// query manager config
	QueryMaxBatchRows *int

	// Http-API config
	HttpApiEnabled   *bool      `name:"http-api-enabled"`
	HttpApiAddresses []string   `name:"http-api-addresses"`
	HttpApiTlsConfig *TLSConfig `embed:"" prefix:"http-api-tls-"`
	HttpApiPath      *string    `name:"http-api-path"`

	// Admin console config
	AdminConsoleEnabled        *bool
	AdminConsoleAddresses      []string  `name:"admin-console-addresses"`
	AdminConsoleTLSConfig      TLSConfig `embed:"" prefix:"admin-console-tls-"`
	AdminConsoleSampleInterval *time.Duration

	// Kafka protocol config
	KafkaServerEnabled          *bool      `name:"kafka-server-enabled"`
	KafkaServerAddresses        []string   `name:"kafka-server-addresses"`
	KafkaServerTLSConfig        *TLSConfig `embed:"" prefix:"kafka-server-tls-"`
	KafkaUseServerTimestamp     *bool
	KafkaMinSessionTimeout      *time.Duration
	KafkaMaxSessionTimeout      *time.Duration
	KafkaInitialJoinDelay       *time.Duration
	KafkaNewMemberJoinTimeout   *time.Duration
	KafkaFetchCacheMaxSizeBytes *ParseableInt

	LifeCycleEndpointEnabled *bool
	LifeCycleAddress         *string
	StartupEndpointPath      *string
	ReadyEndpointPath        *string
	LiveEndpointPath         *string
	MetricsBind              *string `help:"Bind address for Prometheus metrics." default:"localhost:9102" env:"METRICS_BIND"`
	MetricsEnabled           *bool

	// Version manager config
	VersionCompletedBroadcastInterval  *time.Duration
	VersionManagerStoreFlushedInterval *time.Duration

	// Wasm module manager config
	WasmModuleInstances *int

	// Datadog profiling
	DDProfilerTypes           *string
	DDProfilerHostEnvVarName  *string
	DDProfilerPort            *int
	DDProfilerServiceName     *string
	DDProfilerEnvironmentName *string
	DDProfilerVersionName     *string

	SourceStatsEnabled *bool

	// Profiling
	MemProfileEnabled *bool
	CPUProfileEnabled *bool

	// Testing only
	ClientType      *KafkaClientType
	FailureDisabled *bool

	// Debug server
	DebugServerEnabled   *bool
	DebugServerAddresses []string

	Original *string
}

type KafkaClientType int

const (
	KafkaClientTypeConfluent = 1
	KafkaClientTypeLoad      = 2
)

type ParseableInt int

// UnmarshalText Kong uses default Json Unmrashalling which unmarshalls numbers as float64 which can result in loss of precision
// or failure to parse - this ensures large int fields are parsed correctly
// the field needs to be quoted as a string in the config
func (p *ParseableInt) UnmarshalText(text []byte) error {
	s := string(text)
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*p = ParseableInt(i)
	return nil
}

type TLSConfig struct {
	Enabled         bool   `help:"Set to true to enable TLS. TLS must be enabled when using the HTTP 2 API" default:"false"`
	KeyPath         string `help:"Path to a PEM encoded file containing the server private key"`
	CertPath        string `help:"Path to a PEM encoded file containing the server certificate"`
	ClientCertsPath string `help:"Path to a PEM encoded file containing trusted client certificates and/or CA certificates. Only needed with TLS client authentication"`
	ClientAuth      string `help:"Client certificate authentication mode. One of: no-client-cert, request-client-cert, require-any-client-cert, verify-client-cert-if-given, require-and-verify-client-cert"`
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
	if c.NodeID == nil {
		c.NodeID = types.AddressOf(0)
	}

	if c.AdminConsoleEnabled == nil {
		c.AdminConsoleEnabled = types.AddressOf(false)
	}

	if c.AdminConsoleSampleInterval == nil {
		c.AdminConsoleSampleInterval = types.AddressOf(DefaultWebUISampleInterval)
	}

	if c.HttpApiEnabled != nil && *c.HttpApiEnabled {
		c.HttpApiTlsConfig.Enabled = true
	}
	if c.ProcessorCount == nil || (c.ProcessorCount != nil && *c.ProcessorCount == 0) {
		c.ProcessorCount = types.AddressOf(DefaultProcessorCount)
	}
	if c.MaxProcessorBatchesInProgress == nil {
		c.MaxProcessorBatchesInProgress = types.AddressOf(DefaultMaxProcessorBatchesInProgress)
	}
	if c.MemtableMaxSizeBytes == nil {
		c.MemtableMaxSizeBytes = (*ParseableInt)(types.AddressOf(DefaultMemtableMaxSizeBytes))
	}
	if c.MemtableMaxReplaceInterval == nil {
		c.MemtableMaxReplaceInterval = types.AddressOf(DefaultMemtableMaxReplaceInterval)
	}
	if c.MemtableFlushQueueMaxSize == nil {
		c.MemtableFlushQueueMaxSize = types.AddressOf(DefaultMemtableFlushQueueMaxSize)
	}
	if c.StoreWriteBlockedRetryInterval == nil {
		c.StoreWriteBlockedRetryInterval = types.AddressOf(DefaultStoreWriteBlockedRetryInterval)
	}
	if c.MinReplicas == nil || (c.MinReplicas != nil && *c.MinReplicas == 0) {
		c.MinReplicas = types.AddressOf(DefaultMinReplicas)
	}
	if c.MaxReplicas == nil || (c.MaxReplicas != nil && *c.MaxReplicas == 0) {
		c.MaxReplicas = types.AddressOf(DefaultMaxReplicas)
	}
	if c.TableFormat == nil || (c.TableFormat != nil && *c.TableFormat == 0) {
		c.TableFormat = types.AddressOf(DefaultTableFormat)
	}
	if c.MinSnapshotInterval == nil || (c.MinSnapshotInterval != nil && *c.MinSnapshotInterval == 0) {
		c.MinSnapshotInterval = types.AddressOf(DefaultMinSnapshotInterval)
	}
	if c.IdleProcessorCheckInterval == nil || (c.IdleProcessorCheckInterval != nil && *c.IdleProcessorCheckInterval == 0) {
		c.IdleProcessorCheckInterval = types.AddressOf(DefaultIdleProcessorCheckInterval)
	}
	if c.BatchFlushCheckInterval == nil {
		c.BatchFlushCheckInterval = types.AddressOf(DefaultBatchFlushCheckInterval)
	}
	if c.ConsumerRetryInterval == nil {
		c.ConsumerRetryInterval = types.AddressOf(DefaultConsumerRetryInterval)
	}
	if c.MaxBackfillBatchSize == nil {
		c.MaxBackfillBatchSize = types.AddressOf(DefaultMaxBackfillBatchSize)
	}
	if c.ForwardResendDelay == nil {
		c.ForwardResendDelay = types.AddressOf(DefaultForwardResendDelay)
	}

	if c.HttpApiPath == nil || (c.HttpApiPath != nil && *c.HttpApiPath == "") {
		c.HttpApiPath = types.AddressOf(DefaultHTTPAPIServerPath)
	}

	if c.LevelManagerFlushInterval == nil {
		c.LevelManagerFlushInterval = types.AddressOf(DefaultLevelManagerFlushInterval)
	}
	if c.MasterRegistryRecordID == nil {
		c.MasterRegistryRecordID = types.AddressOf(DefaultMasterRecordRegistryID)
	}
	if c.MaxRegistrySegmentTableEntries == nil {
		c.MaxRegistrySegmentTableEntries = types.AddressOf(DefaultMaxRegistrySegmentTableEntries)
	}
	if c.RegistryFormat == nil {
		c.RegistryFormat = types.AddressOf(DefaultRegistryFormat)
	}
	if c.SegmentCacheMaxSize == nil {
		c.SegmentCacheMaxSize = types.AddressOf(DefaultSegmentCacheMaxSize)
	}
	if c.LevelManagerRetryDelay == nil {
		c.LevelManagerRetryDelay = types.AddressOf(DefaultLevelManagerRetryDelay)
	}
	if c.L0CompactionTrigger == nil {
		c.L0CompactionTrigger = types.AddressOf(DefaultL0CompactionTrigger)
	}
	if c.L0MaxTablesBeforeBlocking == nil {
		c.L0MaxTablesBeforeBlocking = types.AddressOf(DefaultL0MaxTablesBeforeBlocking)
	}
	if c.L1CompactionTrigger == nil {
		c.L1CompactionTrigger = types.AddressOf(DefaultL1CompactionTrigger)
	}
	if c.LevelMultiplier == nil || (c.LevelMultiplier != nil && *c.LevelMultiplier == 0) {
		c.LevelMultiplier = types.AddressOf(DefaultLevelMultiplier)
	}
	if c.CompactionPollerTimeout == nil {
		c.CompactionPollerTimeout = types.AddressOf(DefaultCompactionPollerTimeout)
	}
	if c.CompactionJobTimeout == nil {
		c.CompactionJobTimeout = types.AddressOf(DefaultCompactionJobTimeout)
	}
	if c.SSTableDeleteDelay == nil {
		c.SSTableDeleteDelay = types.AddressOf(DefaultSSTableDeleteDelay)
	}
	if c.SSTableDeleteCheckInterval == nil {
		c.SSTableDeleteCheckInterval = types.AddressOf(DefaultSSTableDeleteCheckInterval)
	}
	if c.SSTableRegisterRetryDelay == nil {
		c.SSTableRegisterRetryDelay = types.AddressOf(DefaultSSTableRegisterRetryDelay)
	}
	if c.CompactionMaxSSTableSize == nil {
		c.CompactionMaxSSTableSize = types.AddressOf(DefaultCompactionMaxSSTableSize)
	}

	if c.PrefixRetentionRemoveCheckInterval == nil || (c.PrefixRetentionRemoveCheckInterval != nil && *c.PrefixRetentionRemoveCheckInterval == 0) {
		c.PrefixRetentionRemoveCheckInterval = (*time.Duration)(types.AddressOf(DefaultPrefixRetentionRemoveCheckInterval))
	}

	if c.CompactionWorkerCount == nil || (c.CompactionWorkerCount != nil && *c.CompactionWorkerCount == 0) {
		c.CompactionWorkerCount = types.AddressOf(DefaultCompactionWorkerCount)
	}
	if c.PrefixRetentionRefreshInterval == nil || (c.PrefixRetentionRefreshInterval != nil && *c.PrefixRetentionRefreshInterval == 0) {
		c.PrefixRetentionRefreshInterval = types.AddressOf(DefaultPrefixRetentionRefreshInterval)
	}

	if c.TableCacheMaxSizeBytes == nil || (c.TableCacheMaxSizeBytes != nil && *c.TableCacheMaxSizeBytes == 0) {
		c.TableCacheMaxSizeBytes = (*ParseableInt)(types.AddressOf(DefaultTableCacheMaxSizeBytes))
	}

	if (c.ClusterName != nil && *c.ClusterName == "") || c.ClusterName == nil {
		c.ClusterName = types.AddressOf(DefaultClusterName)
	}
	if c.SequencesObjectName == nil || (c.SequencesObjectName != nil && *c.SequencesObjectName == "") {
		c.SequencesObjectName = types.AddressOf(DefaultSequencesObjectName)
	}
	if c.SequencesRetryDelay == nil {
		c.SequencesRetryDelay = (*time.Duration)(types.AddressOf(DefaultSequencesRetryDelay))
	}
	if c.ClusterManagerLockTimeout == nil {
		c.ClusterManagerLockTimeout = (*time.Duration)(types.AddressOf(DefaultClusterManagerLockTimeout))
	}
	if c.ClusterManagerKeyPrefix == nil || (c.ClusterManagerKeyPrefix != nil && *c.ClusterManagerKeyPrefix == "") {
		c.ClusterManagerKeyPrefix = types.AddressOf(DefaultClusterManagerKeyPrefix)
	}
	if c.ClusterEvictionTimeout == nil {
		c.ClusterEvictionTimeout = (*time.Duration)(types.AddressOf(DefaultClusterEvictionTimeout))
	}
	if c.ClusterStateUpdateInterval == nil {
		c.ClusterStateUpdateInterval = (*time.Duration)(types.AddressOf(DefaultClusterStateUpdateInterval))
	}
	if len(c.ClusterManagerAddresses) == 0 {
		c.ClusterManagerAddresses = DefaultClusterManagerAddresses
	}

	if c.QueryMaxBatchRows == nil {
		c.QueryMaxBatchRows = types.AddressOf(DefaultQueryMaxBatchRows)
	}

	if c.VersionCompletedBroadcastInterval == nil {
		c.VersionCompletedBroadcastInterval = types.AddressOf(DefaultVersionCompletedBroadcastInterval)
	}
	if c.VersionManagerStoreFlushedInterval == nil {
		c.VersionManagerStoreFlushedInterval = types.AddressOf(DefaultVersionManagerStoreFlushedInterval)
	}

	if c.ClientType == nil || (c.ClientType != nil && *c.ClientType == 0) {
		c.ClientType = (*KafkaClientType)(types.AddressOf(KafkaClientTypeConfluent))
	}

	if c.KafkaInitialJoinDelay == nil {
		c.KafkaInitialJoinDelay = types.AddressOf(DefaultKafkaInitialJoinDelay)
	}
	if c.KafkaMinSessionTimeout == nil {
		c.KafkaMinSessionTimeout = types.AddressOf(DefaultKafkaMinSessionTimeout)
	}
	if c.KafkaMaxSessionTimeout == nil {
		c.KafkaMaxSessionTimeout = types.AddressOf(DefaultKafkaMaxSessionTimeout)
	}
	if c.KafkaNewMemberJoinTimeout == nil {
		c.KafkaNewMemberJoinTimeout = types.AddressOf(DefaultKafkaNewMemberJoinTimeout)
	}
	if c.KafkaFetchCacheMaxSizeBytes == nil {
		c.KafkaFetchCacheMaxSizeBytes = (*ParseableInt)(types.AddressOf(DefaultKafkaFetchCacheMaxSizeBytes))
	}

	if c.SSTablePushRetryDelay == nil {
		c.SSTablePushRetryDelay = types.AddressOf(DefaultSSTablePushRetryDelay)
	}

	if c.CommandCompactionInterval == nil {
		c.CommandCompactionInterval = types.AddressOf(DefaultCommandCompactionInterval)
	}

	if c.EtcdCallTimeout == nil {
		c.EtcdCallTimeout = types.AddressOf(DefaultEtcdCallTimeout)
	}

	if c.WasmModuleInstances == nil {
		c.WasmModuleInstances = types.AddressOf(DefaultWasmModuleInstances)
	}

	if c.FailureDisabled == nil {
		c.FailureDisabled = types.AddressOf(DefaultFailureDisabled)
	}

	if c.MetricsEnabled == nil {
		c.MetricsEnabled = types.AddressOf(DefaultMetricsEnabled)
	}

	if c.DDProfilerTypes == nil {
		c.DDProfilerTypes = types.AddressOf("")
	}

	if c.DebugServerEnabled == nil {
		c.DebugServerEnabled = types.AddressOf(false)
	}

	if c.KafkaServerEnabled == nil {
		c.KafkaServerEnabled = types.AddressOf(false)
	}

	if c.KafkaUseServerTimestamp == nil {
		c.KafkaUseServerTimestamp = types.AddressOf(false)
	}

	if c.SourceStatsEnabled == nil {
		c.SourceStatsEnabled = types.AddressOf(false)
	}

	if c.ProcessingEnabled == nil {
		c.ProcessingEnabled = types.AddressOf(false)
	}

	if c.LevelManagerEnabled == nil {
		c.LevelManagerEnabled = types.AddressOf(false)
	}

	if c.LifeCycleEndpointEnabled == nil {
		c.LifeCycleEndpointEnabled = types.AddressOf(false)
	}
}

func (c *Config) Validate() error { //nolint:gocyclo
	if c.NodeID != nil && *c.NodeID < 0 {
		return errors.NewInvalidConfigurationError("node-id must be >= 0")
	}
	if c.NodeID != nil && *c.NodeID >= len(c.ClusterAddresses) {
		return errors.NewInvalidConfigurationError("node-id must be >= 0 and < length cluster-addresses")
	}
	if *c.HttpApiEnabled {
		if len(c.HttpApiAddresses) == 0 {
			return errors.NewInvalidConfigurationError("http-api-addresses must be specified")
		}
		if c.HttpApiTlsConfig != nil && !c.HttpApiTlsConfig.Enabled {
			return errors.NewInvalidConfigurationError("http-api-tls-enabled must be true if http-api-enabled is true")
		}
		if c.HttpApiTlsConfig != nil && c.HttpApiTlsConfig.CertPath == "" {
			return errors.NewInvalidConfigurationError("http-api-tls-cert-path must be specified for HTTP API server")
		}
		if c.HttpApiTlsConfig != nil && c.HttpApiTlsConfig.KeyPath == "" {
			return errors.NewInvalidConfigurationError("http-api-tls-key-path must be specified for HTTP API server")
		}
		if c.HttpApiTlsConfig != nil && c.HttpApiTlsConfig.ClientAuth != ClientAuthModeNoClientCert && c.HttpApiTlsConfig.ClientAuth != "" && c.HttpApiTlsConfig.ClientCertsPath == "" {
			return errors.NewInvalidConfigurationError("http-api-tls-client-certs-path must be provided if client auth is enabled")
		}
	}
	if *c.AdminConsoleEnabled {
		if len(c.AdminConsoleAddresses) == 0 {
			return errors.NewInvalidConfigurationError("admin-console-addresses must be specified")
		}
		if (c.AdminConsoleSampleInterval != nil && *c.AdminConsoleSampleInterval == 0) || c.AdminConsoleSampleInterval == nil {
			c.AdminConsoleSampleInterval = types.AddressOf(DefaultWebUISampleInterval)
		}
	}
	if *c.LifeCycleEndpointEnabled {
		if (c.LifeCycleAddress != nil && *c.LifeCycleAddress == "") || c.LifeCycleAddress == nil {
			return errors.NewInvalidConfigurationError("life-cycle-address must be specified")
		}
		if (c.StartupEndpointPath != nil && *c.StartupEndpointPath == "") || c.StartupEndpointPath == nil {
			return errors.NewInvalidConfigurationError("startup-endpoint-path must be specified")
		}
		if (c.LiveEndpointPath != nil && *c.LiveEndpointPath != "") || c.LiveEndpointPath == nil {
			return errors.NewInvalidConfigurationError("live-endpoint-path must be specified")
		}
		if (c.ReadyEndpointPath != nil && *c.ReadyEndpointPath != "") || c.ReadyEndpointPath == nil {
			return errors.NewInvalidConfigurationError("ready-endpoint-path must be specified")
		}
	}
	if c.ClusterTlsConfig.Enabled {
		if c.ClusterTlsConfig.CertPath == "" {
			return errors.NewInvalidConfigurationError("cluster-tls-cert-path must be specified if cluster-tls-enabled is true")
		}
		if c.ClusterTlsConfig.KeyPath == "" {
			return errors.NewInvalidConfigurationError("cluster-tls-key-path must be specified if cluster-tls-enabled is true")
		}
		if c.ClusterTlsConfig.ClientCertsPath == "" {
			return errors.NewInvalidConfigurationError("cluster-tls-client-certs-path must be specified if cluster-tls-enabled is true")
		}
	}
	if *c.ClusterName == "" {
		return errors.NewInvalidConfigurationError("cluster-name must be specified")
	}
	if *c.ProcessingEnabled && *c.ProcessorCount < 1 {
		return errors.NewInvalidConfigurationError("processor-count must be > 0")
	}
	if *c.ProcessorCount < 0 {
		return errors.NewInvalidConfigurationError("processor-count must be >= 0")
	}
	if *c.MaxBackfillBatchSize < 1 {
		return errors.NewInvalidConfigurationError("max-backfill-batch-size must be > 0")
	}

	if *c.MaxProcessorBatchesInProgress < 1 {
		return errors.NewInvalidConfigurationError("max-processor-batches-in-progress must be > 0")
	}
	if *c.MinSnapshotInterval < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("min-snapshot-interval must be >= 1 millisecond")
	}
	if *c.IdleProcessorCheckInterval < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("idle-processor-check-interval must be >= 1 millisecond")
	}
	if *c.MemtableMaxSizeBytes < 1 {
		return errors.NewInvalidConfigurationError("memtable-max-size-bytes must be > 0")
	}
	if *c.MemtableMaxReplaceInterval < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("memtable-max-replace-time must be >= 1 ms")
	}
	if *c.MemtableFlushQueueMaxSize < 1 {
		return errors.NewInvalidConfigurationError("memtable-flush-queue-max-size must be > 0")
	}
	if *c.MinReplicas < 1 {
		return errors.NewInvalidConfigurationError("min-replicas must be > 0")
	}
	if *c.MaxReplicas < 1 {
		return errors.NewInvalidConfigurationError("max-replicas must be > 0")
	}
	if *c.MinReplicas > *c.MaxReplicas {
		return errors.NewInvalidConfigurationError("min-replicas must be <= max-replicas")
	}
	if *c.TableFormat != common.DataFormatV1 {
		return errors.NewInvalidConfigurationError("table-format must be specified")
	}
	if *c.LevelManagerFlushInterval < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("level-manager-flush-interval must be >= 1ms")
	}
	if *c.SegmentCacheMaxSize < 0 {
		return errors.NewInvalidConfigurationError("segment-cache-max-size must be >= 0")
	}
	if c.DevObjectStoreAddresses == nil {
		c.DevObjectStoreAddresses = []string{DefaultDevObjectStoreAddress}
	}
	if *c.ObjectStoreType == "" {
		*c.ObjectStoreType = DevObjectStoreType
	}
	if *c.ClusterManagerLockTimeout < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("cluster-manager-lock-timeout must be >= 1ms")
	}
	if *c.ClusterManagerKeyPrefix == "" {
		return errors.NewInvalidConfigurationError("cluster-manager-key-prefix must be specified")
	}
	if len(c.ClusterManagerAddresses) == 0 {
		return errors.NewInvalidConfigurationError("cluster-manager-addresses must be specified")
	}
	if *c.ClusterEvictionTimeout < 1*time.Second {
		return errors.NewInvalidConfigurationError("cluster-eviction-timeout must be >= 1s")
	}
	if *c.ClusterStateUpdateInterval < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError("cluster-eviction-timeout must be >= 1ms")
	}

	if *c.KafkaInitialJoinDelay < 0 {
		return errors.NewInvalidConfigurationError("kafka-initial-join-delay must be >= 0")
	}
	if *c.KafkaMinSessionTimeout < 0 {
		return errors.NewInvalidConfigurationError("kafka-min-session-timeout must be >= 0")
	}
	if *c.KafkaMaxSessionTimeout < 0 {
		return errors.NewInvalidConfigurationError("kafka-max-session-timeout must be >= 0")
	}
	if *c.KafkaMaxSessionTimeout <= *c.KafkaMinSessionTimeout {
		return errors.NewInvalidConfigurationError("kafka-max-session-timeout must be > kafka-min-session-timeout")
	}
	return nil
}
