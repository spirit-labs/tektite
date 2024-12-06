package agent

import (
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetchcache"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/group"
	kafkaserver "github.com/spirit-labs/tektite/kafkaserver2"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/minio"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/topicmeta"
	"net"
	"time"
)

type CommandConf struct {
	ObjStoreUsername                string             `help:"username for the object store" required:""`
	ObjStorePassword                string             `help:"password for the object store" required:""`
	ObjStoreURL                     string             `help:"url of the object store" required:""`
	ClusterName                     string             `help:"name of the agent cluster" required:""`
	Location                        string             `help:"location (e.g. availability zone) that the agent runs in" required:""`
	KafkaListenAddress              string             `help:"address to listen on for kafka connections"`
	KafkaTlsConf                    conf.TlsConf       `help:"tls configuration for the Kafka api" embed:"" prefix:"kafka-tls-"`
	InternalTlsConf                 conf.TlsConf       `help:"tls configuration for the internal connections" embed:"" prefix:"internal-tls-"`
	InternalClientTlsConf           conf.ClientTlsConf `help:"client tls configuration for the internal connections" embed:"" prefix:"internal-client-tls-"`
	InternalListenAddress           string             `help:"address to listen on for internal connections"`
	MembershipUpdateIntervalMs      int                `help:"interval between updating cluster membership in ms" default:"5000"`
	MembershipEvictionIntervalMs    int                `help:"interval after which member will be evicted from the cluster" default:"20000"`
	ConsumerGroupInitialJoinDelayMs int                `name:"consumer-group-initial-join-delay-ms" help:"initial delay to wait for more consumers to join a new consumer group before performing the first rebalance, in ms" default:"3000"`
	AuthenticationType              string             `help:"type of authentication. one of sasl/plain, sasl/scram-sha-512, mtls, none" default:"none"`
	AllowScramNonceAsPrefix         bool

	TopicName string `name:"topic-name" help:"name of the topic"`
}

var authTypeMapping = map[string]kafkaserver.AuthenticationType{
	"none":               kafkaserver.AuthenticationTypeNone,
	"sasl/plain":         kafkaserver.AuthenticationTypeSaslPlain,
	"sasl/scram-sha-512": kafkaserver.AuthenticationTypeSaslScram512,
	"mtls":               kafkaserver.AuthenticationTypeMTls,
}

const (
	DefaultKafkaPort    = 9092
	DefaultInternalPort = 2323
)

func CreateConfFromCommandConf(commandConf CommandConf) (Conf, error) {
	cfg := NewConf()
	// Configure listener config
	var listenAddress string
	var kafkaAddress string
	var err error
	if commandConf.KafkaListenAddress == "" {
		listenAddress, err = selectNetworkInterface()
		if err != nil {
			return cfg, err
		}
		kafkaAddress = fmt.Sprintf("%s:%d", listenAddress, DefaultKafkaPort)
	} else {
		kafkaAddress = commandConf.KafkaListenAddress
	}
	cfg.KafkaListenerConfig.Address = kafkaAddress
	cfg.KafkaListenerConfig.TLSConfig = commandConf.KafkaTlsConf

	var clusterAddress string
	if commandConf.InternalListenAddress == "" {
		if listenAddress != "" {
			listenAddress, err = selectNetworkInterface()
			if err != nil {
				return cfg, err
			}
		}
		clusterAddress = fmt.Sprintf("%s:%d", listenAddress, DefaultInternalPort)
		cfg.ClusterListenerConfig.TLSConfig = commandConf.KafkaTlsConf
	} else {
		clusterAddress = commandConf.InternalListenAddress
		cfg.ClusterListenerConfig.TLSConfig = commandConf.InternalTlsConf
	}
	cfg.ClusterListenerConfig.Address = clusterAddress
	cfg.ClusterClientTlsConfig = commandConf.InternalClientTlsConf

	dataBucketName := commandConf.ClusterName + "-data"
	// configure cluster membership
	cfg.ClusterMembershipConfig.BucketName = dataBucketName
	cfg.ClusterMembershipConfig.KeyPrefix = "membership"
	updateInterval, err :=
		validateDurationMs("membership-update-interval-ms", commandConf.MembershipUpdateIntervalMs, 1)
	if err != nil {
		return Conf{}, err
	}
	evictionInterval, err :=
		validateDurationMs("membership-eviction-interval-ms", commandConf.MembershipEvictionIntervalMs, 1)
	if err != nil {
		return Conf{}, err
	}
	cfg.ClusterMembershipConfig.UpdateInterval = updateInterval
	cfg.ClusterMembershipConfig.EvictionInterval = evictionInterval
	// configure controller
	cfg.ControllerConf.SSTableBucketName = dataBucketName
	cfg.ControllerConf.ControllerMetaDataKeyPrefix = "meta-data"
	cfg.ControllerConf.ControllerMetaDataBucketName = dataBucketName
	// We put the controller state machine in a different bucket - as this will likely be configured with an expiry
	// TODO move to Dynamo based state machine
	controllerStateMachineBucketName := commandConf.ClusterName + "-controller-sm"
	cfg.ControllerConf.ControllerStateUpdaterBucketName = controllerStateMachineBucketName
	cfg.ControllerConf.ControllerStateUpdaterKeyPrefix = "meta-state"
	cfg.ControllerConf.AzInfo = commandConf.Location
	cfg.ControllerConf.LsmConf.SSTableBucketName = dataBucketName
	// configure compaction workers
	cfg.CompactionWorkersConf.SSTableBucketName = dataBucketName
	// configure table pusher
	cfg.PusherConf.DataBucketName = dataBucketName
	// configure fetcher
	cfg.FetcherConf.DataBucketName = dataBucketName
	// configure fetch cache
	cfg.FetchCacheConf.DataBucketName = dataBucketName
	cfg.FetchCacheConf.MaxSizeBytes = 1 * 1024 * 1024 * 1024 // 1GiB
	cfg.FetchCacheConf.AzInfo = commandConf.Location
	// configure group coordinator
	cfg.GroupCoordinatorConf.InitialJoinDelay, err =
		validateDurationMs("consumer-group-initial-join-delay-ms", commandConf.ConsumerGroupInitialJoinDelayMs, 0)
	if err != nil {
		return Conf{}, err
	}
	authType, ok := authTypeMapping[commandConf.AuthenticationType]
	if !ok {
		return Conf{}, errors.Errorf("invalid authentication-type: %s", commandConf.AuthenticationType)
	}
	cfg.AuthType = authType
	cfg.AllowScramNonceAsPrefix = commandConf.AllowScramNonceAsPrefix
	if cfg.AllowScramNonceAsPrefix {
		log.Warnf("allow-scram-nonce-as-prefix is set to true to allow SCRAM handshakes to pass with older" +
			" versions of librdkafka which have a bug where the nonce sent in the second SCRAM handshake request is a" +
			" prefix of the required nonce. It is recommended to upgrade clients to later versions of librdkafka where possible" +
			" and not to enable this setting.")
	}
	return cfg, nil
}

func validateDurationMs(configName string, durationMs int, minDurationMs int) (time.Duration, error) {
	if durationMs < minDurationMs {
		return 0, errors.Errorf("invalid value for %s must be >= %d ms", configName, minDurationMs)
	}
	return time.Duration(durationMs) * time.Millisecond, nil
}

func CreateAgentFromCommandConf(commandConf CommandConf) (*Agent, error) {
	cfg, err := CreateConfFromCommandConf(commandConf)
	if err != nil {
		return nil, err
	}
	minioConf := minio.Conf{
		Endpoint: commandConf.ObjStoreURL,
		Username: commandConf.ObjStoreUsername,
		Password: commandConf.ObjStorePassword,
		Secure:   false,
	}
	objStore := minio.NewMinioClient(minioConf)
	if err := objStore.Start(); err != nil {
		return nil, err
	}
	return NewAgent(cfg, objStore)
}

func selectNetworkInterface() (string, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var address string
	for _, addr := range addresses {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				address = ipNet.IP.String()
				break
			}
		}
	}
	if address == "" {
		return "", errors.New("could not find IPV4 network address")
	}
	return address, nil
}

type Conf struct {
	ClusterListenerConfig    ListenerConfig
	ClusterClientTlsConfig   conf.ClientTlsConf
	KafkaListenerConfig      ListenerConfig
	ClusterMembershipConfig  cluster.MembershipConf
	PusherConf               pusher.Conf
	ControllerConf           control.Conf
	CompactionWorkersConf    lsm.CompactionWorkerServiceConf
	FetcherConf              fetcher.Conf
	FetchCacheConf           fetchcache.Conf
	GroupCoordinatorConf     group.Conf
	MaxControllerClients     int
	MaxConnectionsPerAddress int
	AuthType                 kafkaserver.AuthenticationType
	AllowScramNonceAsPrefix  bool
	AddJunkOnScramNonce      bool
  DefaultTopicRetentionTime time.Duration
}

func NewConf() Conf {
	return Conf{
		ClusterMembershipConfig:  cluster.NewMembershipConf(),
		PusherConf:               pusher.NewConf(),
		ControllerConf:           control.NewConf(),
		CompactionWorkersConf:    lsm.NewCompactionWorkerServiceConf(),
		FetcherConf:              fetcher.NewConf(),
		FetchCacheConf:           fetchcache.NewConf(),
		GroupCoordinatorConf:     group.NewConf(),
		MaxControllerClients:     DefaultMaxControllerClients,
		MaxConnectionsPerAddress: DefaultMaxConnectionsPerAddress,
		AuthType:                 kafkaserver.AuthenticationTypeNone,
		DefaultTopicRetentionTime: DefaultDefaultTopicRetentionTime,
	}
}

const (
  DefaultDefaultTopicRetentionTime = 7 * 24 * time.Hour
	DefaultMaxControllerClients     = 10
	DefaultMaxConnectionsPerAddress = 10
)

func (c *Conf) Validate() error {
	if err := c.ClusterListenerConfig.Validate(); err != nil {
		return err
	}
	if err := c.KafkaListenerConfig.Validate(); err != nil {
		return err
	}
	if err := c.ClusterMembershipConfig.Validate(); err != nil {
		return err
	}
	if err := c.PusherConf.Validate(); err != nil {
		return err
	}
	if err := c.ControllerConf.Validate(); err != nil {
		return err
	}
	if err := c.CompactionWorkersConf.Validate(); err != nil {
		return err
	}
	if err := c.FetcherConf.Validate(); err != nil {
		return err
	}
	if err := c.FetchCacheConf.Validate(); err != nil {
		return err
	}
	if err := c.GroupCoordinatorConf.Validate(); err != nil {
		return err
	}
	return nil
}

type ListenerConfig struct {
	Address           string
	AdvertisedAddress string
	TLSConfig         conf.TlsConf
}

func (l *ListenerConfig) Validate() error {
	// TODO
	return nil
}

// FIXME - get rid of these once create/delete topic is complete

func (a *Agent) CreateTopicWithRetry(topicName string, partitions int) {
	for {
		if err := createTopic(topicName, partitions, a); err != nil {
			if common.IsUnavailableError(err) {
				log.Debugf("failed to create topic %v", err)
				time.Sleep(1 * time.Millisecond)
				continue
			}
			if err != nil {
				log.Errorf("failed to create topic %s: %v", topicName, err)
				return
			}
		} else {
			log.Infof("agent created topic %s", topicName)
			return
		}
	}
}

func createTopic(topicName string, partitions int, agent *Agent) error {
	controller := agent.Controller()
	cl, err := controller.Client()
	if err != nil {
		return err
	}
	defer func() {
		err := cl.Close()
		if err != nil {
			panic(err)
		}
	}()

	return cl.CreateOrUpdateTopic(topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: partitions,
	}, true)
}
