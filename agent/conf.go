package agent

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetchcache"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/group"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/minio"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/tx"
	"net"
	"time"
)

type CommandConf struct {
	ObjStoreUsername                string `help:"username for the object store" required:""`
	ObjStorePassword                string `help:"password for the object store" required:""`
	ObjStoreURL                     string `help:"url of the object store" required:""`
	ClusterName                     string `help:"name of the agent cluster" required:""`
	Location                        string `help:"location (e.g. availability zone) that the agent runs in" required:""`
	KafkaListenAddress              string `help:"address to listen on for kafka connections"`
	InternalListenAddress           string `help:"address to listen on for internal connections"`
	MembershipUpdateIntervalMs      int    `help:"interval between updating cluster membership in ms" default:"5000"`
	MembershipEvictionIntervalMs    int    `help:"interval after which member will be evicted from the cluster" default:"20000"`
	ConsumerGroupInitialJoinDelayMs int    `name:"consumer-group-initial-join-delay-ms" help:"initial delay to wait for more consumers to join a new consumer group before performing the first rebalance, in ms" default:"3000"`

	TopicName string `name:"topic-name" help:"name of the topic"`
}

const (
	DefaultKafkaPort    = 9092
	DefaultInternalPort = 2323
)

func CreateConfFromCommandConf(commandConf CommandConf) (Conf, error) {
	cfg := NewConf()

	/*
		FIXME- need to validate the durations etc before setting them
	*/

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
	var clusterAddress string
	if commandConf.InternalListenAddress == "" {
		if listenAddress != "" {
			listenAddress, err = selectNetworkInterface()
			if err != nil {
				return cfg, err
			}
		}
		clusterAddress = fmt.Sprintf("%s:%d", listenAddress, DefaultInternalPort)
	} else {
		clusterAddress = commandConf.InternalListenAddress
	}
	cfg.KafkaListenerConfig.Address = kafkaAddress
	cfg.ClusterListenerConfig.Address = clusterAddress

	dataBucketName := commandConf.ClusterName + "-data"

	cfg.ClusterMembershipConfig.BucketName = dataBucketName
	cfg.ClusterMembershipConfig.KeyPrefix = "membership"
	updateInterval := time.Duration(commandConf.MembershipUpdateIntervalMs) * time.Millisecond
	evictionInterval := time.Duration(commandConf.MembershipEvictionIntervalMs) * time.Millisecond
	cfg.ClusterMembershipConfig.UpdateInterval = updateInterval
	cfg.ClusterMembershipConfig.EvictionInterval = evictionInterval

	cfg.ControllerConf.SSTableBucketName = dataBucketName
	cfg.ControllerConf.ControllerMetaDataKeyPrefix = "meta-data"
	cfg.ControllerConf.ControllerMetaDataBucketName = dataBucketName
	// TODO - should we put the metadata state updator state machine in a different bucket with expiration?
	cfg.ControllerConf.ControllerStateUpdaterBucketName = dataBucketName
	cfg.ControllerConf.ControllerStateUpdaterKeyPrefix = "meta-state"
	cfg.ControllerConf.AzInfo = commandConf.Location

	cfg.ControllerConf.LsmConf.SSTableBucketName = dataBucketName

	cfg.CompactionWorkersConf.SSTableBucketName = dataBucketName

	cfg.PusherConf.DataBucketName = dataBucketName

	cfg.FetcherConf.DataBucketName = dataBucketName

	cfg.FetchCacheConf.DataBucketName = dataBucketName
	cfg.FetchCacheConf.MaxSizeBytes = 1 * 1024 * 1024 * 1024 // 1GiB
	cfg.FetchCacheConf.AzInfo = commandConf.Location

	cfg.GroupCoordinatorConf.InitialJoinDelay = time.Duration(commandConf.ConsumerGroupInitialJoinDelayMs) * time.Millisecond
	return cfg, nil
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
	ClusterListenerConfig   ListenerConfig
	KafkaListenerConfig     ListenerConfig
	ClusterMembershipConfig cluster.MembershipConf
	PusherConf              pusher.Conf
	ControllerConf          control.Conf
	CompactionWorkersConf   lsm.CompactionWorkerServiceConf
	FetcherConf             fetcher.Conf
	FetchCacheConf          fetchcache.Conf
	GroupCoordinatorConf    group.Conf
	TxCoordinatorConf       tx.Conf
	MaxControllerClients    int
}

func NewConf() Conf {
	return Conf{
		ClusterMembershipConfig: cluster.NewMembershipConf(),
		PusherConf:              pusher.NewConf(),
		ControllerConf:          control.NewConf(),
		CompactionWorkersConf:   lsm.NewCompactionWorkerServiceConf(),
		FetcherConf:             fetcher.NewConf(),
		FetchCacheConf:          fetchcache.NewConf(),
		GroupCoordinatorConf:    group.NewConf(),
		TxCoordinatorConf:       tx.NewConf(),
		MaxControllerClients:    DefaultMaxControllerClients,
	}
}

const DefaultMaxControllerClients = 10

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
	if err := c.TxCoordinatorConf.Validate(); err != nil {
		return err
	}
	return nil
}

type ListenerConfig struct {
	Address            string
	AdvertisedAddress  string
	TLSConfig          conf.TLSConfig
	AuthenticationType string
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
				log.Warnf("failed to create topic %v", err)
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

	return cl.CreateTopic(topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: partitions,
	})
}
