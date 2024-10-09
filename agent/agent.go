package agent

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

type Agent struct {
	lock                     sync.Mutex
	cfg                      Conf
	started                  bool
	transportServer          transport.Server
	kafkaServer              *kafkaserver2.KafkaServer
	tablePusher              *pusher.TablePusher
	batchFetcher             *fetcher.BatchFetcher
	controller               *control.Controller
	membership               ClusterMembership
	compactionWorkersService *lsm.CompactionWorkerService
	partitionHashes          *parthash.PartitionHashes
}

func NewAgent(cfg Conf, objStore objstore.Client) (*Agent, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transportServer := transport.NewSocketTransportServer(cfg.ClusterListenerConfig.Address, cfg.ClusterListenerConfig.TLSConfig)
	socketClient, err := transport.NewSocketClient(nil)
	if err != nil {
		return nil, err
	}
	clusterMembershipFactory := func(address string, listener MembershipListener) ClusterMembership {
		return cluster.NewMembership(cfg.ClusterMembershipConfig, address, objStore, listener)
	}
	return NewAgentWithFactories(cfg, objStore, socketClient.CreateConnection, transportServer,
		clusterMembershipFactory)
}

const partitionHashCacheMaxSize = 100000

type ClusterMembershipFactory func(address string, listener MembershipListener) ClusterMembership

type MembershipListener func(state cluster.MembershipState) error

type ClusterMembership interface {
	Start() error
	Stop() error
}

func NewAgentWithFactories(cfg Conf, objStore objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, clusterMembershipFactory ClusterMembershipFactory) (*Agent, error) {
	agent := &Agent{
		cfg: cfg,
	}
	agent.controller = control.NewController(cfg.ControllerConf, objStore, connectionFactory, transportServer)
	topicMetaCache := topicmeta.NewLocalCache(func() (topicmeta.ControllerClient, error) {
		cl, err := agent.controller.Client()
		return cl, err
	})
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicAdded, topicMetaCache.HandleTopicAdded)
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicDeleted, topicMetaCache.HandleTopicDeleted)
	clientFactory := func() (pusher.ControlClient, error) {
		return agent.controller.Client()
	}
	partitionHashes, err := parthash.NewPartitionHashes(partitionHashCacheMaxSize)
	if err != nil {
		return nil, err
	}
	agent.partitionHashes = partitionHashes
	rb, err := pusher.NewTablePusher(cfg.PusherConf, topicMetaCache, objStore, clientFactory, partitionHashes)
	if err != nil {
		return nil, err
	}
	agent.tablePusher = rb
	fetcherClFactory := func() (fetcher.ControlClient, error) {
		return agent.controller.Client()
	}
	bf, err := fetcher.NewBatchFetcher(objStore, topicMetaCache, partitionHashes, fetcherClFactory, nil, cfg.FetcherConf)
	if err != nil {
		return nil, err
	}
	agent.batchFetcher = bf
	agent.kafkaServer = kafkaserver2.NewKafkaServer(cfg.KafkaListenerConfig.Address,
		cfg.KafkaListenerConfig.TLSConfig, cfg.KafkaListenerConfig.AuthenticationType, agent.newKafkaHandler)
	agent.membership = clusterMembershipFactory(transportServer.Address(), agent.controller.MembershipChanged)
	agent.transportServer = transportServer
	clFactory := func() (lsm.ControllerClient, error) {
		return agent.controller.Client()
	}
	agent.compactionWorkersService = lsm.NewCompactionWorkerService(lsm.NewCompactionWorkerServiceConf(), objStore,
		clFactory, true)
	return agent, nil
}

func (a *Agent) Start() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.started {
		return nil
	}
	if err := a.controller.Start(); err != nil {
		return err
	}
	if err := a.membership.Start(); err != nil {
		return err
	}
	if err := a.tablePusher.Start(); err != nil {
		return err
	}
	if err := a.batchFetcher.Start(); err != nil {
		return err
	}
	if err := a.kafkaServer.Start(); err != nil {
		return err
	}
	if err := a.compactionWorkersService.Start(); err != nil {
		return err
	}
	a.started = true
	return nil
}

func (a *Agent) Stop() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.started {
		return nil
	}
	if err := a.compactionWorkersService.Stop(); err != nil {
		return err
	}
	if err := a.transportServer.Stop(); err != nil {
		return err
	}
	if err := a.kafkaServer.Stop(); err != nil {
		return err
	}
	if err := a.batchFetcher.Stop(); err != nil {
		return err
	}
	if err := a.tablePusher.Stop(); err != nil {
		return err
	}
	if err := a.membership.Stop(); err != nil {
		return err
	}
	if err := a.controller.Stop(); err != nil {
		return err
	}
	a.started = false
	return nil
}

func (a *Agent) Conf() Conf {
	return a.cfg
}
