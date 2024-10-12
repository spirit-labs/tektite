package agent

import (
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetchcache"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
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
	fetchCache               *fetchcache.Cache
	manifold                 *membershipChangedManifold
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
	clusterMembershipFactory := func(id string, data []byte, listener MembershipListener) ClusterMembership {
		return cluster.NewMembership(cfg.ClusterMembershipConfig, id, data, objStore, listener)
	}
	return NewAgentWithFactories(cfg, objStore, socketClient.CreateConnection, transportServer,
		clusterMembershipFactory)
}

const partitionHashCacheMaxSize = 100000

type ClusterMembershipFactory func(id string, data []byte, listener MembershipListener) ClusterMembership

type MembershipListener func(state cluster.MembershipState) error

type MembershipData struct {
}

type ClusterMembership interface {
	Start() error
	Stop() error
}

func NewAgentWithFactories(cfg Conf, objStore objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, clusterMembershipFactory ClusterMembershipFactory) (*Agent, error) {
	if !common.Is64BitArch() {
		return nil, errors.New("agent can only run on a 64-bit CPU architecture")
	}
	agent := &Agent{
		cfg: cfg,
	}
	membershipData := common.MembershipData{
		ListenAddress: transportServer.Address(),
		AZInfo:        cfg.FetchCacheConf.AzInfo,
	}
	membershipID := uuid.New().String()
	agent.controller = control.NewController(cfg.ControllerConf, objStore, connectionFactory, transportServer, membershipID)
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
	fetchCache, err := fetchcache.NewCache(objStore, connectionFactory, transportServer, cfg.FetchCacheConf)
	if err != nil {
		return nil, err
	}
	agent.fetchCache = fetchCache
	getter := &fetchCacheGetter{fetchCache: fetchCache}
	bf, err := fetcher.NewBatchFetcher(objStore, topicMetaCache, partitionHashes, fetcherClFactory, getter.get,
		membershipID, cfg.FetcherConf)
	if err != nil {
		return nil, err
	}
	transportServer.RegisterHandler(transport.HandlerIDFetcherTableRegisteredNotification, bf.HandleTableRegisteredNotification)
	agent.batchFetcher = bf
	agent.kafkaServer = kafkaserver2.NewKafkaServer(cfg.KafkaListenerConfig.Address,
		cfg.KafkaListenerConfig.TLSConfig, cfg.KafkaListenerConfig.AuthenticationType, agent.newKafkaHandler)
	agent.manifold = &membershipChangedManifold{listeners: []MembershipListener{agent.controller.MembershipChanged,
		bf.MembershipChanged, fetchCache.MembershipChanged}}
	agent.membership = clusterMembershipFactory(membershipID, membershipData.Serialize(nil), agent.manifold.membershipChanged)
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
	a.fetchCache.Start()
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
	a.fetchCache.Stop()
	if err := a.membership.Stop(); err != nil {
		return err
	}
	if err := a.controller.Stop(); err != nil {
		return err
	}
	a.started = false
	return nil
}

func (a *Agent) DeliveredClusterVersion() int {
	return int(atomic.LoadInt64(&a.manifold.deliveredClusterVersion))
}

func (a *Agent) Conf() Conf {
	return a.cfg
}

type membershipChangedManifold struct {
	listeners               []MembershipListener
	deliveredClusterVersion int64
}

func (m *membershipChangedManifold) membershipChanged(membership cluster.MembershipState) error {
	for _, l := range m.listeners {
		if err := l(membership); err != nil {
			return err
		}
	}
	atomic.StoreInt64(&m.deliveredClusterVersion, int64(membership.ClusterVersion))
	return nil
}

type fetchCacheGetter struct {
	fetchCache *fetchcache.Cache
}

func (o *fetchCacheGetter) get(tableID sst.SSTableID) (*sst.SSTable, error) {
	bytes, err := o.fetchCache.GetTableBytes(tableID)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, errors.Errorf("cannot find sstable %s", tableID)
	}
	table := &sst.SSTable{}
	table.Deserialize(bytes, 0)
	return table, nil
}
