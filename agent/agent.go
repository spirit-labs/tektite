package agent

import (
	"github.com/pkg/errors"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetchcache"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/group"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/spirit-labs/tektite/tx"
	"sync"
	"sync/atomic"
	"time"
)

type Agent struct {
	lock                     sync.RWMutex
	cfg                      Conf
	started                  bool
	transportServer          transport.Server
	kafkaServer              *kafkaserver2.KafkaServer
	tablePusher              *pusher.TablePusher
	batchFetcher             *fetcher.BatchFetcher
	controller               *control.Controller
	controlClientCache       *control.ClientCache
	connCaches               *transport.ConnCaches
	membership               ClusterMembership
	compactionWorkersService *lsm.CompactionWorkerService
	partitionHashes          *parthash.PartitionHashes
	fetchCache               *fetchcache.Cache
	groupCoordinator         *group.Coordinator
	txCoordinator            *tx.Coordinator
	topicMetaCache           *topicmeta.LocalCache
	saslAuthManager          *auth.SaslAuthManager
	scramManager             *auth.ScramManager
	manifold                 *membershipChangedManifold
	partitionLeaders         map[string]map[int]map[int]int32
	clusterMembershipFactory ClusterMembershipFactory
	tableGetter              sst.TableGetter
	authCaches               *auth.UserAuthCaches
}

func NewAgent(cfg Conf, objStore objstore.Client) (*Agent, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transportServer := transport.NewSocketTransportServer(cfg.ClusterListenerConfig.Address, cfg.ClusterListenerConfig.TLSConfig)
	socketClient, err := transport.NewSocketClient(&cfg.ClusterClientTlsConfig)
	if err != nil {
		return nil, err
	}
	clusterMembershipFactory := func(data []byte, listener MembershipListener) ClusterMembership {
		return cluster.NewMembership(cfg.ClusterMembershipConfig, data, objStore, listener)
	}
	return NewAgentWithFactories(cfg, objStore, socketClient.CreateConnection, transportServer,
		clusterMembershipFactory)
}

const partitionHashCacheMaxSize = 100000

type ClusterMembershipFactory func(data []byte, listener MembershipListener) ClusterMembership

type MembershipListener func(thisMemberID int32, state cluster.MembershipState) error

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
		cfg:              cfg,
		partitionLeaders: map[string]map[int]map[int]int32{},
	}
	agent.connCaches = transport.NewConnCaches(cfg.MaxConnectionsPerAddress, connectionFactory)
	agent.controller = control.NewController(cfg.ControllerConf, objStore, agent.connCaches, connectionFactory, transportServer)
	agent.authCaches = auth.NewUserAuthCaches(cfg.UserAuthCacheTimeout, func() (auth.ControlClient, error) {
		return agent.controller.Client()
	})
	agent.controlClientCache = control.NewClientCache(cfg.MaxControllerClients, agent.controller.Client)
	agent.topicMetaCache = topicmeta.NewLocalCache(func() (topicmeta.ControllerClient, error) {
		cl, err := agent.controller.Client()
		return cl, err
	})
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicAdded, agent.topicMetaCache.HandleTopicAdded)
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicDeleted, agent.topicMetaCache.HandleTopicDeleted)
	partitionHashes, err := parthash.NewPartitionHashes(partitionHashCacheMaxSize)
	if err != nil {
		return nil, err
	}
	agent.partitionHashes = partitionHashes
	fetchCache, err := fetchcache.NewCache(objStore, agent.connCaches, transportServer, cfg.FetchCacheConf)
	if err != nil {
		return nil, err
	}
	agent.fetchCache = fetchCache
	getter := &fetchCacheGetter{fetchCache: fetchCache}
	agent.tableGetter = getter.get
	agent.controller.SetTableGetter(getter.get)
	clientFactory := func() (pusher.ControlClient, error) {
		// Note, we do not use a controller client cache here - the table pusher uses its own connection to avoid
		// a deadlock where an operation like delete topic uses a connection then the controller calls table pusher
		// which tries to use the same connection to prePush back to the controller, but gets locked as that connection
		// is in use.
		return agent.controller.Client()
	}
	tablePusher, err := pusher.NewTablePusher(cfg.PusherConf, agent.topicMetaCache, objStore, clientFactory, getter.get, partitionHashes, agent)
	if err != nil {
		return nil, err
	}
	agent.tablePusher = tablePusher
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, tablePusher.HandleDirectWriteRequest)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectProduce, tablePusher.HandleDirectProduceRequest)
	bf, err := fetcher.NewBatchFetcher(objStore, agent.topicMetaCache, partitionHashes, agent.controlClientCache, getter.get,
		cfg.FetcherConf)
	if err != nil {
		return nil, err
	}
	agent.batchFetcher = bf
	groupCoord, err := group.NewCoordinator(cfg.GroupCoordinatorConf, agent.topicMetaCache,
		agent.controlClientCache, agent.connCaches, getter.get)
	if err != nil {
		return nil, err
	}
	agent.groupCoordinator = groupCoord
	agent.txCoordinator = tx.NewCoordinator(agent.controlClientCache, getter.get, agent.connCaches,
		agent.topicMetaCache, partitionHashes)
	agent.kafkaServer = kafkaserver2.NewKafkaServer(cfg.KafkaListenerConfig.Address,
		cfg.KafkaListenerConfig.TLSConfig, cfg.AuthType, agent.newKafkaHandler, agent.authCaches)
	agent.manifold = &membershipChangedManifold{listeners: []MembershipListener{fetchCache.MembershipChanged,
		agent.controller.MembershipChanged, bf.MembershipChanged, groupCoord.MembershipChanged}}
	agent.clusterMembershipFactory = clusterMembershipFactory
	agent.transportServer = transportServer
	clFactory := func() (lsm.ControllerClient, error) {
		cc, err := agent.controller.Client()
		if err != nil {
			return nil, err
		}
		return &compactionWorkerControllerClient{cc: cc}, nil
	}
	agent.compactionWorkersService = lsm.NewCompactionWorkerService(cfg.CompactionWorkersConf, objStore,
		clFactory, getter.get, partitionHashes, true)
	scramManager, err := auth.NewScramManager(auth.ScramAuthTypeSHA512, agent.controlClientCache, getter.get,
		cfg.AllowScramNonceAsPrefix)
	if err != nil {
		return nil, err
	}
	agent.scramManager = scramManager
	saslAuthManager, err := auth.NewSaslAuthManager(scramManager)
	if err != nil {
		return nil, err
	}
	agent.saslAuthManager = saslAuthManager
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
	if err := a.tablePusher.Start(); err != nil {
		return err
	}
	a.fetchCache.Start()
	if err := a.batchFetcher.Start(); err != nil {
		return err
	}
	if err := a.txCoordinator.Start(); err != nil {
		return err
	}
	if err := a.kafkaServer.Start(); err != nil {
		return err
	}
	if err := a.groupCoordinator.Start(); err != nil {
		return err
	}
	a.groupCoordinator.SetKafkaAddress(a.kafkaServer.ListenAddress())
	if err := a.compactionWorkersService.Start(); err != nil {
		return err
	}
	if err := a.transportServer.Start(); err != nil {
		return err
	}
	// We delay creation to start as we need to know the cluster and kafka listen addresses which aren't known until
	// start of the socket servers as they could be using an ephemeral port
	membershipData := common.MembershipData{
		ClusterListenAddress: a.transportServer.Address(),
		KafkaListenerAddress: a.kafkaServer.ListenAddress(),
		Location:             a.cfg.FetchCacheConf.AzInfo,
	}
	a.membership = a.clusterMembershipFactory(membershipData.Serialize(nil), a.manifold.membershipChanged)
	if err := a.membership.Start(); err != nil {
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
	// We must close conn caches and control caches first and incoming kafka requests might be waiting on response from outgoing rpcs
	// these will need to return before the kafka server can close
	a.connCaches.Close()
	a.controlClientCache.Close()
	if err := a.kafkaServer.Stop(); err != nil {
		return err
	}
	if err := a.compactionWorkersService.Stop(); err != nil {
		return err
	}
	if err := a.transportServer.Stop(); err != nil {
		return err
	}
	if err := a.txCoordinator.Stop(); err != nil {
		return err
	}
	if err := a.groupCoordinator.Stop(); err != nil {
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

func (a *Agent) MemberID() int32 {
	return atomic.LoadInt32(&a.manifold.memberID)
}

func (a *Agent) Controller() *control.Controller {
	return a.controller
}

func (a *Agent) TablePusher() *pusher.TablePusher {
	return a.tablePusher
}

func (a *Agent) KafkaListenAddress() string {
	return a.kafkaServer.ListenAddress()
}

func (a *Agent) ClusterListenAddress() string {
	return a.transportServer.Address()
}

func (a *Agent) TableGetter() sst.TableGetter {
	return a.tableGetter
}

type membershipChangedManifold struct {
	listeners               []MembershipListener
	deliveredClusterVersion int64
	memberID                int32
}

func (m *membershipChangedManifold) membershipChanged(thisMemberID int32, membership cluster.MembershipState) error {
	for _, l := range m.listeners {
		if err := l(thisMemberID, membership); err != nil {
			return err
		}
	}
	atomic.StoreInt64(&m.deliveredClusterVersion, int64(membership.ClusterVersion))
	atomic.StoreInt32(&m.memberID, thisMemberID)
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
	return sst.GetSSTableFromBytes(bytes)
}

type compactionWorkerControllerClient struct {
	cc control.Client
}

func (c *compactionWorkerControllerClient) IsCompactedTopic(topicID int) (bool, error) {
	info, exists, err := c.cc.GetTopicInfoByID(topicID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	return info.Compacted, nil
}

func (c *compactionWorkerControllerClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	return c.cc.QueryTablesInRange(keyStart, keyEnd)
}

func (c *compactionWorkerControllerClient) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	return c.cc.ApplyLsmChanges(regBatch)
}

func (c *compactionWorkerControllerClient) PollForJob() (lsm.CompactionJob, error) {
	return c.cc.PollForJob()
}

func (c *compactionWorkerControllerClient) GetRetentionForTopic(topicID int) (time.Duration, bool, error) {
	topicInfo, exists, err := c.cc.GetTopicInfoByID(topicID)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return 0, false, nil
	}
	return topicInfo.RetentionTime, true, nil
}

func (c *compactionWorkerControllerClient) Close() error {
	return c.cc.Close()
}
