package server

import (
	"fmt"
	"github.com/spirit-labs/tektite/admin"
	"github.com/spirit-labs/tektite/api"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/command"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/kafka/load"
	"github.com/spirit-labs/tektite/kafkaserver"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/lock"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/objstore/minio"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/repli"
	"github.com/spirit-labs/tektite/sequence"
	"github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/spirit-labs/tektite/wasm"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spirit-labs/tektite/lifecycle"
	"github.com/spirit-labs/tektite/metrics"
	"github.com/spirit-labs/tektite/remoting"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	_ "net/http/pprof"
	"sync"

	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"

	"github.com/spirit-labs/tektite/conf"
)

func NewServer(config conf.Config) (*Server, error) {
	var cf kafka.ClientFactory
	switch config.ClientType {
	case conf.KafkaClientTypeConfluent:
		cf = kafka.NewMessageProviderFactory
	case conf.KafkaClientTypeLoad:
		cf = load.NewMessageProviderFactory
	default:
		return nil, errors.NewTektiteErrorf(errors.InvalidConfiguration,
			"unexpected ClientType: %d", config.ClientType)
	}
	return NewServerWithClientFactory(config, cf)
}

func NewServerWithClientFactory(config conf.Config, clientFactory kafka.ClientFactory) (*Server, error) {

	if err := config.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	standalone := len(config.ClusterAddresses) == 1

	var clustStateMgr clustmgr.StateManager
	if standalone {
		clustStateMgr = clustmgr.NewLocalStateManager(config.ProcessorCount + 1)
	} else {
		clustStateMgr = clustmgr.NewClusteredStateManager(config.ClusterManagerKeyPrefix, config.ClusterName, config.NodeID,
			config.ClusterManagerAddresses, config.ClusterEvictionTimeout, config.ClusterStateUpdateInterval,
			config.EtcdCallTimeout, config.ProcessorCount+1, config.MaxReplicas)
	}

	var levelManagerClientFactory levels.ClientFactory
	var localLevMgrClientFactory *localLevelManagerClientFactory
	if config.LevelManagerEnabled {
		localLevMgrClientFactory = &localLevelManagerClientFactory{cfg: &config}
		levelManagerClientFactory = localLevMgrClientFactory
	} else {
		levelManagerClientFactory = &externalLevelManagerClientFactory{cfg: &config}
	}

	var lockManager lock.Manager
	if standalone {
		lockManager = lock.NewInMemLockManager()
	} else {
		client := clustStateMgr.(*clustmgr.ClusteredStateManager).GetClient()
		lockManager = &clusterManagerLockManager{
			lockTimeout:    config.ClusterManagerLockTimeout,
			clustMgrClient: client,
		}
	}
	var objStoreClient objstore.Client
	switch config.ObjectStoreType {
	case conf.DevObjectStoreType:
		objStoreClient = dev.NewDevStoreClient(config.DevObjectStoreAddresses[0])
	case conf.EmbeddedObjectStoreType:
		objStoreClient = dev.NewInMemStore(0)
	case conf.MinioObjectStoreType:
		objStoreClient = minio.NewMinioClient(&config)
	default:
		return nil, errors.NewTektiteErrorf(errors.InvalidConfiguration, "invalid object store type: %s", config.ObjectStoreType)
	}
	sequenceManager := sequence.NewSequenceManager(objStoreClient, config.SequencesObjectName, lockManager,
		config.SequencesRetryDelay)
	lifeCycleMgr := lifecycle.NewLifecycleEndpoints(config)

	tableCache, err := tabcache.NewTableCache(objStoreClient, &config)
	if err != nil {
		return nil, err
	}

	// We use the same instance of a level manager client for the store and prefix retentions service
	levMgrClient := levelManagerClientFactory.CreateLevelManagerClient()

	dataStore := store.NewStore(objStoreClient, levMgrClient, tableCache, config)

	var compactionService *levels.CompactionWorkerService
	if config.CompactionWorkersEnabled {
		// each compaction worker has its own level manager client to avoid contention, so we pass in the factory
		compactionService = levels.NewCompactionWorkerService(&config, levelManagerClientFactory, tableCache, objStoreClient, true)
	}

	remotingServer := remoting.NewServer(config.ClusterAddresses[config.NodeID], config.ClusterTlsConfig)

	versionManager := vmgr.NewVersionManager(sequenceManager, levMgrClient, &config, config.ClusterAddresses...)

	moduleManager := wasm.NewModuleManager(objStoreClient, lockManager, &config)
	invokerFactory := &wasm.InvokerFactory{ModManager: moduleManager}
	exprFactory := &expr.ExpressionFactory{ExternalInvokerFactory: invokerFactory}

	theParser := parser.NewParser(&wasmFunctionChecker{moduleManager})

	streamManager := opers.NewStreamManager(clientFactory, dataStore, levMgrClient, exprFactory, &config, false)

	handlerFactory := &batchHandlerFactory{
		cfg:           &config,
		streamManager: streamManager,
	}
	flushNotifier := &levelManagerFlushNotifier{}
	vmgrClient := proc.NewVmgrClient(&config)
	processorManager := proc.NewProcessorManagerWithVmgrClient(clustStateMgr, streamManager, dataStore,
		&config, repli.NewReplicator, handlerFactory.createBatchHandler, flushNotifier, streamManager, !config.FailureDisabled,
		vmgrClient)
	vmgrClient.SetProcessorManager(processorManager)
	clustStateMgr.SetClusterStateHandler(processorManager.HandleClusterState)
	processorManager.RegisterStateHandler(versionManager.HandleClusterState)

	streamManager.SetProcessorManager(processorManager)

	queryManager := query.NewManager(processorManager, processorManager, config.NodeID, streamManager, dataStore,
		streamManager.StreamMetaIteratorProvider(), query.NewDefaultRemoting(&config), config.ClusterAddresses,
		config.QueryMaxBatchRows, exprFactory, theParser)

	levelManagerService := levels.NewLevelManagerService(processorManager, &config, objStoreClient, tableCache,
		proc.NewLevelManagerCommandIngestor(processorManager), processorManager)

	processorManager.RegisterStateHandler(levelManagerService.HandleClusterState)

	processorManager.SetLevelMgrProcessorInitialisedCallback(levelManagerService.ActivateLevelManager)

	handlerFactory.levelManagerBatchHandler = proc.NewLevelManagerBatchHandler(levelManagerService)
	flushNotifier.levelMgrService = levelManagerService

	dmVersionSetter := levels.NewVersionSetter(&config, processorManager, dataStore)

	theMetrics := metrics.NewServer(config, !config.MetricsEnabled)

	commandMgr := command.NewCommandManager(streamManager, queryManager, sequenceManager, lockManager,
		processorManager, processorManager.VersionManagerClient(), theParser, &config)
	var commandSignaller *command.RemotingSignaller
	if !standalone {
		commandSignaller = command.NewSignaller(commandMgr, remotingServer, &config)
		commandMgr.SetCommandSignaller(commandSignaller)
	}
	processorManager.RegisterStateHandler(commandMgr.HandleClusterState)

	var apiServer *api.HTTPAPIServer
	if config.HttpApiEnabled {
		apiServer = api.NewHTTPAPIServer(config.HttpApiAddresses[config.NodeID], config.HttpApiPath,
			queryManager, commandMgr, theParser, moduleManager, config.HttpApiTlsConfig)
	}

	var kafkaServer *kafkaserver.Server
	var kafkaGroupCoordinator *kafkaserver.GroupCoordinator
	if config.KafkaServerEnabled {
		metaProvider, err := kafkaserver.NewMetaDataProvider(&config, processorManager, streamManager)
		if err != nil {
			return nil, err
		}
		processorProvider := &kafkaProcesorProvider{
			procMgr:       processorManager,
			streamManager: streamManager,
		}
		kafkaGroupCoordinator, err = kafkaserver.NewGroupCoordinator(&config, processorProvider, streamManager,
			metaProvider, dataStore, processorManager)
		if err != nil {
			return nil, err
		}
		kafkaServer = kafkaserver.NewServer(&config,
			metaProvider, processorProvider, kafkaGroupCoordinator, dataStore, streamManager)
	}

	var adminServer *admin.Server
	if config.AdminConsoleEnabled {
		adminServer, err = admin.NewServer(&config, levMgrClient, streamManager, processorManager)
		if err != nil {
			return nil, err
		}
	}

	// Other wiring
	if localLevMgrClientFactory != nil {
		localLevMgrClientFactory.procMgr = processorManager
		levMgrClient.(*proc.LevelManagerLocalClient).SetProcessorManager(processorManager)
	}

	// Wire remoting handlers

	repli.SetClusterMessageHandlers(remotingServer, processorManager)
	teeHandler := &remoting.TeeBlockingClusterMessageHandler{}
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageVersionsMessage, teeHandler)
	processorManager.SetClusterMessageHandlers(remotingServer, teeHandler)
	levelManagerService.SetClusterMessageHandlers(remotingServer)
	dataStore.SetClusterMessageHandlers(teeHandler)
	versionManager.SetClusterMessageHandlers(remotingServer)
	queryManager.SetClusterMessageHandlers(remotingServer, teeHandler)

	services := []service{
		remotingServer,
		clustStateMgr,
		objStoreClient,
		versionManager,
		processorManager,
		levelManagerService,
		levMgrClient,
		tableCache,
		dataStore,
		vmgrClient,
		dmVersionSetter,
		streamManager,
		queryManager,
		moduleManager,
		commandMgr,
		commandSignaller,
		apiServer,
		kafkaGroupCoordinator,
		kafkaServer,
		compactionService,
		theMetrics,
		lifeCycleMgr,
		adminServer,
	}

	server := &Server{
		conf:                config,
		nodeID:              config.NodeID,
		lifeCycleMgr:        lifeCycleMgr,
		remotingServer:      remotingServer,
		services:            services,
		metrics:             theMetrics,
		store:               dataStore,
		lockManager:         lockManager,
		sequenceManager:     sequenceManager,
		processorManager:    processorManager,
		clusterStateManager: clustStateMgr,
		streamManager:       streamManager,
		queryManager:        queryManager,
		levelManagerService: levelManagerService,
		versionManager:      versionManager,
		kafkaServer:         kafkaServer,
		apiServer:           apiServer,
	}
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageShutdownMessage, &shutdownMessageHandler{s: server})
	return server, nil
}

type Server struct {
	lock                sync.RWMutex
	nodeID              int
	lifeCycleMgr        *lifecycle.Endpoints
	remotingServer      remoting.Server
	services            []service
	stopped             bool
	conf                conf.Config
	metrics             *metrics.Server
	store               *store.Store
	processorManager    proc.Manager
	clusterStateManager clustmgr.StateManager
	streamManager       opers.StreamManager
	queryManager        query.Manager
	lockManager         lock.Manager
	sequenceManager     sequence.Manager
	versionManager      *vmgr.VersionManager
	levelManagerService *levels.LevelManagerService
	kafkaServer         *kafkaserver.Server
	apiServer           *api.HTTPAPIServer
	webuiServer         *admin.Server
	parser              *parser.Parser
	shutDownPhase       int
	stopWaitGroup       *sync.WaitGroup
	shutdownComplete    bool
}

type service interface {
	Start() error
	Stop() error
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		panic("server cannot be restarted")
	}

	if s.conf.DebugServerEnabled {
		if s.conf.NodeID >= len(s.conf.DebugServerAddresses) {
			return errors.NewTektiteErrorf(errors.InvalidConfiguration, "no entry in DebugServerAddresses for node id %d", s.conf.NodeID)
		}
		address := s.conf.DebugServerAddresses[s.conf.NodeID]
		go func() {
			log.Debug(http.ListenAndServe(address, nil))
		}()
	}

	if err := s.maybeEnabledDatadogProfiler(); err != nil {
		return err
	}

	var err error
	for _, serv := range s.services {
		if !serviceIsNil(serv) {
			log.Debugf("node %d starting service %s", s.nodeID, reflect.TypeOf(serv).String())
			start := time.Now()
			if err = serv.Start(); err != nil {
				return errors.WithStack(err)
			}
			log.Debugf("node %d service %s starting took %d ms", s.nodeID, reflect.TypeOf(serv).String(),
				time.Now().Sub(start).Milliseconds())
		}
	}
	// We delay starting kafka and api server until replicators are ready
	if err := s.processorManager.EnsureReplicatorsReady(); err != nil {
		return err
	}
	if s.kafkaServer != nil {
		if err := s.kafkaServer.Activate(); err != nil {
			return err
		}
	}
	if s.apiServer != nil {
		if err := s.apiServer.Activate(); err != nil {
			return err
		}
	}

	s.lifeCycleMgr.SetActive(true)

	log.Infof("tektite server %d started", s.nodeID)
	return nil
}

func (s *Server) maybeEnabledDatadogProfiler() error {
	ddProfileTypes := s.conf.DDProfilerTypes
	if ddProfileTypes == "" {
		return nil
	}

	ddHost := os.Getenv(s.conf.DDProfilerHostEnvVarName)
	if ddHost == "" {
		return errors.NewTektiteErrorf(errors.InvalidConfiguration, "Env var %s for DD profiler host is not set", s.conf.DDProfilerHostEnvVarName)
	}

	var profileTypes []profiler.ProfileType
	aProfTypes := strings.Split(ddProfileTypes, ",")
	for _, sProfType := range aProfTypes {
		switch sProfType {
		case "CPU":
			profileTypes = append(profileTypes, profiler.CPUProfile)
		case "HEAP":
			profileTypes = append(profileTypes, profiler.HeapProfile)
		case "BLOCK":
			profileTypes = append(profileTypes, profiler.BlockProfile)
		case "MUTEX":
			profileTypes = append(profileTypes, profiler.MutexProfile)
		case "GOROUTINE":
			profileTypes = append(profileTypes, profiler.GoroutineProfile)
		default:
			return errors.NewTektiteErrorf(errors.InvalidConfiguration, "Unknown Datadog profile type: %s", sProfType)
		}
	}

	agentAddress := fmt.Sprintf("%s:%d", ddHost, s.conf.DDProfilerPort)

	log.Debugf("starting Datadog continuous profiler with service name: %s environment %s version %s agent address %s profile types %s",
		s.conf.DDProfilerServiceName, s.conf.DDProfilerEnvironmentName, s.conf.DDProfilerVersionName, agentAddress, ddProfileTypes)

	return profiler.Start(
		profiler.WithService(s.conf.DDProfilerServiceName),
		profiler.WithEnv(s.conf.DDProfilerEnvironmentName),
		profiler.WithVersion(s.conf.DDProfilerVersionName),
		profiler.WithAgentAddr(agentAddress),
		profiler.WithProfileTypes(profileTypes...),
	)
}

func (s *Server) Shutdown() error {
	return s.stop(true)
}

func (s *Server) Stop() error {
	return s.stop(false)
}

func (s *Server) stop(shutdown bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		return nil
	}
	if s.conf.DDProfilerTypes != "" {
		profiler.Stop()
	}
	s.lifeCycleMgr.SetActive(false)
	// We stop the version manager first to ensure it doesn't attempt to get new sequence ids while the server is
	// shutting down which can make the release of the sequences cluster lock fail, leaving it applied
	if err := s.versionManager.Stop(); err != nil {
		panic(err) // Will never error
	}
	for i := len(s.services) - 1; i >= 0; i-- {
		serv := s.services[i]
		if !serviceIsNil(serv) {
			servStopStart := time.Now()
			log.Debugf("tektite node %d stopping service %s", s.nodeID, reflect.TypeOf(serv).String())
			if err := serv.Stop(); err != nil {
				return errors.WithStack(err)
			}
			log.Debugf("tektite node %d service %s stopping took %d ms", s.nodeID, reflect.TypeOf(serv).String(),
				time.Now().Sub(servStopStart).Milliseconds())
		}
	}
	s.stopped = true
	if s.stopWaitGroup != nil {
		// This lets main exit
		s.stopWaitGroup.Done()
	}
	if shutdown {
		s.shutdownComplete = true
	}
	log.Infof("tektite server %d stopped", s.nodeID)
	return nil
}

func serviceIsNil(s service) bool {
	return s == nil || reflect.ValueOf(s).IsNil()
}

func (s *Server) GetConfig() conf.Config {
	return s.conf
}

func (s *Server) GetProcessorManager() proc.Manager {
	return s.processorManager
}

func (s *Server) GetStreamManager() opers.StreamManager {
	return s.streamManager
}

func (s *Server) GetLevelManager() *levels.LevelManager {
	return s.levelManagerService.GetLevelManager()
}

func (s *Server) GetQueryManager() query.Manager {
	return s.queryManager
}

func (s *Server) GetStore() *store.Store {
	return s.store
}

func (s *Server) SetStopWaitGroup(waitGroup *sync.WaitGroup) {
	s.stopWaitGroup = waitGroup
}

type clusterManagerLockManager struct {
	lockTimeout    time.Duration
	clustMgrClient clustmgr.Client
}

func (c *clusterManagerLockManager) GetLock(lockName string) (bool, error) {
	return c.clustMgrClient.GetLock(lockName, c.lockTimeout)
}

func (c *clusterManagerLockManager) ReleaseLock(lockName string) (bool, error) {
	err := c.clustMgrClient.ReleaseLock(lockName)
	return true, err
}

type batchHandlerFactory struct {
	cfg                      *conf.Config
	streamManager            opers.StreamManager
	levelManagerBatchHandler proc.BatchHandler
}

func (bh *batchHandlerFactory) createBatchHandler(processorID int) proc.BatchHandler {
	if bh.cfg.LevelManagerEnabled && processorID >= bh.cfg.ProcessorCount {
		return bh.levelManagerBatchHandler
	} else {
		return bh.streamManager
	}
}

type levelManagerFlushNotifier struct {
	levelMgrService *levels.LevelManagerService
}

func (d *levelManagerFlushNotifier) AddFlushedCallback(callback func(err error)) {
	if err := d.levelMgrService.AddFlushedCallback(callback); err != nil {
		log.Errorf("failed to add levelManager flushed callback %v", err)
	}
}

type kafkaProcesorProvider struct {
	procMgr       proc.Manager
	streamManager opers.StreamManager
}

func (k *kafkaProcesorProvider) GetProcessorForPartition(topicName string, partitionID int) (proc.Processor, bool) {
	endpointInfo := k.streamManager.GetKafkaEndpoint(topicName)
	if endpointInfo == nil || endpointInfo.InEndpoint == nil {
		return nil, false
	}
	processorID, ok := endpointInfo.InEndpoint.GetPartitionProcessorMapping()[partitionID]
	if !ok {
		return nil, false
	}
	return k.procMgr.GetProcessor(processorID)
}

func (k *kafkaProcesorProvider) GetProcessor(processorID int) (proc.Processor, bool) {
	return k.procMgr.GetProcessor(processorID)
}

func (k *kafkaProcesorProvider) NodeForPartition(partitionID int, mappingID string, partitionCount int) int {
	return k.procMgr.NodeForPartition(partitionID, mappingID, partitionCount)
}

type localLevelManagerClientFactory struct {
	cfg     *conf.Config
	procMgr proc.Manager
}

func (l *localLevelManagerClientFactory) CreateLevelManagerClient() levels.Client {
	client := proc.NewLevelManagerLocalClient(l.cfg)
	client.SetProcessorManager(l.procMgr)
	return client
}

type externalLevelManagerClientFactory struct {
	cfg *conf.Config
}

func (e *externalLevelManagerClientFactory) CreateLevelManagerClient() levels.Client {
	return levels.NewExternalClient(e.cfg.ExternalLevelManagerAddresses,
		e.cfg.ExternalLevelManagerTlsConfig, e.cfg.LevelManagerRetryDelay)
}

type wasmFunctionChecker struct {
	moduleManager *wasm.ModuleManager
}

func (w *wasmFunctionChecker) FunctionExists(functionName string) bool {
	_, ok, err := w.moduleManager.GetFunctionMetadata(functionName)
	return ok && err == nil
}
