package server

import (
	"fmt"
	"github.com/spirit-labs/tektite/admin"
	"github.com/spirit-labs/tektite/asl/api"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/cmdmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/kafka"
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
	"github.com/spirit-labs/tektite/server"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/spirit-labs/tektite/wasm"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	_ "net/http/pprof"
	"sync"

	log "github.com/spirit-labs/tektite/logger"
)

func NewServer(config conf.Config) (*Server, error) {
	var cf kafka.ClientFactory
	switch config.ClientType {
	case conf.KafkaClientTypeConfluent:
		cf = kafka.NewMessageProviderFactory
	default:
		return nil, common.NewTektiteErrorf(common.InvalidConfiguration,
			"unexpected ClientType: %d", config.ClientType)
	}
	return NewServerWithClientFactory(config, cf)
}

func NewServerWithClientFactory(config conf.Config, clientFactory kafka.ClientFactory) (*Server, error) {
	standalone := len(config.ClusterAddresses) == 1
	var clustStateMgr clustmgr.StateManager
	if standalone {
		clustStateMgr = clustmgr.NewFixedStateManager(config.ProcessorCount+1, 1)
	} else {
		clustStateMgr = clustmgr.NewClusteredStateManager(config.ClusterManagerKeyPrefix, config.ClusterName, config.NodeID,
			config.ClusterManagerAddresses, config.ClusterEvictionTimeout, config.ClusterStateUpdateInterval,
			config.EtcdCallTimeout, config.ProcessorCount+1, config.MaxReplicas, config.LogScope)
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
		return nil, common.NewTektiteErrorf(common.InvalidConfiguration, "invalid object store type: %s", config.ObjectStoreType)
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
	return NewServerWithArgs(config, clientFactory, objStoreClient, clustStateMgr, lockManager)
}

func NewServerWithArgs(config conf.Config, clientFactory kafka.ClientFactory, objStoreClient objstore.Client,
	clustStateMgr clustmgr.StateManager, lockMgr lock.Manager) (*Server, error) {

	if err := config.Validate(); err != nil {
		return nil, errwrap.WithStack(err)
	}

	standalone := len(config.ClusterAddresses) == 1

	var levelManagerClientFactory levels.ClientFactory
	var localLevMgrClientFactory *localLevelManagerClientFactory
	if config.LevelManagerEnabled {
		localLevMgrClientFactory = &localLevelManagerClientFactory{cfg: &config}
		levelManagerClientFactory = localLevMgrClientFactory
	} else {
		levelManagerClientFactory = &externalLevelManagerClientFactory{cfg: &config}
	}

	sequenceManager := sequence.NewSequenceManager(objStoreClient, config.SequencesObjectName, lockMgr,
		config.SequencesRetryDelay, config.BucketName)
	tableCache, err := tabcache.NewTableCache(objStoreClient, &config)
	if err != nil {
		return nil, err
	}

	// We use the same instance of a level manager client for the store and prefix retentions service
	levMgrClient := levelManagerClientFactory.CreateLevelManagerClient()

	var compactionService *levels.CompactionWorkerService
	if config.CompactionWorkersEnabled {
		// each compaction worker has its own level manager client to avoid contention, so we pass in the factory
		compactionService = levels.NewCompactionWorkerService(&config, levelManagerClientFactory, tableCache, objStoreClient, true)
	}

	remotingServer := remoting.NewServer(config.ClusterAddresses[config.NodeID], config.ClusterTlsConfig)

	versionManager := vmgr.NewVersionManager(sequenceManager, levMgrClient, &config, config.ClusterAddresses...)

	moduleManager := wasm.NewModuleManager(objStoreClient, lockMgr, &config)
	invokerFactory := &wasm.InvokerFactory{ModManager: moduleManager}
	exprFactory := &expr.ExpressionFactory{ExternalInvokerFactory: invokerFactory}

	theParser := parser.NewParser(&wasmFunctionChecker{moduleManager})

	streamManager := opers.NewStreamManager(clientFactory, levMgrClient, exprFactory, &config, false)

	handlerFactory := &batchHandlerFactory{
		cfg:           &config,
		streamManager: streamManager,
	}
	flushNotifier := &levelManagerFlushNotifier{}
	vmgrClient := proc.NewVmgrClient(&config)
	processorManager := proc.NewProcessorManagerWithVmgrClient(clustStateMgr, streamManager,
		&config, repli.NewReplicator, handlerFactory.createBatchHandler, flushNotifier, streamManager,
		vmgrClient, objStoreClient, levMgrClient, tableCache, !config.FailureDisabled)
	vmgrClient.SetProcessorManager(processorManager)
	clustStateMgr.SetClusterStateHandler(processorManager.HandleClusterState)
	processorManager.RegisterStateHandler(versionManager.HandleClusterState)

	streamManager.SetProcessorManager(processorManager)

	queryManager := query.NewManager(processorManager, processorManager, config.NodeID, streamManager,
		streamManager.StreamMetaIteratorProvider(), processorManager, query.NewDefaultRemoting(&config), config.ClusterAddresses,
		config.QueryMaxBatchRows, exprFactory, theParser)

	levelManagerService := levels.NewLevelManagerService(processorManager, &config, objStoreClient, tableCache,
		proc.NewLevelManagerCommandIngestor(processorManager), processorManager)

	processorManager.RegisterStateHandler(levelManagerService.HandleClusterState)

	processorManager.SetLevelMgrProcessorInitialisedCallback(levelManagerService.ActivateLevelManager)

	handlerFactory.levelManagerBatchHandler = proc.NewLevelManagerBatchHandler(levelManagerService)
	flushNotifier.levelMgrService = levelManagerService

	commandMgr := cmdmgr.NewCommandManager(streamManager, queryManager, sequenceManager, lockMgr,
		processorManager, processorManager.VersionManagerClient(), theParser, &config)
	var commandSignaller *cmdmgr.RemotingSignaller
	if !standalone {
		commandSignaller = cmdmgr.NewSignaller(commandMgr, remotingServer, &config)
		commandMgr.SetCommandSignaller(commandSignaller)
	}
	processorManager.RegisterStateHandler(commandMgr.HandleClusterState)

	scramManager, err := auth.NewScramManager(streamManager, processorManager, theParser, queryManager, &config, auth.ScramAuthTypeSHA256)
	if err != nil {
		return nil, err
	}

	var apiServer *api.HTTPAPIServer
	if config.HttpApiEnabled {
		apiServer = api.NewHTTPAPIServer(config.NodeID, config.HttpApiAddresses, config.HttpApiPath,
			queryManager, commandMgr, theParser, moduleManager, scramManager, config.HttpApiTlsConfig,
			config.AuthenticationEnabled, config.AuthenticationCacheTimeout)
	}

	var kafkaServer *kafkaserver.Server
	var kafkaGroupCoordinator *kafkaserver.GroupCoordinator
	if config.KafkaServerEnabled {
		metaProvider, err := kafkaserver.NewMetaDataProvider(&config, processorManager, streamManager)
		if err != nil {
			return nil, err
		}
		kafkaGroupCoordinator, err = kafkaserver.NewGroupCoordinator(&config, processorManager, streamManager,
			metaProvider, processorManager)
		if err != nil {
			return nil, err
		}
		kafkaServer, err = kafkaserver.NewServer(&config, metaProvider, processorManager, kafkaGroupCoordinator, streamManager, scramManager, sequenceManager)
		if err != nil {
			return nil, err
		}
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
		vmgrClient,
		streamManager,
		queryManager,
		moduleManager,
		commandMgr,
		commandSignaller,
		apiServer,
		scramManager,
		kafkaGroupCoordinator,
		kafkaServer,
		compactionService,
		adminServer,
	}

	s := &Server{
		conf:                config,
		nodeID:              config.NodeID,
		remotingServer:      remotingServer,
		services:            services,
		lockManager:         lockMgr,
		sequenceManager:     sequenceManager,
		processorManager:    processorManager,
		commandManager:      commandMgr,
		clusterStateManager: clustStateMgr,
		streamManager:       streamManager,
		queryManager:        queryManager,
		levelManagerService: levelManagerService,
		versionManager:      versionManager,
		kafkaServer:         kafkaServer,
		apiServer:           apiServer,
		scramManager:        scramManager,
		levelMgrClient:      levMgrClient,
	}
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageShutdownMessage,
		server.NewShutdownMessageHandler(processorManager, streamManager, versionManager, levelManagerService,
			clustStateMgr, s.Shutdown, config.NodeID))
	return s, nil
}

type Server struct {
	lock                sync.RWMutex
	nodeID              int
	remotingServer      remoting.Server
	services            []service
	stopped             bool
	conf                conf.Config
	processorManager    proc.Manager
	commandManager      cmdmgr.Manager
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
	levelMgrClient      levels.Client
	scramManager        *auth.ScramManager
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

	log.Infof("%s: starting tektite server %d", s.conf.LogScope, s.nodeID)

	if s.conf.DebugServerEnabled {
		if s.conf.NodeID >= len(s.conf.DebugServerAddresses) {
			return common.NewTektiteErrorf(common.InvalidConfiguration, "no entry in DebugServerAddresses for node id %d", s.conf.NodeID)
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
			log.Infof("node %d starting service %s", s.nodeID, reflect.TypeOf(serv).String())
			start := time.Now()
			if err = serv.Start(); err != nil {
				return errwrap.WithStack(err)
			}
			log.Infof("node %d service %s starting took %d ms", s.nodeID, reflect.TypeOf(serv).String(),
				time.Now().Sub(start).Milliseconds())
		}
	}

	// We delay starting kafka and api server until replicators are ready
	if err := s.processorManager.EnsureReplicatorsReady(); err != nil {
		return err
	}

	// We must activate command manager - prompting it to load commands *after* replicators are ready or can
	// cause processing of batches (e.g. for storing compaction results or deleting slabs) to fail
	if err := s.commandManager.Activate(); err != nil {
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

	log.Infof("%s: tektite server %d started", s.conf.LogScope, s.nodeID)
	return nil
}

func (s *Server) maybeEnabledDatadogProfiler() error {
	ddProfileTypes := s.conf.DDProfilerTypes
	if ddProfileTypes == "" {
		return nil
	}

	ddHost := os.Getenv(s.conf.DDProfilerHostEnvVarName)
	if ddHost == "" {
		return common.NewTektiteErrorf(common.InvalidConfiguration, "Env var %s for DD profiler host is not set", s.conf.DDProfilerHostEnvVarName)
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
			return common.NewTektiteErrorf(common.InvalidConfiguration, "Unknown Datadog profile type: %s", sProfType)
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

	start := time.Now()
	log.Infof("%s: stopping tektite server %d", s.conf.LogScope, s.nodeID)

	if s.stopped {
		return nil
	}
	if s.conf.DDProfilerTypes != "" {
		profiler.Stop()
	}
	// Stop level manager client first, in case go routines are in retry loop executing level mgr ops
	if err := s.levelMgrClient.Stop(); err != nil {
		return err
	}
	// We stop the version manager before the services to ensure it doesn't attempt to get new sequence ids while the server is
	// shutting down which can make the release of the sequences cluster lock fail, leaving it applied
	if err := s.versionManager.Stop(); err != nil {
		panic(err) // Will never error
	}
	for i := len(s.services) - 1; i >= 0; i-- {
		serv := s.services[i]
		if !serviceIsNil(serv) {
			servStopStart := time.Now()
			log.Infof("tektite node %d stopping service %s", s.nodeID, reflect.TypeOf(serv).String())
			if err := serv.Stop(); err != nil {
				return err
			}
			log.Infof("tektite node %d service %s stopping took %d ms", s.nodeID, reflect.TypeOf(serv).String(),
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
	log.Infof("%s: tektite server %d stopped in %d ms", s.conf.LogScope, s.nodeID, time.Now().Sub(start).Milliseconds())
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

func (s *Server) GetCommandManager() cmdmgr.Manager {
	return s.commandManager
}

func (s *Server) GetKafkaServer() *kafkaserver.Server {
	return s.kafkaServer
}

func (s *Server) GetScramManager() *auth.ScramManager {
	return s.scramManager
}

func (s *Server) GetApiServer() *api.HTTPAPIServer {
	return s.apiServer
}

func (s *Server) SetStopWaitGroup(waitGroup *sync.WaitGroup) {
	s.stopWaitGroup = waitGroup
}

func (s *Server) WasShutdown() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.shutdownComplete
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
