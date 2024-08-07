package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"html/template"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type Server struct {
	lock                      sync.Mutex
	cfg                       *conf.Config
	httpServer                *http.Server
	levelMgrClient            levels.Client
	streamManager             opers.StreamManager
	procManager               proc.Manager
	listener                  net.Listener
	closeWg                   sync.WaitGroup
	homeTemplate              *template.Template
	databaseTemplate          *template.Template
	topicsTemplate            *template.Template
	streamsTemplate           *template.Template
	configTemplate            *template.Template
	clusterTemplate           *template.Template
	dbStats                   *databaseStats
	lastDbStatsRequestTime    time.Time
	topicsData                []topicData
	lastTopicsDataRequestTime time.Time
	streamsData               []streamData
	lastStreamsRequestTime    time.Time
	clusterData               *clusterData
	lastClusterRequestTime    time.Time
	startTime                 uint64
}

func NewServer(cfg *conf.Config, levelMgrClient levels.Client, streamManager opers.StreamManager, procManager proc.Manager) (*Server, error) {
	funcMap := template.FuncMap{
		"format_bytes": formatBytes,
	}
	homeTemplate, err := template.New("home").Funcs(funcMap).Parse(homeTemplate)
	if err != nil {
		return nil, err
	}
	databaseTemplate, err := template.New("database").Funcs(funcMap).Parse(databaseTemplate)
	if err != nil {
		return nil, err
	}
	topicsTemplate, err := template.New("topics").Funcs(funcMap).Parse(topicsTemplate)
	if err != nil {
		return nil, err
	}
	streamsTemplate, err := template.New("streams").Funcs(funcMap).Parse(streamsTemplate)
	if err != nil {
		return nil, err
	}
	configTemplate, err := template.New("config").Funcs(funcMap).Parse(configTemplate)
	if err != nil {
		return nil, err
	}
	clusterTemplate, err := template.New("cluster").Funcs(funcMap).Parse(clusterTemplate)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg:              cfg,
		levelMgrClient:   levelMgrClient,
		streamManager:    streamManager,
		procManager:      procManager,
		homeTemplate:     homeTemplate,
		databaseTemplate: databaseTemplate,
		topicsTemplate:   topicsTemplate,
		streamsTemplate:  streamsTemplate,
		configTemplate:   configTemplate,
		clusterTemplate:  clusterTemplate,
		startTime:        arista.NanoTime(),
	}, nil
}

func (s *Server) Start() error {
	tlsConf, err := conf.CreateServerTLSConfig(s.cfg.AdminConsoleTLSConfig)
	if err != nil {
		return err
	}
	s.httpServer = &http.Server{
		IdleTimeout: 0,
		TLSConfig:   tlsConf,
	}
	mux := http.NewServeMux()
	s.httpServer.Handler = mux
	mux.HandleFunc("/", s.ServeHome)
	mux.HandleFunc("/database", s.ServeDatabase)
	mux.HandleFunc("/topics", s.ServeTopics)
	mux.HandleFunc("/streams", s.ServeStreams)
	mux.HandleFunc("/config", s.ServeConfig)
	mux.HandleFunc("/cluster", s.ServeCluster)

	listenAddress := s.cfg.AdminConsoleAddresses[s.cfg.NodeID]
	s.listener, err = common.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	s.closeWg = sync.WaitGroup{}
	s.closeWg.Add(1)
	common.Go(func() {
		defer s.closeWg.Done()
		if tlsConf != nil {
			err = s.httpServer.ServeTLS(s.listener, "", "")
		} else {
			err = s.httpServer.Serve(s.listener)
		}
		if !errors.Is(http.ErrServerClosed, err) {
			log.Errorf("Failed to start the Web UI server: %v", err)
		}
	})
	return err
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	if err := s.listener.Close(); err != nil {
		// Ignore
	}
	s.closeWg.Wait()
	return nil
}

type homeData struct {
	Date   string
	Memory uint64
	Uptime string
}

func (s *Server) ServeHome(response http.ResponseWriter, _ *http.Request) {

	now := time.Now().Format("2006-01-02 15:04:05")
	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)
	memUsage := memStats.Alloc / 1024 / 1024 // Memory usage in MB
	ut := time.Duration(arista.NanoTime() - s.startTime)
	ut = 1e9 * (ut / 1e9) // round to seconds
	data := homeData{
		Date:   now,
		Memory: memUsage,
		Uptime: ut.String(),
	}

	html := strings.Builder{}
	err := s.homeTemplate.Execute(&html, data)
	if err != nil {
		s.handleError(err, response)
		return
	}

	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

type databaseStats struct {
	LevelMgrStats levels.Stats
	LevelStats    map[int]*levels.LevelStats
}

func (s *Server) ServeDatabase(response http.ResponseWriter, _ *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()
	dbStats, err := s.getDatabaseStats()
	if err != nil {
		s.handleError(err, response)
		return
	}
	html := strings.Builder{}
	err = s.databaseTemplate.Execute(&html, dbStats)
	if err != nil {
		s.handleError(err, response)
		return
	}
	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

func (s *Server) getDatabaseStats() (*databaseStats, error) {
	now := time.Now()
	if now.Sub(s.lastDbStatsRequestTime) < s.cfg.AdminConsoleSampleInterval {
		// We cache the value so as not to overwhelm the level manager with requests
		return s.dbStats, nil
	}
	stats, err := s.levelMgrClient.GetStats()
	if err != nil {
		return nil, err
	}
	maxLevel := 0
	for level := range stats.LevelStats {
		if level > maxLevel {
			maxLevel = level
		}
	}
	//levelStats := make([]*levels.LevelStats, maxLevel+1)
	//for level, ls := range stats.LevelStats {
	//	levelStats[level] = ls
	//}
	s.dbStats = &databaseStats{
		LevelMgrStats: stats,
		LevelStats:    stats.LevelStats,
	}
	s.lastDbStatsRequestTime = now
	return s.dbStats, nil
}

type topicData struct {
	Name       string
	Partitions int
	RW         string
}

func (s *Server) getTopicsData() []topicData {
	now := time.Now()
	if now.Sub(s.lastTopicsDataRequestTime) < s.cfg.AdminConsoleSampleInterval {
		// We cache the value so as not to overwhelm the stream manager with requests
		return s.topicsData
	}
	endpoints := s.streamManager.GetAllKafkaEndpoints()
	topicDatas := make([]topicData, len(endpoints))
	for i, endpoint := range endpoints {
		var rw string
		if endpoint.InEndpoint != nil && endpoint.OutEndpoint != nil {
			rw = "rw"
		} else if endpoint.InEndpoint != nil {
			rw = "w"
		} else {
			rw = "r"
		}
		topicDatas[i] = topicData{
			Name:       endpoint.Name,
			Partitions: endpoint.Schema.Partitions,
			RW:         rw,
		}
	}
	// sort by topic name
	sort.SliceStable(topicDatas, func(i, j int) bool {
		return strings.Compare(topicDatas[i].Name, topicDatas[j].Name) < 0
	})
	s.topicsData = topicDatas
	s.lastTopicsDataRequestTime = now
	return s.topicsData
}

func (s *Server) ServeTopics(response http.ResponseWriter, _ *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()

	topicsData := s.getTopicsData()

	html := strings.Builder{}
	err := s.topicsTemplate.Execute(&html, topicsData)
	if err != nil {
		s.handleError(err, response)
		return
	}

	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

type streamData struct {
	Name          string
	StreamDef     string
	InSchema      string
	OutSchema     string
	InPartitions  int
	InMapping     string
	OutPartitions int
	OutMapping    string
	ChildStreams  string
}

func (s *Server) getStreamsData() []streamData {
	now := time.Now()
	if now.Sub(s.lastStreamsRequestTime) < s.cfg.AdminConsoleSampleInterval {
		return s.streamsData
	}
	allStreams := s.streamManager.GetAllStreams()
	var streamsData []streamData
	for _, stream := range allStreams {
		if stream.SystemStream {
			continue
		}
		var childSlice []string
		for childStream := range stream.DownstreamStreamNames {
			childSlice = append(childSlice, childStream)
		}
		var childStr string
		if len(childSlice) > 0 {
			sort.Strings(childSlice)
			var childBuilder strings.Builder
			for i, childStream := range childSlice {
				childBuilder.WriteString(childStream)
				if i != len(childSlice)-1 {
					childBuilder.WriteString(", ")
				}
			}
			childStr = childBuilder.String()
		}
		sd := streamData{
			Name:          stream.StreamDesc.StreamName,
			StreamDef:     opers.ExtractStreamDefinition(stream.Tsl),
			InSchema:      stream.InSchema.EventSchema.String(),
			OutSchema:     stream.OutSchema.EventSchema.String(),
			InPartitions:  stream.InSchema.Partitions,
			InMapping:     stream.InSchema.MappingID,
			OutPartitions: stream.OutSchema.Partitions,
			OutMapping:    stream.OutSchema.MappingID,
			ChildStreams:  childStr,
		}
		streamsData = append(streamsData, sd)
	}
	// sort by stream name
	sort.SliceStable(streamsData, func(i, j int) bool {
		return strings.Compare(streamsData[i].Name, streamsData[j].Name) < 0
	})
	s.streamsData = streamsData
	s.lastStreamsRequestTime = now
	return s.streamsData
}

func (s *Server) ServeStreams(response http.ResponseWriter, _ *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()

	streamsData := s.getStreamsData()
	html := strings.Builder{}
	err := s.streamsTemplate.Execute(&html, streamsData)
	if err != nil {
		s.handleError(err, response)
		return
	}

	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

func (s *Server) ServeConfig(response http.ResponseWriter, _ *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()

	html := strings.Builder{}
	err := s.configTemplate.Execute(&html, s.cfg.Original)
	if err != nil {
		s.handleError(err, response)
		return
	}

	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

type clusterData struct {
	NodeCount int
	LiveNodes []int
	NodesData []*nodeData
}

type nodeData struct {
	NodeID       int
	ReplicaCount int
	LeaderCount  int
	Processors   []processorInfo
}

type processorInfo struct {
	ProcessorID int
	Leader      bool
	Backup      bool
	Synced      bool
}

func (s *Server) getClusterData() *clusterData {
	now := time.Now()
	if now.Sub(s.lastClusterRequestTime) < s.cfg.AdminConsoleSampleInterval {
		return s.clusterData
	}
	numNodes := len(s.cfg.ClusterAddresses)
	procCount := s.cfg.ProcessorCount
	if s.cfg.LevelManagerEnabled {
		procCount++
	}
	nodes := make([]*nodeData, numNodes)
	for i := 0; i < procCount; i++ {
		gs, ok := s.procManager.GetGroupState(i)
		if ok {
			for _, gn := range gs.GroupNodes {
				nd := nodes[gn.NodeID]
				if nd == nil {
					nd = &nodeData{NodeID: gn.NodeID}
					nodes[gn.NodeID] = nd
				}
				nd.ReplicaCount++
				if gn.Leader {
					nd.LeaderCount++
				}
				nd.Processors = append(nd.Processors, processorInfo{
					ProcessorID: i,
					Leader:      gn.Leader,
					Backup:      !gn.Leader,
					Synced:      gn.Valid,
				})
			}
		}
	}
	var liveNodeIDs []int
	var newNodes []*nodeData
	for _, nd := range nodes {
		if nd != nil {
			newNodes = append(newNodes, nd)
			liveNodeIDs = append(liveNodeIDs, nd.NodeID)
		}
	}
	sort.Ints(liveNodeIDs)
	sort.SliceStable(newNodes, func(i, j int) bool {
		return newNodes[i].NodeID < newNodes[j].NodeID
	})
	s.clusterData = &clusterData{
		NodeCount: numNodes,
		LiveNodes: liveNodeIDs,
		NodesData: newNodes,
	}
	s.lastClusterRequestTime = now
	return s.clusterData
}

func (s *Server) ServeCluster(response http.ResponseWriter, _ *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clusterData := s.getClusterData()
	html := strings.Builder{}
	err := s.clusterTemplate.Execute(&html, clusterData)
	if err != nil {
		s.handleError(err, response)
		return
	}
	response.Header().Set("Content-Type", "text/html")
	_, err = response.Write([]byte(html.String()))
	if err != nil {
		log.Errorf("failed to write admin response: %v", err)
	}
}

func (s *Server) handleError(err error, response http.ResponseWriter) {
	log.Errorf("failed to server web ui: %v", err)
	http.Error(response, "internal error when serving web-ui, please consult server logs for further details", http.StatusInternalServerError)
}

func formatBytes(bytes int) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
		tb = gb * 1024
		pb = tb * 1024
	)
	switch {
	case bytes >= pb:
		return fmt.Sprintf("%.2f PiB (%d bytes)", float64(bytes)/float64(pb), bytes)
	case bytes >= tb:
		return fmt.Sprintf("%.2f TiB (%d bytes)", float64(bytes)/float64(tb), bytes)
	case bytes >= gb:
		return fmt.Sprintf("%.2f GiB (%d bytes)", float64(bytes)/float64(gb), bytes)
	case bytes >= mb:
		return fmt.Sprintf("%.2f MiB (%d bytes)", float64(bytes)/float64(mb), bytes)
	case bytes >= kb:
		return fmt.Sprintf("%.2f KiB (%d bytes)", float64(bytes)/float64(kb), bytes)
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}
