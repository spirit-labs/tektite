package fetcher

import (
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"sync"
	"sync/atomic"
)

type BatchFetcher struct {
	objStore        objstore.Client
	topicProvider   topicInfoProvider
	partitionHashes *parthash.PartitionHashes
	controlFactory  controllerClientFactory
	tableGetter     sst.TableGetter
	recentTables    PartitionRecentTables
	controlClients  *clientCache
	dataBucketName  string
	readExecs       []readExecutor
	localCache      *LocalSSTCache
	execAssignPos   int64
}

func NewBatchFetcher(objStore objstore.Client, topicProvider topicInfoProvider, partitionHashes *parthash.PartitionHashes,
	controlFactory controllerClientFactory, tableGetter sst.TableGetter, cfg Conf) (*BatchFetcher, error) {
	localCache, err := NewLocalSSTCache(cfg.LocalCacheNumEntries, cfg.LocalCacheMaxBytes)
	if err != nil {
		return nil, err
	}
	return &BatchFetcher{
		objStore:        objStore,
		topicProvider:   topicProvider,
		partitionHashes: partitionHashes,
		controlFactory:  controlFactory,
		tableGetter:     tableGetter,
		recentTables:    CreatePartitionRecentTables(cfg.MaxCachedTablesPerPartition),
		controlClients:  newClientCache(cfg.MaxControllerConnections, controlFactory),
		readExecs:       make([]readExecutor, cfg.NumReadExecutors),
		localCache:      localCache,
		dataBucketName:  cfg.DataBucketName,
	}, nil
}

type Conf struct {
	DataBucketName              string
	MaxControllerConnections    int
	MaxCachedTablesPerPartition int
	NumReadExecutors            int
	LocalCacheNumEntries        int
	LocalCacheMaxBytes          int
}

func NewConf() Conf {
	return Conf{
		DataBucketName:              DefaultDataBucketName,
		MaxControllerConnections:    DefaultMaxControllerConnections,
		MaxCachedTablesPerPartition: DefaultMaxCachedTablesPerPartition,
		NumReadExecutors:            DefaultNumReadExecutors,
		LocalCacheNumEntries:        DefaultLocalCacheNumEntries,
		LocalCacheMaxBytes:          DefaultLocalCacheMaxBytes,
	}
}

func (c *Conf) Validate() error {
	return nil
}

const (
	DefaultDataBucketName              = "tektite-data"
	DefaultMaxControllerConnections    = 10
	DefaultMaxCachedTablesPerPartition = 100
	DefaultNumReadExecutors            = 8
	DefaultLocalCacheNumEntries        = 20
	DefaultLocalCacheMaxBytes          = 256 * 1024 * 1024 // 256MiB
	readExecChannelSize                = 10
)

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, error)
}

type controllerClientFactory func() (ControlClient, error)

type ControlClient interface {
	FetchTablesForPrefix(topicID int, partitionID int, prefix []byte, offsetStart int64) (lsm.OverlappingTables, int64, error)
	Close() error
}

func (b *BatchFetcher) Start() error {
	for i := 0; i < len(b.readExecs); i++ {
		b.readExecs[i].ch = make(chan *FetchState, readExecChannelSize)
		b.readExecs[i].start()
	}
	return nil
}

func (b *BatchFetcher) Stop() error {
	for i := 0; i < len(b.readExecs); i++ {
		b.readExecs[i].stop()
	}
	return nil
}

func (b *BatchFetcher) HandleFetchRequest(req *kafkaprotocol.FetchRequest,
	completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	pos := atomic.AddInt64(&b.execAssignPos, 1)
	readExec := &b.readExecs[pos%int64(len(b.readExecs))]
	// No need to shuffle partitions as golang map has non-deterministic iteration order - this ensures we don't have
	// the same partition getting all the data and others starving
	fetchState := FetchState{
		bf:              b,
		req:             req,
		partitionStates: map[int]map[int]*PartitionFetchState{},
		completionFunc:  completionFunc,
		readExec:        readExec,
	}
	if err := fetchState.init(); err != nil {
		return err
	}
	if len(fetchState.partitionStates) == 0 {
		// all errored
		return completionFunc(&fetchState.resp)
	}
	return fetchState.read()
}

func (b *BatchFetcher) getTableFromCache(tableID sst.SSTableID) (*sst.SSTable, error) {
	// First look in local cache
	table, ok := b.localCache.Get(tableID)
	if ok {
		return table, nil
	}
	// Then in distributed cache
	table, err := b.tableGetter(tableID)
	if err != nil {
		return nil, err
	}
	// Add to local cache
	b.localCache.Put(tableID, table)
	return table, nil
}

func (b *BatchFetcher) getClient() (ControlClient, error) {
	return b.controlClients.getClient()
}

type readExecutor struct {
	ch     chan *FetchState
	stopWG sync.WaitGroup
}

func (r *readExecutor) start() {
	r.stopWG.Add(1)
	go r.loop()
}

func (r *readExecutor) stop() {
	close(r.ch)
	r.stopWG.Wait()
}

func (r *readExecutor) loop() {
	defer r.stopWG.Done()
	for fs := range r.ch {
		if err := fs.read(); err != nil {
			log.Errorf("failed to execute read on fetch state: %v", err)
		}
	}
}
