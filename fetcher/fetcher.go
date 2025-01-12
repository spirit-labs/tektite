package fetcher

import (
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
)

/*
BatchFetcher handles Kafka FetchRequests and implements the read path in Tektite. There is one of these on every Agent.
When a request arrives, for each partition in the request, BatchFetcher must determine which SSTables contain the data
of interest for offset >= fetchOffset. First it looks in 'recent tables' - this is a per partition cache of SSTable ids
and lastReadableOffset for each partition that is maintained. If it has enough information from there it uses those ids,
which would typically be the case for reads of very recently produced data. If not, which would typically be the case
for historic consumers, a query is sent to the controller to request ids for the key range of interest.
Once ids have been obtained and lastReadableOffset is known, an iterator can be created using LazySSTableIterators - which
lazily pull SSTables as they are iterated over. The iterator is iterated over to obtain the recordset(s) and the partition
response is prepared and returned to the caller.
When iterating, SSTables are first looked for in a small local cache of SSTables. We maintain this because it's common
that recent data could be fetched for multiple partitions, and that data could live in the same table. Caching the table
locally saves multiple possibly remote calls to other agents to retrieve the table from the fetch cache.
If not found locally the table is requested from the fetch cache. This is a distributed cache, spread across all agents
in the same AZ. The distributed cache will get the table from object store if it doesn't have it.
Recent consumers - i.e. ones that don't lag very far behind the latest offset in a partition are the most common. We
wish to avoid going to the controller to request table ids every time a recent fetch request arrives. We therefore
cache the most recently registered SSTable ids locally in PartitionRecentTables. When a fetch request arrives it can
then inspect the ids there and has no need to go to the controller. Whenever a new table is registered with the
controller and lastReadableOffset is updated it sends a notification to all agents that might be interested. This updates
the cache of ids in PartitionRecentTables.
*/
type BatchFetcher struct {
	objStore           objstore.Client
	topicProvider      topicInfoProvider
	partitionHashes    *parthash.PartitionHashes
	controlFactory     control.ClientFactory
	tableGetter        sst.TableGetter
	recentTables       PartitionRecentTables
	controlClientCache *control.ClientCache
	dataBucketName     string
	readExecs          []readExecutor
	localCache         *LocalSSTCache
	execAssignPos      int64
	resetSequence      int64
	memberID           int32
	compressionType    compress.CompressionType
}

func NewBatchFetcher(objStore objstore.Client, topicProvider topicInfoProvider, partitionHashes *parthash.PartitionHashes,
	controlClientCache *control.ClientCache, tableGetter sst.TableGetter, cfg Conf) (*BatchFetcher, error) {
	localCache, err := NewLocalSSTCache(cfg.LocalCacheNumEntries, cfg.LocalCacheMaxBytes)
	if err != nil {
		return nil, err
	}
	bf := &BatchFetcher{
		objStore:           objStore,
		topicProvider:      topicProvider,
		partitionHashes:    partitionHashes,
		controlClientCache: controlClientCache,
		tableGetter:        tableGetter,
		readExecs:          make([]readExecutor, cfg.NumReadExecutors),
		localCache:         localCache,
		dataBucketName:     cfg.DataBucketName,
		memberID:           -1,
		compressionType:    cfg.FetchCompressionType,
	}
	bf.recentTables = CreatePartitionRecentTables(cfg.MaxCachedTablesPerPartition, bf)
	return bf, nil
}

type Conf struct {
	DataBucketName              string
	MaxCachedTablesPerPartition int
	NumReadExecutors            int
	LocalCacheNumEntries        int
	LocalCacheMaxBytes          int
	FetchCompressionType        compress.CompressionType
}

func NewConf() Conf {
	return Conf{
		DataBucketName:              DefaultDataBucketName,
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
	DefaultMaxCachedTablesPerPartition = 100
	DefaultNumReadExecutors            = 8
	DefaultLocalCacheNumEntries        = 10
	DefaultLocalCacheMaxBytes          = 128 * 1024 * 1024 // 128MiB
	readExecChannelSize                = 10
	defaultFetchMaxBytes               = 1024 * 1024
)

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error)
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

func (b *BatchFetcher) HandleTableRegisteredNotification(_ *transport.ConnectionContext, request []byte,
	_ []byte, _ transport.ResponseWriter) error {
	notif := &control.TablesRegisteredNotification{}
	notif.Deserialize(request, 0)
	return b.recentTables.handleTableRegisteredNotification(notif)
}

func (b *BatchFetcher) HandleFetchRequest(authContext *auth.Context, apiVersion int16, req *kafkaprotocol.FetchRequest,
	completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	if apiVersion < 3 {
		// Version 3 of api introduces max bytes, so we default it for earlier versions
		req.MaxBytes = defaultFetchMaxBytes
	}
	pos := atomic.AddInt64(&b.execAssignPos, 1)
	readExec := &b.readExecs[pos%int64(len(b.readExecs))]
	// No need to shuffle partitions as golang map has non-deterministic iteration order - this ensures we don't have
	// the same partition getting all the data and others starving
	fetchState, err := newFetchState(authContext, b, req, readExec, completionFunc)
	if err != nil {
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

func (b *BatchFetcher) getClient() (control.Client, error) {
	return b.controlClientCache.GetClient()
}

func (b *BatchFetcher) MembershipChanged(thisMemberID int32, membership cluster.MembershipState) error {
	atomic.StoreInt32(&b.memberID, thisMemberID)
	b.recentTables.membershipChanged(membership)
	return nil
}

type readExecutor struct {
	lock    sync.Mutex
	stopped bool
	ch      chan *FetchState
	stopWG  sync.WaitGroup
}

func (r *readExecutor) execFetchState(fs *FetchState) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.stopped {
		return
	}
	r.ch <- fs
}

func (r *readExecutor) start() {
	r.stopWG.Add(1)
	go r.loop()
}

func (r *readExecutor) stop() {
	r.closeChannel()
	r.stopWG.Wait()
}

func (r *readExecutor) closeChannel() {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.ch)
	r.stopped = true
}

func (r *readExecutor) loop() {
	defer r.stopWG.Done()
	for fs := range r.ch {
		if err := fs.read(); err != nil {
			log.Errorf("failed to execute read on fetch state: %v", err)
		}
	}
}
