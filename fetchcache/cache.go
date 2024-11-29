package fetchcache

import (
	"encoding/binary"
	"github.com/dgraph-io/ristretto"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/consistent"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
)

/*
Cache is a distributed read cache of byte slices. It used to cache sstables across multiple agents so that fetch
requests for different consumers, requesting partition data from the same sstables, which is extremely common for
recent data, don't go to the object store each time, which would be inefficient and expensive.
In an agent cluster we actually maintain multiple distributed caches - one for each availability zone that the agents
are running in. This is because we do not want cache requests to cross availability zones to reduce cross-AZ costs on AWS.
When requesting an entry from the cache, we use a consistent hash ring to determine the target agent for the entry, if
the target is the same agent it is served directly from there otherwise an RPC is sent to the target node to request the
bytes.
On receipt of the request, the key is looked up in the in-memory cache. We use ristretto for this, which is a highly
concurrent LRU cache. If not found in the cache it is looked up from the object store, added to the cache and returned.
As we use consistent hashing, as agents are added or removed from the cluster, most keys will still map to the same
agents so keys won't be re-requested from the object store. If keys do migrate, then old bytes in cache will eventually
get pushed out by the LRU mechanism.
*/
type Cache struct {
	lock            sync.RWMutex
	objStore        objstore.Client
	connCaches      *transport.ConnCaches
	transportServer transport.Server
	cache           *ristretto.Cache
	consist         *consistent.HashRing
	members         map[int32]cluster.MembershipEntry
	cfg             Conf
	stats           CacheStats
}

type CacheStats struct {
	Misses   int64
	Hits     int64
	Gets     int64
	NotFound int64
}

func NewCache(objStore objstore.Client, connCaches *transport.ConnCaches, transportServer transport.Server,
	cfg Conf) (*Cache, error) {
	tableSizeEstimate := 16 * 1024 * 1024
	if cfg.MaxSizeBytes < tableSizeEstimate {
		return nil, errors.Errorf("fetch cache maxSizeBytes must be >= %d", tableSizeEstimate)
	}
	maxItemsEstimate := cfg.MaxSizeBytes / tableSizeEstimate
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * maxItemsEstimate),
		MaxCost:     int64(cfg.MaxSizeBytes),
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &Cache{
		objStore:        objStore,
		connCaches:      connCaches,
		transportServer: transportServer,
		members:         make(map[int32]cluster.MembershipEntry),
		cache:           cache,
		consist:         consistent.NewConsistentHash(cfg.VirtualFactor),
		cfg:             cfg,
	}, nil
}

type Conf struct {
	MaxSizeBytes        int
	DataBucketName      string
	AzInfo              string
	VirtualFactor       int
	ObjStoreCallTimeout time.Duration
}

func NewConf() Conf {
	return Conf{
		ObjStoreCallTimeout: DefaultObjStoreCallTimeout,
		VirtualFactor:       DefaultVirtualFactor,
		DataBucketName:      DefaultDataBucketName,
		MaxSizeBytes:        DefaultMaxSizeBytes,
	}
}

func (c *Conf) Validate() error {
	return nil
}

const (
	DefaultObjStoreCallTimeout = 5 * time.Second
	DefaultVirtualFactor       = 100
	DefaultDataBucketName      = "tektite-data"
	DefaultMaxSizeBytes        = 128 * 1024 * 1024
)

func (c *Cache) Start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.transportServer.RegisterHandler(transport.HandlerIDFetchCacheGetTableBytes, c.handleGetTableBytes)
}

func (c *Cache) Stop() {
}

func (c *Cache) MembershipChanged(_ int32, membership cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	newMembers := make(map[int32]cluster.MembershipEntry, len(membership.Members))
	for _, member := range membership.Members {
		data := extractMembershipData(&member)
		if data.Location != c.cfg.AzInfo {
			// Each AZ has it's own cache so we don't have cross AZ calls when looking up in cache
			// Here, Az is different so we ignore
			continue
		}
		newMembers[member.ID] = member
		_, exists := c.members[member.ID]
		if !exists {
			// member added
			c.consist.Add(data.ClusterListenAddress)
		}
	}
	for memberID, member := range c.members {
		_, exists := newMembers[memberID]
		if !exists {
			// member removed
			data := extractMembershipData(&member)
			removed := c.consist.Remove(data.ClusterListenAddress)
			if !removed {
				panic("failed to remove member from consistent ring")
			}
		}
	}
	c.members = newMembers
	return nil
}

func extractMembershipData(member *cluster.MembershipEntry) *common.MembershipData {
	var membershipData common.MembershipData
	membershipData.Deserialize(member.Data, 0)
	return &membershipData
}

func (c *Cache) GetTableBytes(key []byte) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	target, ok := c.getTargetForKey(key)
	if !ok {
		return nil, common.NewTektiteErrorf(common.Unavailable, "no cache members")
	}
	if target == c.transportServer.Address() {
		// Target is this node - we can do a direct call
		return c.getFromCache(key)
	}
	conn, err := c.connCaches.GetConnection(target)
	if err != nil {
		return nil, err
	}
	req := createRequestBuffer()
	req = binary.BigEndian.AppendUint32(req, uint32(len(key)))
	req = append(req, key...)
	resp, err := conn.SendRPC(transport.HandlerIDFetchCacheGetTableBytes, req)
	if err != nil {
		// Always close connection on error
		if err := conn.Close(); err != nil {
			// Ignore
		}
		return nil, err
	}
	lb := binary.BigEndian.Uint32(resp)
	bytes := make([]byte, lb)
	copy(bytes, resp[4:4+lb])
	return bytes, nil
}

func (c *Cache) getTargetForKey(key []byte) (string, bool) {
	return c.consist.Get(key)
}

func (c *Cache) handleGetTableBytes(_ *transport.ConnectionContext, request []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	lb := binary.BigEndian.Uint32(request[2:])
	key := request[6 : 6+lb]

	bytes, err := c.getFromCache(key)
	if err != nil {
		return responseWriter(nil, err)
	}
	return sendBytesResponse(responseWriter, responseBuff, bytes)
}

func (c *Cache) getFromCache(key []byte) ([]byte, error) {
	atomic.AddInt64(&c.stats.Gets, 1)
	v, ok := c.cache.Get(key)
	if ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		return v.([]byte), nil
	}
	bytes, err := objstore.GetWithTimeout(c.objStore, c.cfg.DataBucketName, string(key), c.cfg.ObjStoreCallTimeout)
	if err != nil {
		return nil, err
	}
	if len(bytes) > 0 {
		atomic.AddInt64(&c.stats.Misses, 1)
		c.cache.Set(key, bytes, int64(len(bytes)))
	} else {
		atomic.AddInt64(&c.stats.NotFound, 1)
	}
	return bytes, nil
}

func (c *Cache) GetStats() CacheStats {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stats
}

func sendBytesResponse(responseWriter transport.ResponseWriter, responseBuff []byte, tableBytes []byte) error {
	responseBuff = binary.BigEndian.AppendUint32(responseBuff, uint32(len(tableBytes)))
	responseBuff = append(responseBuff, tableBytes...)
	return responseWriter(responseBuff, nil)
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}
