package fetchcache

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	mrand "math/rand"
	"testing"
)

func TestCacheSingleNode(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	localTransports := transport.NewLocalTransports()
	transportServer, err := localTransports.NewLocalServer("server-address-1")
	require.NoError(t, err)

	cfg := NewConf()
	cfg.DataBucketName = "test-bucket"
	cfg.AzInfo = "test-az"
	cfg.MaxSizeBytes = 16 * 1024 * 1024
	connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
	cache, err := NewCache(objStore, connCaches, transportServer, cfg)
	require.NoError(t, err)

	cache.Start()
	defer cache.Stop()

	membershipData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
		Location:             cfg.AzInfo,
	}
	err = cache.MembershipChanged(0, cluster.MembershipState{
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: membershipData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	stats := cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 0, int(stats.Misses))

	key1 := []byte("some-key-1")
	bytes, err := cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, 0, len(bytes))

	stats = cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 0, int(stats.Misses))
	require.Equal(t, 1, int(stats.NotFound))

	tableBytes := []byte("quwdhiquwhdiquwhdiuqd")
	err = objStore.Put(context.Background(), cfg.DataBucketName, string(key1), tableBytes)
	require.NoError(t, err)

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))

	// ristretto has async put
	cache.cache.Wait()

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 1, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 2, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))
}

func TestCacheMultipleNodes(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	localTransports := transport.NewLocalTransports()

	cfg := NewConf()
	cfg.DataBucketName = "test-bucket"
	cfg.AzInfo = "test-az"
	cfg.MaxSizeBytes = 16 * 1024 * 1024

	numNodes := 5

	var caches []*Cache
	var members []cluster.MembershipEntry

	for i := 0; i < numNodes; i++ {
		transportServer, err := localTransports.NewLocalServer(uuid.New().String())
		require.NoError(t, err)
		connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
		cache, err := NewCache(objStore, connCaches, transportServer, cfg)
		require.NoError(t, err)
		cache.Start()
		membershipData := common.MembershipData{
			ClusterListenAddress: transportServer.Address(),
			Location:             cfg.AzInfo,
		}
		members = append(members, cluster.MembershipEntry{
			ID:   int32(i),
			Data: membershipData.Serialize(nil),
		})
		caches = append(caches, cache)
	}
	defer func() {
		for _, cache := range caches {
			cache.Stop()
		}
	}()

	membership := cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members:        members,
	}
	for i, cache := range caches {
		err := cache.MembershipChanged(int32(i), membership)
		require.NoError(t, err)
	}

	numKeys := 1000
	kvs := setupData(t, numKeys, objStore, cfg.DataBucketName)

	numGetsPerKey := 10
	for i := 0; i < numGetsPerKey; i++ {
		for _, kv := range kvs {
			clientCache := caches[mrand.Intn(len(caches))]

			v, err := clientCache.GetTableBytes(kv.Key)
			require.NoError(t, err)

			require.Equal(t, kv.Value, v)
		}
	}

	totHits := 0
	totMisses := 0
	totGets := 0
	expectedMissRatio := 1 / float64(numGetsPerKey)
	expectedHitRatio := 1 - expectedMissRatio
	avgGetsPerNode := float64(numKeys*numGetsPerKey) / float64(numNodes)

	for _, cache := range caches {
		stats := cache.GetStats()
		require.GreaterOrEqual(t, float64(stats.Hits), 0.5*avgGetsPerNode) // Allow for imbalance
		totGets += int(stats.Gets)
		totHits += int(stats.Hits)
		totMisses += int(stats.Misses)
		missRatio := float64(stats.Misses) / float64(stats.Gets)
		hitRatio := float64(stats.Hits) / float64(stats.Gets)
		require.Equal(t, expectedMissRatio, missRatio)
		require.Equal(t, expectedHitRatio, hitRatio)
	}

	require.Equal(t, numKeys*numGetsPerKey, totGets)
	require.Equal(t, numKeys*(numGetsPerKey-1), totHits)
	require.Equal(t, numKeys, totMisses)

	cacheMemberIDMap := map[*Cache]int32{}
	for i, cache := range caches {
		cacheMemberIDMap[cache] = int32(i)
	}

	nodesToRemove := 3
	for i := 0; i < nodesToRemove; i++ {
		memberToRemove := mrand.Intn(len(members))

		newCaches := append([]*Cache{}, caches[:memberToRemove]...)
		newMembers := append([]cluster.MembershipEntry{}, members[:memberToRemove]...)
		newCaches = append(newCaches, caches[memberToRemove+1:]...)
		newMembers = append(newMembers, members[memberToRemove+1:]...)

		for _, c := range newCaches {
			memberID, ok := cacheMemberIDMap[c]
			require.True(t, ok)
			err := c.MembershipChanged(memberID, cluster.MembershipState{
				LeaderVersion:  1,
				ClusterVersion: 2,
				Members:        members,
			})
			require.NoError(t, err)
		}

		// get keys again
		for i := 0; i < numGetsPerKey; i++ {
			for _, kv := range kvs {
				clientCache := caches[mrand.Intn(len(caches))]
				v, err := clientCache.GetTableBytes(kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, v)
			}
		}
	}
}

func TestMultipleAZs(t *testing.T) {
	// Setup, say 9 nodes, 3 in each AZ
	numAzs := 3
	numNodesPerAz := 5

	objStore := dev.NewInMemStore(0)
	localTransports := transport.NewLocalTransports()

	cfg := NewConf()
	cfg.DataBucketName = "test-bucket"
	cfg.AzInfo = "test-az"
	cfg.MaxSizeBytes = 16 * 1024 * 1024

	var allCaches []*Cache
	var members []cluster.MembershipEntry
	var allAzCaches [][]*Cache

	var memberID int32
	for i := 0; i < numAzs; i++ {
		az := fmt.Sprintf("AZ-%d", i)
		var azCaches []*Cache
		for j := 0; j < numNodesPerAz; j++ {
			transportServer, err := localTransports.NewLocalServer(uuid.New().String())
			require.NoError(t, err)
			cfgCopy := cfg
			cfgCopy.AzInfo = az
			connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
			cache, err := NewCache(objStore, connCaches, transportServer, cfgCopy)
			require.NoError(t, err)
			cache.Start()
			membershipData := common.MembershipData{
				ClusterListenAddress: transportServer.Address(),
				Location:             az,
			}
			members = append(members, cluster.MembershipEntry{
				ID:   memberID,
				Data: membershipData.Serialize(nil),
			})
			memberID++
			allCaches = append(allCaches, cache)
			azCaches = append(azCaches, cache)
		}
		allAzCaches = append(allAzCaches, azCaches)
	}

	membership := cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members:        members,
	}
	for i, cache := range allCaches {
		err := cache.MembershipChanged(int32(i), membership)
		require.NoError(t, err)
	}

	numKeys := 1000
	kvs := setupData(t, numKeys, objStore, cfg.DataBucketName)

	numGetsPerKey := 10

	// Do all the gets against each az in turn - make sure the hits don't reach other AZs
	for i, azCaches := range allAzCaches {
		for j := 0; j < numGetsPerKey; j++ {
			for _, kv := range kvs {
				clientCache := azCaches[mrand.Intn(len(azCaches))]
				v, err := clientCache.GetTableBytes(kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, v)
			}
		}
		otherAzGetsBefore := getOtherAzGets(allAzCaches, i)

		// All the gets should be in same AZ
		var azGets int64
		for _, cache := range azCaches {
			azGets += cache.GetStats().Gets
		}
		require.Equal(t, numKeys*numGetsPerKey, int(azGets))

		otherAzGetsAfter := getOtherAzGets(allAzCaches, i)

		// verify no gets on other AZs
		require.Equal(t, otherAzGetsBefore, otherAzGetsAfter)
	}
}

func getOtherAzGets(allAzCaches [][]*Cache, exceptIndex int) int64 {
	var otherAZGets int64
	for j, azCaches := range allAzCaches {
		if j != exceptIndex {
			for _, azCache := range azCaches {
				otherAZGets += azCache.GetStats().Gets
			}
		}
	}
	return otherAZGets
}

func setupData(t *testing.T, numKeys int, objectStore objstore.Client, dataBucketName string) []common.KV {
	var kvs []common.KV
	for i := 0; i < numKeys; i++ {
		key := []byte(uuid.New().String())
		value := randomBytes(1000)
		kvs = append(kvs, common.KV{
			Key:   key,
			Value: value,
		})
		err := objectStore.Put(context.Background(), dataBucketName, string(key), value)
		require.NoError(t, err)
	}
	return kvs
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}
