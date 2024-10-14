package fetcher

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalCache(t *testing.T) {
	localCache, err := NewLocalSSTCache(100, 32*1024*1024)
	require.NoError(t, err)

	entriesMap := map[string]*sst.SSTable{}

	numEntries := 100
	for i := 0; i < numEntries; i++ {
		entriesMap[uuid.New().String()] = &sst.SSTable{}
	}

	for id, table := range entriesMap {
		bid := []byte(id)
		localCache.Put(bid, table)
		localCache.cache.Wait()
		tab, ok := localCache.Get(bid)
		require.True(t, ok)
		require.Equal(t, table, tab)
	}
}

func BenchmarkLocalCache(b *testing.B) {

	localCache, err := NewLocalSSTCache(100, 64*1024*1024)
	require.NoError(b, err)

	var keys [][]byte
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(uuid.New().String()))
	}

	for i := 0; i < b.N; i++ {
		key := keys[i%100]
		localCache.Put(key, &sst.SSTable{})
	}
}
