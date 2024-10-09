package fetcher

import (
	"github.com/dgraph-io/ristretto"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/sst"
)

/*
LocalSSTCache caches the most recently retrieved SSTables locally. When there are multiple fetch requests for different
partitions being processed around the same time on an agent, and those fetch requests are for recently produced data,
it's likely that the data for multiple partitions lives in the same recently pushed SSTables. Without a local SSTable
cache, each fetcher would independently pull SSTables from the distributed fetch cache which would often result in
remote calls. To make this more efficient we cache a reasonably small number of the most recently retrieved SSTables
locally on the agent, so that recent fetchers can retrieve them from there rather than making duplicate remote calls.
*/
type LocalSSTCache struct {
	cache *ristretto.Cache
}

func NewLocalSSTCache(maxTablesEstimate int, maxSizeBytes int) (*LocalSSTCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * maxTablesEstimate),
		MaxCost:     int64(maxSizeBytes),
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &LocalSSTCache{
		cache: cache,
	}, nil
}

func (m *LocalSSTCache) Put(key sst.SSTableID, value *sst.SSTable) bool {
	sKey := common.ByteSliceToStringZeroCopy(key)
	return m.cache.Set(sKey, value, int64(value.SizeBytes()))
}

func (m *LocalSSTCache) Get(key sst.SSTableID) (*sst.SSTable, bool) {
	sKey := common.ByteSliceToStringZeroCopy(key)
	v, ok := m.cache.Get(sKey)
	if !ok {
		return nil, false
	}
	return v.(*sst.SSTable), true
}
