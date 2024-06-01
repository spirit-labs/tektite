package tabcache

import (
	"github.com/dgraph-io/ristretto"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"
	"sync"
)

type Cache struct {
	cache      *ristretto.Cache
	cloudStore objstore.Client
	// We only have this to prevent golang race detector flagging issue in ristretto cache
	// as the ristretto cache `isClosed` flag is mutated without locking
	lock sync.RWMutex
}

func NewTableCache(cloudStore objstore.Client, cfg *conf.Config) (*Cache, error) {
	maxItemsEstimate := int(*cfg.TableCacheMaxSizeBytes) / int(*cfg.MemtableMaxSizeBytes)
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * maxItemsEstimate),
		MaxCost:     int64(*cfg.TableCacheMaxSizeBytes),
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &Cache{
		cache:      cache,
		cloudStore: cloudStore,
	}, nil
}

func (tc *Cache) Start() error {
	return nil
}

func (tc *Cache) Stop() error {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.cache.Close()
	return nil
}

func (tc *Cache) GetSSTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	skey := common.ByteSliceToStringZeroCopy(tableID)
	t, ok := tc.cache.Get(skey)
	if ok {
		return t.(*sst.SSTable), nil //nolint:forcetypeassert
	}
	b, err := tc.cloudStore.Get(tableID)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	ssTable := &sst.SSTable{}
	ssTable.Deserialize(b, 0)
	tc.cache.Set(skey, ssTable, int64(len(b)))
	return ssTable, nil
}

func (tc *Cache) AddSSTable(tableID sst.SSTableID, table *sst.SSTable) error {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	tc.cache.Set(string(tableID), table, int64(table.SizeBytes()))
	tc.cache.Wait()
	return nil
}

func (tc *Cache) DeleteSSTable(tableID sst.SSTableID) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	tc.cache.Del(string(tableID))
}
