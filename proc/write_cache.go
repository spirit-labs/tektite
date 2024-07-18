package proc

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"unsafe"
)

// WriteCache is mainly used to cache entries for aggregations
type WriteCache struct {
	lruCache    *lru.Cache
	processorID int
}

type cacheEntry struct {
	value []byte
	size  int
}

func (ce *cacheEntry) Size() int {
	return ce.size
}

var byteSliceOverhead = int(unsafe.Sizeof([]byte{1}))
var cacheEntryOverhead = int(unsafe.Sizeof(cacheEntry{}))

func NewWriteCache(maxSizeBytes int, processorID int) *WriteCache {
	lruCache, err := lru.New(maxSizeBytes)
	if err != nil {
		panic(err)
	}
	return &WriteCache{
		lruCache:    lruCache,
		processorID: processorID,
	}
}

func (w *WriteCache) Put(kv common.KV) {
	keyNoVersion := kv.Key[:len(kv.Key)-8]
	w.lruCache.RemoveOldest()
	size := cacheEntryOverhead + len(kv.Value) + // value
		byteSliceOverhead + len(kv.Key) // key
	w.lruCache.Add(common.ByteSliceToStringZeroCopy(keyNoVersion), cacheEntry{
		value: kv.Value,
		size:  size,
	})
}

func (w *WriteCache) Get(keyNoVersion []byte) ([]byte, bool) {
	v, ok := w.lruCache.Get(common.ByteSliceToStringZeroCopy(keyNoVersion))
	if !ok {
		return nil, false
	}
	return v.(cacheEntry).value, true
}

func (w *WriteCache) Clear() {
	log.Debugf("clearing write cache to store for processor %d", w.processorID)
	w.lruCache.Purge()
}
