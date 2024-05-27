package levels

import (
	lru "github.com/hashicorp/golang-lru"
	"sync"
)

type segmentCache struct {
	// Entries are also stored in a pre-flush cache before flush occurs - as there is a possibility could be evicted
	// from the lru before flush.
	preFlushCache map[string]*segment
	lruCache      *lru.Cache
	lock          sync.Mutex
}

const preFlushCacheInitialSize = 100

func newSegmentCache(maxSize int) *segmentCache {
	var lruCache *lru.Cache
	if maxSize > 0 {
		var err error
		lruCache, err = lru.New(maxSize)
		if err != nil {
			panic(err)
		}
	}
	return &segmentCache{
		preFlushCache: make(map[string]*segment, preFlushCacheInitialSize),
		lruCache:      lruCache,
	}
}

func (s *segmentCache) get(segmentID string) *segment {
	s.lock.Lock()
	defer s.lock.Unlock()
	seg, ok := s.preFlushCache[segmentID]
	if ok {
		return seg
	}
	if s.lruCache == nil {
		return nil
	}
	o, ok := s.lruCache.Get(segmentID)
	if !ok {
		return nil
	}
	return o.(*segment)
}

func (s *segmentCache) put(segmentID string, segment *segment) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.preFlushCache[segmentID] = segment
	if s.lruCache != nil {
		s.lruCache.Add(segmentID, segment)
	}
}

func (s *segmentCache) delete(segmentID string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Until they are flushed, segments cannot go in the LRU as there is a possibility they could get evicted before
	// flush occurred and then data-loss could occur.
	delete(s.preFlushCache, segmentID)
	if s.lruCache != nil {
		s.lruCache.Remove(segmentID)
	}
}

func (s *segmentCache) flush() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.preFlushCache = make(map[string]*segment, preFlushCacheInitialSize)
}
