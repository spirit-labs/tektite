package levels

import (
	lru "github.com/hashicorp/golang-lru"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
)

type segmentCache struct {
	// Entries are also stored in a pre-flush cache before flush occurs - as there is a possibility could be evicted
	// from the lru before flush.
	livePreflushCache   map[string]*segment
	sealedPreflushCache map[string]*segment
	lruCache            *lru.Cache
	lock                sync.RWMutex
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
		livePreflushCache: createPreflushCache(),
		lruCache:          lruCache,
	}
}

func createPreflushCache() map[string]*segment {
	return make(map[string]*segment, preFlushCacheInitialSize)
}

func (s *segmentCache) get(segmentID string) *segment {
	s.lock.RLock()
	defer s.lock.RUnlock()
	seg, ok := s.lookInPreflushCaches(segmentID)
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

func (s *segmentCache) lookInPreflushCaches(segmentID string) (*segment, bool) {
	if s.sealedPreflushCache != nil {
		seg, ok := s.sealedPreflushCache[segmentID]
		if ok {
			return seg, true
		}
	}
	seg, ok := s.livePreflushCache[segmentID]
	if ok {
		return seg, true
	}
	return nil, false
}

func (s *segmentCache) put(segmentID string, segment *segment) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.livePreflushCache[segmentID] = segment
	if s.lruCache != nil {
		s.lruCache.Add(segmentID, segment)
		log.Debugf("%p segment %s added to lru cache", s, segmentID)
	}
}

func (s *segmentCache) delete(segmentID string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.livePreflushCache, segmentID)
	if s.lruCache != nil {
		s.lruCache.Remove(segmentID)
		log.Debugf("%p segment %s deleted from lru cache", s, segmentID)
	}
}

func (s *segmentCache) sealPreflushCache() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sealedPreflushCache = s.livePreflushCache
	s.livePreflushCache = createPreflushCache()
}

func (s *segmentCache) flushSealedCache() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sealedPreflushCache = nil
}
