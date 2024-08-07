package sanity

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
)

func NewSanityStore() *SanityStore {
	return &SanityStore{data: map[string][]valueHolder{}}
}

type SanityStore struct {
	lock         sync.Mutex
	data         map[string][]valueHolder
	deadVersions []levels.VersionRange
}

type valueHolder struct {
	version uint64
	value   []byte
}

var procStores = make(map[int]*SanityStore)
var mapLock sync.Mutex

func GetSanityStore(processorID int) *SanityStore {
	mapLock.Lock()
	defer mapLock.Unlock()
	ss, ok := procStores[processorID]
	if !ok {
		ss = NewSanityStore()
		procStores[processorID] = ss
	}
	return ss
}

func ClearStores() {
	mapLock.Lock()
	defer mapLock.Unlock()
	for _, ss := range procStores {
		ss.Clear()
	}
}

func RegisterDeadVersionRange(versionRange levels.VersionRange) {
	mapLock.Lock()
	defer mapLock.Unlock()
	for _, ss := range procStores {
		ss.RegisterDeadVersionRange(versionRange)
	}
}

func (s *SanityStore) Put(key []byte, value []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	version := encoding.DecodeKeyVersion(key)
	keyNoVersion := key[:len(key)-8]
	sKey := string(keyNoVersion)
	values, ok := s.data[sKey]
	//log.Infof("sanitystore putting key %v value %v", key, value)
	if !ok {
		s.data[sKey] = []valueHolder{{value: value, version: version}}
	} else {
		// check version is higher
		prevHolder := values[len(values)-1]
		if version < prevHolder.version {
			panic(fmt.Sprintf("version for key %v (%d) is lower than previous version (%d)", key, version, prevHolder.version))
		}
		s.data[sKey] = append(values, valueHolder{value: value, version: version})
	}
}

func (s *SanityStore) LogValuesForKey(keyNoVersion []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	values, ok := s.data[string(keyNoVersion)]
	if !ok {
		log.Infof("sanity store has no values for key %v", keyNoVersion)
	} else {
		log.Infof("sanity store has %d values for key %v:\n", len(values), keyNoVersion)
		for _, value := range values {
			log.Infof("%v\n", value)
		}
	}
}

func (s *SanityStore) Get(keyNoVersion []byte, maxVersion uint64) []byte {
	s.lock.Lock()
	defer s.lock.Unlock()
	values, ok := s.data[string(keyNoVersion)]
	if !ok {
		return nil
	}
	for i := len(values) - 1; i >= 0; i-- {
		holder := values[i]
		if holder.version > maxVersion {
			continue
		}
		if s.isDead(holder.version) {
			continue
		}
		if len(holder.value) == 0 {
			// tombstone
			return nil
		}
		return holder.value
	}
	return nil
}

func (s *SanityStore) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data = map[string][]valueHolder{}
	s.deadVersions = nil
}

func (s *SanityStore) RegisterDeadVersionRange(versionRange levels.VersionRange) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Infof("registering dead version range %v", versionRange)
	s.deadVersions = append(s.deadVersions, versionRange)
}

func (s *SanityStore) UnregisterDeadVersionRange(versionRange levels.VersionRange) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var newVersions []levels.VersionRange
	for _, rng := range s.deadVersions {
		if rng.VersionStart != versionRange.VersionStart || rng.VersionEnd != versionRange.VersionEnd {
			newVersions = append(newVersions, rng)
		}
	}
	s.deadVersions = newVersions
}

func (s *SanityStore) isDead(version uint64) bool {
	for _, versionRange := range s.deadVersions {
		if version >= versionRange.VersionStart && version <= versionRange.VersionEnd {
			return true
		}
	}
	return false
}
