package opers

import (
	"github.com/spirit-labs/tektite/proc"
	"sync"
)

const maxHashesInSlice = 250

type partitionHashCache struct {
	mappingID string
	hashes    [][]byte
	hashesMap map[int][]byte
	lock      sync.RWMutex
}

func newPartitionHashCache(mappingID string, partitions int) *partitionHashCache {
	var hashesSlice [][]byte
	var hashesMap map[int][]byte
	if partitions > maxHashesInSlice {
		hashesMap = map[int][]byte{}
	} else {
		hashesSlice = make([][]byte, partitions)
		for i := 0; i < partitions; i++ {
			hashesSlice[i] = proc.CalcPartitionHash(mappingID, uint64(i))
		}
	}
	return &partitionHashCache{
		mappingID: mappingID,
		hashes:    hashesSlice,
		hashesMap: hashesMap,
	}
}

func (p *partitionHashCache) getHash(partitionID int) []byte {
	if p.hashesMap == nil {
		// Precalculated so no need for locking
		return p.hashes[partitionID]
	}
	// If caching in a map, we need to use a lock
	p.lock.RLock()
	hash, ok := p.hashesMap[partitionID]
	if ok {
		p.lock.RUnlock()
		return hash
	}
	p.lock.RUnlock()
	// Upgrade to write lock and check again
	p.lock.Lock()
	defer p.lock.Unlock()
	hash, ok = p.hashesMap[partitionID]
	if ok {
		return hash
	}
	hash = proc.CalcPartitionHash(p.mappingID, uint64(partitionID))
	p.hashesMap[partitionID] = hash
	return hash
}
