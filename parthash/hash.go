package parthash

import (
	"crypto/sha256"
	"encoding/binary"
	lru "github.com/hashicorp/golang-lru"
	"github.com/spirit-labs/tektite/common"
	"sync"
)

type PartitionHashes struct {
	lock sync.RWMutex
	cache *lru.Cache
}

func NewPartitionHashes(size int) (*PartitionHashes, error) {
	var cache *lru.Cache
	if size > 0 {
		var err error
		cache, err = lru.New(size)
		if err != nil {
			return nil, err
		}
	}
	return &PartitionHashes{cache: cache}, nil
}

func (p *PartitionHashes) GetPartitionHash(topicID int, partitionID int) ([]byte, error) {
	// We cache partition hashes in an LRU as crypto hashes like sha-256 are usually quite slow
	kb := createKeyBytes(topicID, partitionID)
	if p.cache == nil {
		return createHash(kb)
	}
	key := common.ByteSliceToStringZeroCopy(kb)
	hash := p.getFromCache(key)
	if hash != nil {
		return hash, nil
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	hash, err := createHash(kb)
	if err != nil {
		return nil, err
	}
	p.cache.Add(key, hash)
	return hash, nil
}

func createHash(kb []byte) ([]byte, error) {
	hashFunc := sha256.New()
	if _, err := hashFunc.Write(kb); err != nil {
		return nil, err
	}
	out := hashFunc.Sum(nil)
	// we take the first 128 bits
	return out[:16], nil
}

func (p *PartitionHashes) getFromCache(key string) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	h, ok := p.cache.Get(key)
	if ok {
		return h.([]byte)
	}
	return nil
}

func createKeyBytes(topicID int, partitionID int) []byte {
	kb := make([]byte, 16)
	binary.BigEndian.PutUint64(kb, uint64(topicID))
	binary.BigEndian.PutUint64(kb[8:], uint64(partitionID))
	return kb
}

func CreatePartitionHash(topicID int, partitionID int) ([]byte, error) {
	return createHash(createKeyBytes(topicID, partitionID))
}

