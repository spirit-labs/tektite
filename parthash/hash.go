package parthash

import (
	"crypto/sha256"
	"encoding/binary"
	lru "github.com/hashicorp/golang-lru"
	"github.com/spirit-labs/tektite/common"
)

type PartitionHashes struct {
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
	kb := make([]byte, 16)
	binary.BigEndian.PutUint64(kb, uint64(topicID))
	binary.BigEndian.PutUint64(kb[8:], uint64(partitionID))
	var key string
	if p.cache != nil {
		key = common.ByteSliceToStringZeroCopy(kb)
		h, ok := p.cache.Get(key)
		if ok {
			return h.([]byte), nil
		}
	}
	hashFunc := sha256.New()
	if _, err := hashFunc.Write(kb); err != nil {
		return nil, err
	}
	out := hashFunc.Sum(nil)
	// we take the first 128 bits
	r := out[:16]
	if p.cache != nil {
		p.cache.Add(key, r)
	}
	return r, nil
}

func CreatePartitionHash(topicID int, partitionID int) ([]byte, error) {
	kb := make([]byte, 16)
	binary.BigEndian.PutUint64(kb, uint64(topicID))
	binary.BigEndian.PutUint64(kb[8:], uint64(partitionID))
	hashFunc := sha256.New()
	if _, err := hashFunc.Write(kb); err != nil {
		return nil, err
	}
	out := hashFunc.Sum(nil)
	return out[:16], nil
}

