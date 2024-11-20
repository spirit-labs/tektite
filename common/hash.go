package common

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/spirit-labs/tektite/mit"
	"hash/fnv"
	"math"
)

func CalcPartition(hash uint32, numPartitions int) uint32 {
	return hash % uint32(numPartitions)
}

type HashFunc func([]byte) uint32

func HashSha256(key []byte) uint32 {
	h := sha256.New()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	res := h.Sum(nil)
	h2 := fnv.New32()
	if _, err := h2.Write(res); err != nil {
		panic(err)
	}
	return h2.Sum32()
}

func HashFnv(key []byte) uint32 {
	h := fnv.New32()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	return h.Sum32()
}

func DefaultHash(key []byte) uint32 {
	return mit.KafkaCompatibleMurmur2Hash(key)
}

func CalcMemberForHash(hash []byte, n int) int {
	top32bits := binary.BigEndian.Uint32(hash)
	res := int(uint64(top32bits) * uint64(n) / uint64(math.MaxUint32))
	// edge case where top32bits = math.MaxUint32:
	if res == n {
		res = n - 1
	}
	return res
}
