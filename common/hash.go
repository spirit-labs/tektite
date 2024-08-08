package common

import (
	"crypto/sha256"
	"github.com/spirit-labs/tektite/mit"
	"hash/fnv"
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
