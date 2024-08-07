package common

import (
	"fmt"
	"github.com/spirit-labs/tektite/mit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHashFnv(t *testing.T) {
	testHash(t, HashFnv)
}

func TestHashSha256(t *testing.T) {
	testHash(t, HashSha256)
}

func TestHashMurmur2(t *testing.T) {
	testHash(t, mit.KafkaCompatibleMurmur2Hash)
}

func testHash(t *testing.T, hashFunc HashFunc) {
	numHashes := 100
	counts := make(map[uint32]int)
	for i := 0; i < numHashes; i++ {
		s := fmt.Sprintf("%d", i)
		h := hashFunc([]byte(s))
		counts[h]++
	}
	// Should be no collisions with a small number of hashes
	require.Equal(t, len(counts), numHashes)
}

var hashTestBytes = []byte("uiwiuhqwduqhduwdqwd")

func BenchmarkMurmur(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mit.KafkaCompatibleMurmur2Hash(hashTestBytes)
	}
}

func BenchmarkFnv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		HashFnv(hashTestBytes)
	}
}

func BenchmarkSha256(b *testing.B) {
	for i := 0; i < b.N; i++ {
		HashSha256(hashTestBytes)
	}
}
