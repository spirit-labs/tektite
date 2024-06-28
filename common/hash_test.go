// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
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
	testHash(t, KafkaCompatibleMurmur2Hash)
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
		KafkaCompatibleMurmur2Hash(hashTestBytes)
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
