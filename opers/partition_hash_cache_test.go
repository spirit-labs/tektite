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

package opers

import (
	"bytes"
	"fmt"
	"github.com/spirit-labs/tektite/proc"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPartitionHashCacheSlice(t *testing.T) {
	testPartitionHashCache(t, maxHashesInSlice)
}

func TestPartitionHashCacheMap(t *testing.T) {
	testPartitionHashCache(t, maxHashesInSlice+1)
}

func TestPartitionHashCacheMapConcurrentSlice(t *testing.T) {
	testPartitionHashCacheConcurrent(t, maxHashesInSlice/4, 4)
}

func TestPartitionHashCacheMapConcurrentMap(t *testing.T) {
	testPartitionHashCacheConcurrent(t, maxHashesInSlice, 4)
}

func testPartitionHashCache(t *testing.T, partitions int) {
	mappingID := "aardvarks"
	cache := newPartitionHashCache(mappingID, partitions)
	for i := 0; i < partitions; i++ {
		hash := cache.getHash(i)
		expected := proc.CalcPartitionHash(mappingID, uint64(i))
		require.Equal(t, expected, hash)
	}
}

func testPartitionHashCacheConcurrent(t *testing.T, partitionsPerGR int, numGRs int) {
	mappingID := "aardvarks"
	totPartitions := partitionsPerGR * numGRs
	cache := newPartitionHashCache(mappingID, totPartitions)
	var wg sync.WaitGroup
	wg.Add(numGRs)
	for i := 0; i < numGRs; i++ {
		start := i * partitionsPerGR
		end := start + partitionsPerGR
		go func() {
			for partID := start; partID < end; partID++ {
				hash := cache.getHash(partID)
				expected := proc.CalcPartitionHash(mappingID, uint64(partID))
				if !bytes.Equal(expected, hash) {
					panic("bytes not equal")
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkPartitionHashCacheSlice(b *testing.B) {
	cache := newPartitionHashCache("aardvarks", maxHashesInSlice/2)
	b.ResetTimer()
	l := 0
	for i := 0; i < b.N; i++ {
		hash := cache.getHash(i % 100)
		l += int(hash[0])
	}
	b.StopTimer()
	fmt.Print(l)
}

func BenchmarkPartitionHashCacheMap(b *testing.B) {
	cache := newPartitionHashCache("aardvarks", maxHashesInSlice+1)
	b.ResetTimer()
	l := 0
	for i := 0; i < b.N; i++ {
		hash := cache.getHash(i % 100)
		l += int(hash[0])
	}
	b.StopTimer()
	fmt.Print(l)
}

func BenchmarkCalcPartitionHash(b *testing.B) {
	l := 0
	for i := 0; i < b.N; i++ {
		hash := proc.CalcPartitionHash("aardvarks", uint64(i%100))
		l += int(hash[0])
	}
	b.StopTimer()
	fmt.Print(l)
}
