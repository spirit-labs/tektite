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
