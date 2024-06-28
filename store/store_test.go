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

package store

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableMaxSizeBytes = 5 * 1024 * 1024
	cfg.MemtableFlushQueueMaxSize = 10
	cfg.MemtableMaxReplaceInterval = 10 * time.Minute

	store := SetupStoreWithConfig(t, cfg)
	defer stopStore(t, store)

	// We pretend all versions are complete so memtables get pushed to cloud straight away
	store.updateLastCompletedVersion(math.MaxInt64)

	cs := store.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert
	sizeStart := cs.Size()
	batchSize := 100
	numSSTables := 4
	ks := 0
	// Write data until numSSTables sstables have been flushed to the object store
	for cs.Size() < sizeStart+numSSTables {
		writeKVs(t, store, ks, batchSize)
		ks += batchSize
	}

	// Now we get each key - some will come from active memtable, others flush queue, others sstable
	for i := 0; i < ks; i++ {
		k := []byte(fmt.Sprintf("prefix/key-%010d", i))
		v, err := store.Get(k)
		require.NoError(t, err)
		expectedV := []byte(fmt.Sprintf("prefix/value-%010d", i))
		require.Equal(t, expectedV, v)
	}

	// Now get some non-existent keys
	v, err := store.Get([]byte("foobar"))
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = store.Get([]byte(fmt.Sprintf("prefix/key-%010d", ks)))
	require.NoError(t, err)
	require.Nil(t, v)
	v, err = store.Get([]byte("prefix/key-"))
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestNewerEntriesOverrideOlderOnesAfterPush(t *testing.T) {
	// Test that new entries override older ones after they've been pushed in different sstable to the cloud store (L0)
	// This effectively checks that sstables are retrieved and added in the correct order
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableMaxSizeBytes = 16 * 1024 * 1024
	cfg.MemtableFlushQueueMaxSize = 10
	cfg.MemtableMaxReplaceInterval = 10 * time.Minute

	store := SetupStoreWithConfig(t, cfg)
	defer stopStore(t, store)
	store.updateLastCompletedVersion(math.MaxInt64) // Let table be flushed immediately
	cs := store.cloudStoreClient.(*dev.InMemStore)  //nolint:forcetypeassert
	sizeStart := cs.Size()
	batch := mem.NewBatch()
	batch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key1"), 0),
		Value: []byte("val1"),
	})
	batch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key2"), 0),
		Value: []byte("val2"),
	})
	writeBatchInSSTable(t, store, batch)
	require.Equal(t, sizeStart+1, cs.Size())
	batch = mem.NewBatch()
	batch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key1"), 0),
		Value: []byte("val1_1"),
	})
	batch.AddEntry(common.KV{
		Key: encoding.EncodeVersion([]byte("key2"), 0),
	})
	writeBatchInSSTable(t, store, batch)
	require.Equal(t, sizeStart+2, cs.Size())

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, true)
	curr := iter.Current()
	curr.Key = curr.Key[:len(curr.Key)-8]
	require.Equal(t, "key1", string(curr.Key))
	require.Equal(t, "val1_1", string(curr.Value))
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func writeBatchInSSTable(t *testing.T, store *Store, batch *mem.Batch) {
	var errVal atomic.Value
	err := store.Write(batch)
	require.NoError(t, err)
	err = store.Flush(true, false)
	require.NoError(t, err)
	ev := errVal.Load()
	if ev != nil {
		//goland:noinspection GoTypeAssertionOnErrors
		err = ev.(error)
		require.NoError(t, err)
	}
}

func TestSimpleIterate(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateAllThenAddMore(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 10, 10)
	iteratePairs(t, iter, 10, 10)
}

func TestIteratorPicksUpAddedDataAfterDataWithGreaterKeyAlreadyExists(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	// Test case where data already exists with greater key before iter is created
	// Then iter is created, and it's not valid
	// Then add data in range
	// And it picks up new data

	writeKVs(t, store, 5, 1)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 4))
	ke := []byte(fmt.Sprintf("prefix/key-%010d", 5))

	iter, err := store.NewIterator(ks, ke, math.MaxUint64, false)
	require.NoError(t, err)

	requireIterValid(t, iter, false)

	writeKVs(t, store, 4, 1)

	requireIterValid(t, iter, true)

	iteratePairs(t, iter, 4, 1)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateInRange(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))
	ke := []byte(fmt.Sprintf("prefix/key-%010d", 7))

	iter, err := store.NewIterator(ks, ke, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 3, 4)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateInRangeNoEndRange(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))

	iter, err := store.NewIterator(ks, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 3, 7)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateThenDelete(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 0, 5)

	requireIterValid(t, iter, true)

	// Then we delete the rest of them from the memtable
	writeKVsWithTombstones(t, store, 5, 5)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateThenReplaceThenDelete(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 0, 5)
	requireIterValid(t, iter, true)

	err = store.forceReplaceMemtable()
	require.NoError(t, err)
	requireIterValid(t, iter, true)

	// Then we delete the rest of them from the memtable
	writeKVsWithTombstones(t, store, 5, 5)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateAllMemtableThenReplace(t *testing.T) {
	store := SetupStore(t)
	//goland:noinspection GoUnhandledErrorResult
	defer store.Stop()

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	// fully iterate through the memtable, then force a replace then fully iterate again. and repeat

	ks := 0
	for i := 0; i < 10; i++ {
		writeKVs(t, store, ks, 10)
		iteratePairs(t, iter, ks, 10)
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)

		err = store.forceReplaceMemtable()
		require.NoError(t, err)
		ks += 10
	}
}

func TestIterateSomeMemtableThenReplace(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	// partially iterate through the memtable, then force a replace then fully iterate again. and repeat

	ks := 0
	for i := 0; i < 10; i++ {
		writeKVs(t, store, ks, 10)

		// Only iterate through some of them
		iteratePairs(t, iter, ks, 5)
		requireIterValid(t, iter, true)

		err = store.forceReplaceMemtable()
		require.NoError(t, err)

		ks += 5

		// Then iterate through the rest
		err = iter.Next()
		require.NoError(t, err)
		iteratePairs(t, iter, ks, 5)
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)

		ks += 5
	}
}

func TestIterateThenReplaceThenAddWithSmallerKey(t *testing.T) {
	store := SetupStore(t)
	defer stopStore(t, store)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, store, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	err = store.forceReplaceMemtable()
	require.NoError(t, err)

	// Now add some more but with smaller keys we've already seen, and different values
	writeKVsWithValueSuffix(t, store, 0, 10, "suf")

	// We shouldn't see these
	requireIterValid(t, iter, false)

	// Add some more - we should see these
	writeKVs(t, store, 10, 10)
	iteratePairs(t, iter, 10, 10)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateShuffledKeys(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableFlushQueueMaxSize = 1000
	store := SetupStoreWithConfig(t, cfg)
	defer stopStore(t, store)

	// Add a bunch of entries in random order, and add in small batches, replacing memtable each time
	// Then iterate through them and verify in order and all correct

	numEntries := 10000
	entries := prepareRanges(numEntries, 1, 0)
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })

	batchSize := 10
	var batch *mem.Batch
	for _, entry := range entries {
		if batch == nil {
			batch = mem.NewBatch()
		}
		batch.AddEntry(common.KV{
			Key:   encoding.EncodeVersion(entry.keyStart, 0),
			Value: entry.keyEnd,
		})
		if batch.Len() == batchSize {
			err := store.Write(batch)
			require.NoError(t, err)
			batch = nil
			err = store.forceReplaceMemtable()
			require.NoError(t, err)
		}
	}

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	iteratePairs(t, iter, 0, numEntries)
}

func TestPeriodicMemtableReplace(t *testing.T) {
	// Make sure that the memtable is periodically replaced
	maxReplaceTime := 250 * time.Millisecond
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableMaxReplaceInterval = maxReplaceTime

	store := SetupStoreWithConfig(t, cfg)
	store.updateLastCompletedVersion(math.MaxInt64)
	defer stopStore(t, store)

	cs := store.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert

	numIters := 3
	ks := 0
	start := time.Now()
	for i := 0; i < numIters; i++ {
		storeSize := cs.Size()

		writeKVs(t, store, ks, 10)

		log.Debugf("store size is currently %d", storeSize)

		testutils.WaitUntil(t, func() (bool, error) {
			// Wait until the SSTable should has been pushed to the cloud store
			return cs.Size() == storeSize+1, nil
		})

		// Make sure all the data is there
		log.Debug("creating iterator....")
		iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
		require.NoError(t, err)
		iteratePairs(t, iter, 0, 10*(i+1))
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)
		ks += 10
	}
	dur := time.Since(start)
	require.True(t, dur > time.Duration(numIters)*maxReplaceTime)
}

func TestMemtableReplaceWhenMaxSizeReached(t *testing.T) {

	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableMaxSizeBytes = 16 * 1024 * 1024
	cfg.MemtableFlushQueueMaxSize = 10
	cfg.MemtableMaxReplaceInterval = 10 * time.Minute

	store := SetupStoreWithConfig(t, cfg)
	store.updateLastCompletedVersion(math.MaxInt64)

	defer stopStore(t, store)

	cs := store.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert

	// Add entries until several SSTables have been flushed to cloud store
	numSSTables := 10
	ks := 0
	sizeStart := cs.Size()
	vs := make([]byte, 1000)
	for i := 0; i < len(vs); i++ {
		vs[i] = 'x'
	}
	batchSize := 100
	for cs.Size() < sizeStart+numSSTables {
		writeKVsWithParams(t, store, ks, batchSize, string(vs), false)
		ks += batchSize
	}

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)
	iteratePairsWithParams(t, iter, 0, ks, string(vs))
}

func SetupStore(t *testing.T) *Store {
	t.Helper()
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	return SetupStoreWithConfig(t, cfg)
}

func SetupStoreWithConfig(t testing.TB, conf conf.Config) *Store {
	t.Helper()
	conf.L0MaxTablesBeforeBlocking = math.MaxInt
	cloudStore := dev.NewInMemStore(100 * time.Millisecond)
	lmClient := &levels.InMemClient{}
	bi := &testCommandBatchIngestor{}
	tabCache, err := tabcache.NewTableCache(cloudStore, &conf)
	require.NoError(t, err)
	levelManager := levels.NewLevelManager(&conf, cloudStore, tabCache, bi.ingest, false, false, false)
	bi.lm = levelManager
	lmClient.LevelManager = levelManager
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)
	tableCache, err := tabcache.NewTableCache(cloudStore, &conf)
	require.NoError(t, err)
	err = tableCache.Start()
	require.NoError(t, err)
	store := NewStore(cloudStore, lmClient, tableCache, conf)
	err = store.Start()
	require.NoError(t, err)
	return store
}

func stopStore(t testing.TB, store *Store) {
	err := store.Stop()
	require.NoError(t, err)
}

func iteratePairs(t *testing.T, iter iteration.Iterator, keyStart int, numPairs int) {
	iteratePairsWithParams(t, iter, keyStart, numPairs, "")
}

func iteratePairsWithParams(t *testing.T, iter iteration.Iterator, keyStart int, numPairs int, valueSuffix string) {
	t.Helper()
	for i := 0; i < numPairs; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		curr.Key = curr.Key[:len(curr.Key)-8] // strip version
		require.Equal(t, []byte(fmt.Sprintf("prefix/key-%010d", keyStart+i)), curr.Key)
		require.Equal(t, []byte(fmt.Sprintf("prefix/value-%010d%s", keyStart+i, valueSuffix)), curr.Value)
		if i != numPairs-1 {
			err := iter.Next()
			require.NoError(t, err)
		}
	}
}

func requireIterValid(t *testing.T, iter iteration.Iterator, valid bool) {
	t.Helper()
	v, err := iter.IsValid()
	require.NoError(t, err)
	require.Equal(t, valid, v)
}

func writeKVs(t *testing.T, store *Store, keyStart int, numPairs int) { //nolint:unparam
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, "", false)
}

func writeKVsWithTombstones(t *testing.T, store *Store, keyStart int, numPairs int) {
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, "", true)
}

func writeKVsWithValueSuffix(t *testing.T, store *Store, keyStart int, numPairs int, valueSuffix string) {
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, valueSuffix, false)
}

func writeKVsWithParams(t testing.TB, store *Store, keyStart int, numPairs int, valueSuffix string,
	tombstones bool) {
	t.Helper()
	batch := mem.NewBatch()
	for i := 0; i < numPairs; i++ {
		k := []byte(fmt.Sprintf("prefix/key-%010d", keyStart+i))
		var v []byte
		if tombstones {
			v = nil
		} else {
			v = []byte(fmt.Sprintf("prefix/value-%010d%s", keyStart+i, valueSuffix))
		}
		batch.AddEntry(common.KV{
			Key:   encoding.EncodeVersion(k, 0),
			Value: v,
		})
	}
	err := store.Write(batch)
	require.NoError(t, err)
}

func prepareRanges(numEntries int, rangeSize int, rangeGap int) []rng {
	entries := make([]rng, 0, numEntries)
	start := 0
	for i := 0; i < numEntries; i++ {
		ks := fmt.Sprintf("prefix/key-%010d", i)
		ke := fmt.Sprintf("prefix/value-%010d", i)
		start += rangeSize + rangeGap
		entries = append(entries, rng{
			keyStart: []byte(ks),
			keyEnd:   []byte(ke),
		})
	}
	return entries
}

type rng struct {
	keyStart []byte
	keyEnd   []byte
}
