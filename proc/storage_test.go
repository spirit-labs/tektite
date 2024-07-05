package proc

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
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
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

func SetupProcessorStoreWithConfig(t testing.TB, conf conf.Config) (*ProcessorStore, *ProcessorManager) {
	t.Helper()
	conf.L0MaxTablesBeforeBlocking = math.MaxInt
	cloudStore := dev.NewInMemStore(0)
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
	stateMgr := &testClustStateMgr{}
	vmgrClient := &testVmgrClient{}
	mgr := NewProcessorManagerWithVmgrClient(stateMgr, &testReceiverInfoProvider{}, &conf, nil,
		createTestBatchHandler, nil, &testIngestNotifier{}, vmgrClient, cloudStore, lmClient,
		tabCache, false)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	cs := clustmgr.ClusterState{
		Version: 0,
		GroupStates: [][]clustmgr.GroupNode{
			{clustmgr.GroupNode{
				NodeID:        0,
				Leader:        true,
				Valid:         true,
				JoinedVersion: 0,
			}},
		},
	}
	err = mgr.HandleClusterState(cs)
	require.NoError(t, err)
	proc := mgr.GetProcessor(0)
	require.NotNil(t, proc)
	processorStore := proc.(*processor).store.(*ProcessorStore)
	return processorStore, mgr
}

type testCommandBatchIngestor struct {
	lm *levels.LevelManager
}

func (tc *testCommandBatchIngestor) ingest(buff []byte, complFunc func(error)) {
	regBatch := levels.RegistrationBatch{}
	regBatch.Deserialize(buff, 1)
	go func() {
		err := tc.lm.ApplyChanges(regBatch, false, 0)
		complFunc(err)
	}()
}

func TestGet(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.MemtableMaxSizeBytes = 5 * 1024 * 1024
	cfg.MemtableFlushQueueMaxSize = 10
	cfg.MemtableMaxReplaceInterval = 10 * time.Minute
	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	// We pretend all versions are complete so memtables get pushed to cloud straight away
	atomic.StoreInt64(&mgr.lastCompletedVersion, math.MaxInt64)

	cs := mgr.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert
	sizeStart := cs.Size()
	batchSize := 100
	numSSTables := 4
	ks := 0
	// Write data until numSSTables sstables have been flushed to the object store
	for cs.Size() < sizeStart+numSSTables {
		writeKVs(t, procStore, ks, batchSize)
		ks += batchSize
	}

	// Now we get each key - some will come from active memtable, others flush queue, others sstable
	for i := 0; i < ks; i++ {
		k := []byte(fmt.Sprintf("prefix/key-%010d", i))
		v, err := procStore.Get(k)
		require.NoError(t, err)
		expectedV := []byte(fmt.Sprintf("prefix/value-%010d", i))
		require.Equal(t, expectedV, v)
	}

	// Now get some non-existent keys
	v, err := procStore.Get([]byte("foobar"))
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = procStore.Get([]byte(fmt.Sprintf("prefix/key-%010d", ks)))
	require.NoError(t, err)
	require.Nil(t, v)
	v, err = procStore.Get([]byte("prefix/key-"))
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

	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	atomic.StoreInt64(&mgr.lastCompletedVersion, math.MaxInt64)

	cs := mgr.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert
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
	writeBatchInSSTable(t, procStore, batch)
	require.Equal(t, sizeStart+1, cs.Size())
	batch = mem.NewBatch()
	batch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key1"), 0),
		Value: []byte("val1_1"),
	})
	batch.AddEntry(common.KV{
		Key: encoding.EncodeVersion([]byte("key2"), 0),
	})
	writeBatchInSSTable(t, procStore, batch)
	require.Equal(t, sizeStart+2, cs.Size())

	iter, err := procStore.NewIterator(nil, nil, math.MaxUint64, false)
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

func writeBatchInSSTable(t *testing.T, store *ProcessorStore, batch *mem.Batch) {
	var errVal atomic.Value
	err := store.Write(batch)
	require.NoError(t, err)
	ch := make(chan error, 1)
	cb := func(err error) {
		ch <- err
	}
	err = store.flush(cb)
	require.NoError(t, err)
	err = <-ch
	require.NoError(t, err)
	ev := errVal.Load()
	if ev != nil {
		//goland:noinspection GoTypeAssertionOnErrors
		err = ev.(error)
		require.NoError(t, err)
	}
}

func TestSimpleIterate(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	writeKVs(t, procStore, 0, 10)

	iter, err := procStore.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)

	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIteratorPicksUpAddedDataAfterDataWithGreaterKeyAlreadyExists(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	// Test case where data already exists with greater key before iter is created
	// Then iter is created, and it's not valid
	// Then add data in range
	// And it picks up new data

	writeKVs(t, procStore, 5, 1)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 4))
	ke := []byte(fmt.Sprintf("prefix/key-%010d", 5))

	iter, err := procStore.NewIterator(ks, ke, math.MaxUint64, false)
	require.NoError(t, err)

	requireIterValid(t, iter, false)

	writeKVs(t, procStore, 4, 1)

	requireIterValid(t, iter, true)

	iteratePairs(t, iter, 4, 1)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateInRange(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))
	ke := []byte(fmt.Sprintf("prefix/key-%010d", 7))

	writeKVs(t, procStore, 0, 10)

	iter, err := procStore.NewIterator(ks, ke, math.MaxUint64, false)
	require.NoError(t, err)

	iteratePairs(t, iter, 3, 4)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateInRangeNoEndRange(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	procStore, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))

	writeKVs(t, procStore, 0, 10)

	iter, err := procStore.NewIterator(ks, nil, math.MaxUint64, false)
	require.NoError(t, err)

	iteratePairs(t, iter, 3, 7)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateThenDelete(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	store, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	writeKVs(t, store, 0, 10)

	iter, err := store.NewIterator(nil, nil, math.MaxUint64, false)
	require.NoError(t, err)

	iteratePairs(t, iter, 0, 5)

	requireIterValid(t, iter, true)

	// Then we delete the rest of them from the memtable
	writeKVsWithTombstones(t, store, 5, 5)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestPeriodicMemtableReplace(t *testing.T) {
	// Make sure that the memtable is periodically replaced
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	maxReplaceTime := 250 * time.Millisecond
	cfg.MemtableMaxReplaceInterval = maxReplaceTime
	store, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	cs := mgr.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert

	numIters := 3
	ks := 0
	start := time.Now()
	for i := 0; i < numIters; i++ {
		storeSize := cs.Size()

		writeKVs(t, store, ks, 10)

		testutils.WaitUntil(t, func() (bool, error) {
			// Wait until the SSTable should has been pushed to the cloud store
			return cs.Size() == storeSize+1, nil
		})

		// Make sure all the data is there
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
	store, mgr := SetupProcessorStoreWithConfig(t, cfg)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	atomic.StoreInt64(&mgr.lastCompletedVersion, math.MaxInt64)

	cs := mgr.cloudStoreClient.(*dev.InMemStore) //nolint:forcetypeassert

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

func iteratePairs(t *testing.T, iter iteration.Iterator, keyStart int, numPairs int) {
	iteratePairsWithParams(t, iter, keyStart, numPairs, "")
}

func iteratePairsWithParams(t *testing.T, iter iteration.Iterator, keyStart int, numPairs int, valueSuffix string) {
	t.Helper()
	for i := 0; i < numPairs; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		key := curr.Key[:len(curr.Key)-8] // strip version
		log.Debugf("got key %s value %s", string(key), string(iter.Current().Value))
		require.Equal(t, []byte(fmt.Sprintf("prefix/key-%010d", keyStart+i)), key)
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

func writeKVs(t *testing.T, store Store, keyStart int, numPairs int) { //nolint:unparam
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, "", false)
}

func writeKVsWithTombstones(t *testing.T, store Store, keyStart int, numPairs int) {
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, "", true)
}

func writeKVsWithValueSuffix(t *testing.T, store Store, keyStart int, numPairs int, valueSuffix string) {
	t.Helper()
	writeKVsWithParams(t, store, keyStart, numPairs, valueSuffix, false)
}

func writeKVsWithParams(t testing.TB, store Store, keyStart int, numPairs int, valueSuffix string,
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
