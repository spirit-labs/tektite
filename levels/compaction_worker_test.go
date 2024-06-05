package levels

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestCompactionIncrementingData(t *testing.T) {

	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	// Make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	rangeStart := 0
	numEntriesPerTable := 10
	tableCount := 0
	numLevels := 4
	// we will generate sufficient tables to fill approx numLevels levels
	numTables := getNumTablesToFill(numLevels, l0CompactionTrigger, l1CompactionTrigger, levelMultiplier)
	numTables-- // subtract one as we don't want l0 to be completely full and thus trigger a compaction at the end

	for tableCount < numTables {
		tableName := fmt.Sprintf("sst-%05d", tableCount)
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRange(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore())
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	// Now we wait for all in-progress jobs to complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Now we verify the data - generate a merging iterator over all the data in the lsm
	mi := createIterator(t, lm, nil, nil)
	for i := 0; i < numTables*numEntriesPerTable; i++ {
		valid, err := mi.IsValid()
		require.NoError(t, err)
		require.True(t, valid)
		curr := mi.Current()
		expectedKey := fmt.Sprintf("key%06d", i)
		expectedVal := fmt.Sprintf("val%06d", i)
		require.Equal(t, expectedKey, string(curr.Key[:len(curr.Key)-8])) // trim version
		require.Equal(t, expectedVal, string(curr.Value))
		err = mi.Next()
		require.NoError(t, err)
	}
	valid, err := mi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)

	err = lm.Validate(true)
	require.NoError(t, err)
}

func TestCompactionOverwritingData(t *testing.T) {
	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	numKeys := 10000
	numTables := 2000
	numEntriesPerTable := 10
	startVersion := 100
	versionsMap := map[int]int{}
	keysMap := map[int]int{}

	// We set last flushed to a high value to make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	for i := 0; i < numTables; i++ {
		si := &iteration.StaticIterator{}
		randKeys := make([]int, numEntriesPerTable)

		// choose some rand keys (no duplicates allowed) and sort them
		randKeysMap := make(map[int]struct{}, numEntriesPerTable)
		for j := 0; j < numEntriesPerTable; j++ {
			for {
				r := random.Intn(numKeys)
				_, exists := randKeysMap[r]
				if !exists {
					randKeys[j] = r
					randKeysMap[r] = struct{}{}
					break
				}
			}
		}
		sort.Ints(randKeys)

		for _, k := range randKeys {
			key := fmt.Sprintf("key%06d", k)
			v := random.Intn(numKeys)
			val := fmt.Sprintf("val%06d", v)
			ver, ok := versionsMap[k]
			if !ok {
				ver = startVersion
				versionsMap[k] = startVersion + 1
			} else {
				versionsMap[k]++
			}
			si.AddKV(encoding.EncodeVersion([]byte(key), uint64(ver)), []byte(val))
			keysMap[k] = v
		}

		table, smallestKey, largestKey, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, si)
		require.NoError(t, err)
		buff := table.Serialize()

		sstName := fmt.Sprintf("sst-%06d", i)
		err = lm.GetObjectStore().Put([]byte(sstName), buff)
		require.NoError(t, err)

		addTable(t, lm, sstName, smallestKey, largestKey)
	}

	// Now we wait for all in-progress jobs to complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Check the data
	expectedKeys := make([]int, len(keysMap))
	i := 0
	for k := range keysMap {
		expectedKeys[i] = k
		i++
	}
	sort.Ints(expectedKeys)

	mi := createIterator(t, lm, nil, nil)
	for _, k := range expectedKeys {

		valid, err := mi.IsValid()
		require.NoError(t, err)
		require.True(t, valid)
		curr := mi.Current()

		v, ok := keysMap[k]
		require.True(t, ok)
		expectedVersion, ok := versionsMap[k]
		require.True(t, ok)
		expectedVersion--

		expectedKey := fmt.Sprintf("key%06d", k)
		expectedVal := fmt.Sprintf("val%06d", v)

		require.Equal(t, expectedKey, string(curr.Key[:len(curr.Key)-8])) // trim version
		require.Equal(t, expectedVal, string(curr.Value))

		ver := math.MaxUint64 - binary.BigEndian.Uint64(curr.Key[len(curr.Key)-8:])
		require.Equal(t, expectedVersion, int(ver))

		err = mi.Next()
		require.NoError(t, err)
	}
	valid, err := mi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)

	err = lm.Validate(true)
	require.NoError(t, err)
}

func TestCompactionDeleteData(t *testing.T) {
	t.Skip("skipping as flaky test")

	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	// Make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	rangeStart := 0
	numEntriesPerTable := 10
	tableCount := 0
	numLevels := 4
	numTables := getNumTablesToFill(numLevels, l0CompactionTrigger, l1CompactionTrigger, levelMultiplier)
	numTables-- // subtract one as we don't want l0 to be completely full and thus trigger a compaction at the end

	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRange(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore())
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Now we will write deletes for this data
	rangeStart = 0
	tableCount = 0
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTombstoneTableWithKeyRange(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore())
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}
	endRangeStart := rangeStart

	// Now we wait for all in-progress jobs to complete
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// iterator shouldn't show anything
	mi := createIterator(t, lm, nil, nil)
	valid, err := mi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)

	// now we add more data, this should push all the deletes out
	rangeStart = endRangeStart
	tableCount = 0
	// we add some extra tables here to make sure all deletes are pushed out
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRange(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore())
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	// Now we wait for all in-progress jobs to complete
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Now, should be no deletes
	for i := 0; i <= lm.getLastLevel(); i++ {
		iter, err := lm.LevelIterator(i)
		require.NoError(t, err)
		for {
			te, err := iter.Next()
			require.NoError(t, err)
			if te == nil {
				break
			}
			require.Equal(t, float64(0), te.DeleteRatio)
		}
	}

	err = lm.Validate(true)
	require.NoError(t, err)
}

func TestRandomUpdateDeleteData(t *testing.T) {
	t.Skip("skipping as flaky test")

	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	// Make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	numKeys := 10000
	numTables := 3000
	numEntriesPerTable := 10

	type entry struct {
		val     int
		ver     int
		updated bool
		deleted bool
	}

	keys := make([]entry, numKeys)

	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	for i := 0; i < numTables; i++ {
		si := &iteration.StaticIterator{}
		randKeys := make([]int, numEntriesPerTable)

		// choose some rand keys (no duplicates allowed) and sort them
		randKeysMap := make(map[int]struct{}, numEntriesPerTable) // dedupper
		for j := 0; j < numEntriesPerTable; j++ {
			for {
				r := random.Intn(numKeys)
				_, exists := randKeysMap[r]
				if !exists {
					randKeys[j] = r
					randKeysMap[r] = struct{}{}
					break
				}
			}
		}
		sort.Ints(randKeys)

		m := map[int]struct{}{}
		for _, k := range randKeys {
			_, exists := m[k]
			if exists {
				panic("duplicate")
			}
			m[k] = struct{}{}
		}

		for _, k := range randKeys {
			key := fmt.Sprintf("key%06d", k)

			entry := &keys[k]

			update := true
			if entry.updated {
				// choose update or delete by tossing a coin
				update = random.Intn(2) == 1
			}
			ver := entry.ver

			if update {
				v := random.Intn(numKeys)
				val := fmt.Sprintf("val%06d", v)
				si.AddKV(encoding.EncodeVersion([]byte(key), uint64(ver)), []byte(val))
				entry.updated = true
				entry.deleted = false
				entry.val = v
			} else {
				// delete
				si.AddKV(encoding.EncodeVersion([]byte(key), uint64(ver)), nil)
				entry.updated = false
				entry.deleted = true
				entry.val = 0
			}
			entry.ver = ver + 1
		}

		table, smallestKey, largestKey, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, si)
		require.NoError(t, err)
		buff := table.Serialize()

		sstName := fmt.Sprintf("sst-%06d", i)
		err = lm.GetObjectStore().Put([]byte(sstName), buff)
		require.NoError(t, err)

		addTable(t, lm, sstName, smallestKey, largestKey)
	}

	// Now we wait for all in-progress jobs to complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Check the data

	mi := createIterator(t, lm, nil, nil)
	for k, entry := range keys {
		if entry.deleted {
			continue
		}
		if !entry.updated {
			// unused entry
			continue
		}

		valid, err := mi.IsValid()
		require.NoError(t, err)
		require.True(t, valid)
		curr := mi.Current()

		ver := math.MaxUint64 - binary.BigEndian.Uint64(curr.Key[len(curr.Key)-8:])

		expectedVersion := entry.ver - 1
		expectedKey := fmt.Sprintf("key%06d", k)
		expectedVal := fmt.Sprintf("val%06d", entry.val)

		require.Equal(t, expectedKey, string(curr.Key[:len(curr.Key)-8])) // trim version
		require.Equal(t, expectedVal, string(curr.Value))

		require.Equal(t, expectedVersion, int(ver))

		err = mi.Next()
		require.NoError(t, err)
	}
	valid, err := mi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)

	err = lm.Validate(true)
	require.NoError(t, err)
}

func getNumTablesToFill(numLevels int, l0Trigger int, l1Trigger int, multiplier int) int {
	if numLevels == 1 {
		return l0Trigger
	}
	levelTables := l1Trigger
	numTables := l1Trigger
	for i := 2; i < numLevels; i++ {
		levelTables = levelTables * multiplier
		numTables += levelTables
	}
	return numTables + l0Trigger
}

func TestCompactionExpiredPrefix(t *testing.T) {
	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	// Make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	rangeStart := 0
	numEntriesPerTable := 10
	tableCount := 0
	numLevels := 4
	// we will generate sufficient tables to fill approx numLevels levels
	numTables := getNumTablesToFill(numLevels, l0CompactionTrigger, l1CompactionTrigger, levelMultiplier)
	numTables-- // subtract one as we don't want l0 to be completely full and thus trigger a compaction at the end

	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), "prefix1")
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	// wait for compaction jobs to complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	retentionMs := 10
	err = lm.RegisterPrefixRetentions([]retention.PrefixRetention{{
		Prefix:    []byte("prefix1"),
		Retention: uint64(retentionMs),
	}}, false, 0)
	require.NoError(t, err)

	// Wait for retention to expire
	time.Sleep(time.Duration(retentionMs) * time.Millisecond)

	// Add more keys - different prefix - this should cause the expired prefixes to be compacted out
	rangeStart = 0
	tableCount = 0
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), "prefix2")
		addTable(t, lm, tableName, smallestKey, largestKey)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	// Wait for compactions to complete
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// There should be no prefix1 in any levels apart from the last - when the lsm is originally loaded with prefix1,
	// there will be some prefix1 tables in the last level which won't be full and prefix1 won't be expired.
	// when we add prefix2 some will get pushed to last level but it won't be full yet and there is no overlap between
	// prefix1 and prefix2 tables so a merge won't occur, thus leaving prefix1 in last level. This is OK - it will
	// get removed once that level becomes full, and it gets pushed to next level.

	for i := 0; i < lm.getLastLevel(); i++ {
		iter, err := lm.LevelIterator(i)
		require.NoError(t, err)
		for {
			te, err := iter.Next()
			require.NoError(t, err)
			if te == nil {
				break
			}
			overlap := hasOverlap([]byte("prefix1"), []byte("prefix2"), te.RangeStart, te.RangeEnd)
			require.False(t, overlap)
		}
	}
}

func TestCompactionDeadVersions(t *testing.T) {
	l0CompactionTrigger := 4
	l1CompactionTrigger := 4
	levelMultiplier := 10
	lm, tearDown := setup(t, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = l0CompactionTrigger
		cfg.L1CompactionTrigger = l1CompactionTrigger
		cfg.L0MaxTablesBeforeBlocking = 2 * l0CompactionTrigger
		cfg.LevelMultiplier = levelMultiplier
	})
	defer tearDown(t)

	// Make sure all entries get compacted
	err := lm.StoreLastFlushedVersion(math.MaxInt64, false, 0)
	require.NoError(t, err)

	numEntriesPerTable := 10
	tableCount := 0
	numLevels := 4
	// we will generate sufficient tables to fill approx numLevels levels
	numTables := getNumTablesToFill(numLevels, l0CompactionTrigger, l1CompactionTrigger, levelMultiplier)
	numTables-- // subtract one as we don't want l0 to be completely full and thus trigger a compaction at the end

	// First add a table with version 1000

	tableName := uuid.New().String()
	smallestKey, largestKey := buildAndRegisterTableWithKeyRangeAndVersion(t, tableName, 0, 9,
		lm.GetObjectStore(), false, "", 1000)
	addTableWithMinMaxVersion(t, lm, tableName, smallestKey, largestKey, 1000, 1000)
	rangeStart := 10

	// Then a load of tables with version 1500
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), false, "", 1500)
		addTableWithMinMaxVersion(t, lm, tableName, smallestKey, largestKey, 1500, 1500)
		rangeStart += numEntriesPerTable
		tableCount++
	}

	// Then a table with version 2000
	tableName = uuid.New().String()
	lastRangeStart := rangeStart
	smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName, rangeStart, rangeStart+numEntriesPerTable-1,
		lm.GetObjectStore(), false, "", 2000)
	addTableWithMinMaxVersion(t, lm, tableName, smallestKey, largestKey, 2000, 2000)

	// wait for compaction jobs to complete
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Now register deadversions which includes 1500
	rng := VersionRange{
		VersionStart: 1300,
		VersionEnd:   1700,
	}

	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)

	// a big compaction should ensue

	// wait for compaction jobs to complete
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	// Nothing should be left apart from the first and last table

	iter := createIterator(t, lm, nil, nil)
	i := 0
	for {
		valid, err := iter.IsValid()
		require.NoError(t, err)
		if !valid {
			break
		}
		key := iter.Current().Key
		keyNoversion := key[:len(key)-8]

		expectedKey := fmt.Sprintf("key%06d", i)
		require.Equal(t, expectedKey, string(keyNoversion))

		err = iter.Next()
		require.NoError(t, err)
		i++
		if i == 10 {
			i = lastRangeStart
		}
	}
}

func setup(t *testing.T, cfgFunc func(cfg *conf.Config)) (*LevelManager, func(t *testing.T)) {
	lm, tearDown := setupLevelManagerWithDedup(t, true, true, false, cfgFunc)

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.CompactionWorkerCount = 4
	// we set this small to get about 10 entries per table so we can fill up levels
	cfg.CompactionMaxSSTableSize = 550

	lmClientFactory := &inMemClientFactory{lm: lm}
	tableCache, err := tabcache.NewTableCache(lm.GetObjectStore(), cfg)
	require.NoError(t, err)
	err = tableCache.Start()
	require.NoError(t, err)
	cws := NewCompactionWorkerService(cfg, lmClientFactory, tableCache, lm.GetObjectStore())
	err = cws.Start()
	require.NoError(t, err)
	tearDown2 := func(t *testing.T) {
		err := cws.Stop()
		require.NoError(t, err)
		tearDown(t)
	}
	return lm, tearDown2
}

type inMemClientFactory struct {
	lm *LevelManager
}

func (i *inMemClientFactory) CreateLevelManagerClient() Client {
	return &InMemClient{LevelManager: i.lm}
}

func createIterator(t *testing.T, lm *LevelManager, keyStart []byte, keyEnd []byte) *iteration.MergingIterator {
	otids, _, _, err := lm.GetTableIDsForRange(keyStart, keyEnd)
	require.NoError(t, err)
	var chainIters []iteration.Iterator
	for _, notids := range otids {
		var iters []iteration.Iterator
		for _, id := range notids {
			buff, err := lm.GetObjectStore().Get(id)
			require.NoError(t, err)
			require.NotNil(t, buff)
			sstable := &sst.SSTable{}
			sstable.Deserialize(buff, 0)
			iter, err := sstable.NewIterator(nil, nil)
			require.NoError(t, err)
			iters = append(iters, iter)
		}
		chainIter := iteration.NewChainingIterator(iters)
		chainIters = append(chainIters, chainIter)
	}
	mi, err := iteration.NewMergingIterator(chainIters, false, math.MaxUint64)
	require.NoError(t, err)
	return mi
}

func buildAndRegisterTableWithKeyRange(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, false, "", 0)
}

func buildAndRegisterTableWithKeyRangeWithPrefix(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client, keyPrefix string) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, false, keyPrefix, 0)
}

func buildAndRegisterTombstoneTableWithKeyRange(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, true, "", 0)
}

func buildAndRegisterTableWithKeyRangeAndVersion(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client, tombstones bool, keyPrefix string, version int) ([]byte, []byte) {
	si := &iteration.StaticIterator{}
	for i := rangeStart; i <= rangeEnd; i++ {
		key := fmt.Sprintf("%skey%06d", keyPrefix, i)
		var val []byte
		if !tombstones {
			val = []byte(fmt.Sprintf("val%06d", i))
		}
		si.AddKV(encoding.EncodeVersion([]byte(key), uint64(version)), val)
	}
	table, smallestKey, largestKey, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, si)
	require.NoError(t, err)
	buff := table.Serialize()
	err = cloudStore.Put([]byte(name), buff)
	require.NoError(t, err)
	return smallestKey, largestKey
}

func addTable(t *testing.T, lm *LevelManager, tableName string, rangeStart []byte, rangeEnd []byte) {
	addTableWithMinMaxVersion(t, lm, tableName, rangeStart, rangeEnd, 0, 0)
}

func addTableWithMinMaxVersion(t *testing.T, lm *LevelManager, tableName string, rangeStart []byte, rangeEnd []byte,
	minVersion int, maxVersion int) {
	regBatch := RegistrationBatch{
		ClusterName: "test_cluster",
		Registrations: []RegistrationEntry{
			{
				Level:      0,
				TableID:    []byte(tableName),
				KeyStart:   rangeStart,
				KeyEnd:     rangeEnd,
				MinVersion: uint64(minVersion),
				MaxVersion: uint64(maxVersion),
			},
		},
	}
	validateRegBatch(regBatch, lm.GetObjectStore())
	for {
		err := lm.ApplyChanges(regBatch, false, 0)
		if err == nil {
			break
		}
		if common.IsUnavailableError(err) {
			require.Equal(t,
				"TEK1001 - unable to accept L0 add - L0 is full", err.Error())
			time.Sleep(1 * time.Millisecond)
		} else {
			require.NoError(t, err)
		}
	}
}
