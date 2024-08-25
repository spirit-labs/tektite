package levels

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
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

	prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix = append(prefix, []byte("prefix1_")...)

	for tableCount < numTables {
		tableName := fmt.Sprintf("sst-%05d", tableCount)
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefix)
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

	require.GreaterOrEqual(t, lm.getLastLevel(), numLevels-1)

	// Now we verify the data - generate a merging iterator over all the data in the lsm
	mi := createIterator(t, lm, nil, nil)
	for i := 0; i < numTables*numEntriesPerTable; i++ {
		valid, curr, err := mi.Next()
		require.NoError(t, err)
		require.True(t, valid)
		pref := common.ByteSliceCopy(prefix)
		expectedKey := append(pref, []byte(fmt.Sprintf("key%06d", i))...)
		expectedVal := fmt.Sprintf("val%06d", i)
		require.Equal(t, expectedKey, curr.Key[:len(curr.Key)-8]) // trim version
		require.Equal(t, expectedVal, string(curr.Value))
	}
	valid, _, err := mi.Next()
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

	prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix = append(prefix, []byte("prefix1_")...)

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
			prefixCopy := common.ByteSliceCopy(prefix)
			key := append(prefixCopy, []byte(fmt.Sprintf("key%06d", k))...)
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
		err = lm.GetObjectStore().Put(context.Background(), conf.DefaultBucketName, sstName, buff)
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

		valid, curr, err := mi.Next()
		require.NoError(t, err)
		require.True(t, valid)

		v, ok := keysMap[k]
		require.True(t, ok)
		expectedVersion, ok := versionsMap[k]
		require.True(t, ok)
		expectedVersion--

		prefixCopy := common.ByteSliceCopy(prefix)
		expectedKey := append(prefixCopy, []byte(fmt.Sprintf("key%06d", k))...)
		expectedVal := fmt.Sprintf("val%06d", v)

		require.Equal(t, expectedKey, curr.Key[:len(curr.Key)-8]) // trim version
		require.Equal(t, expectedVal, string(curr.Value))

		ver := math.MaxUint64 - binary.BigEndian.Uint64(curr.Key[len(curr.Key)-8:])
		require.Equal(t, expectedVersion, int(ver))
	}
	valid, _, err := mi.Next()
	require.NoError(t, err)
	require.False(t, valid)

	err = lm.Validate(true)
	require.NoError(t, err)
}

func TestCompactionTombstones(t *testing.T) {

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

	prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix = append(prefix, []byte("prefix1_")...)

	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefix)
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

	lm.dumpLevelInfo()

	require.GreaterOrEqual(t, lm.getLastLevel(), numLevels-1)

	totSize := lm.GetStats().TotBytes

	// Now we will write tombstones for all this data
	rangeStart = 0
	tableCount = 0
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTombstoneTableWithKeyRangeAndPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefix)
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
	valid, _, err := mi.Next()
	require.NoError(t, err)
	require.False(t, valid)

	// now we add more data, this should push all the expired entries out, as we prioritise compaction of older
	// tables
	rangeStart = endRangeStart
	tableCount = 0
	// we add some extra tables here to make sure all deletes are pushed out
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefix)
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

	size := lm.GetStats().TotBytes

	// Not always exactly equal as can have some deletes in last level
	require.Less(t, size, int(float64(totSize)*1.05))

	err = lm.Validate(true)
	require.NoError(t, err)
}

func TestRandomUpdateDeleteData(t *testing.T) {

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

	prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix = append(prefix, []byte("prefix1_")...)

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
			prefixCopy := common.ByteSliceCopy(prefix)
			key := append(prefixCopy, []byte(fmt.Sprintf("key%06d", k))...)

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
		err = lm.GetObjectStore().Put(context.Background(), conf.DefaultBucketName, sstName, buff)
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

		valid, curr, err := mi.Next()
		require.NoError(t, err)
		require.True(t, valid)

		ver := math.MaxUint64 - binary.BigEndian.Uint64(curr.Key[len(curr.Key)-8:])

		expectedVersion := entry.ver - 1
		prefixCopy := common.ByteSliceCopy(prefix)
		expectedKey := append(prefixCopy, []byte(fmt.Sprintf("key%06d", k))...)
		expectedVal := fmt.Sprintf("val%06d", entry.val)

		require.Equal(t, expectedKey, curr.Key[:len(curr.Key)-8]) // trim version
		require.Equal(t, expectedVal, string(curr.Value))

		require.Equal(t, expectedVersion, int(ver))
	}
	valid, _, err := mi.Next()
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

	retention := 2 * time.Second
	prefix1 := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix1 = append(prefix1, []byte("prefix1_")...)
	expiredSlabID := int(binary.BigEndian.Uint64(prefix1[16:]))
	err = lm.RegisterSlabRetention(expiredSlabID, retention, false, 0)
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
			lm.GetObjectStore(), prefix1)
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

	// Wait for retention to expire
	time.Sleep(retention)

	// Add more keys - different prefix - this should cause the expired prefixes to be compacted out

	prefix2 := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix2 = append(prefix2, []byte("prefix2_")...)

	rangeStart = 0
	tableCount = 0
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefix2)
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

	endRange := common.IncBigEndianBytes(prefix1)
	for i := 0; i < lm.getLastLevel(); i++ {
		iter, err := lm.LevelIterator(i)
		require.NoError(t, err)
		for {
			te, err := iter.Next()
			require.NoError(t, err)
			if te == nil {
				break
			}
			overlap := hasOverlap(prefix1, endRange, te.RangeStart, te.RangeEnd)
			require.False(t, overlap)
		}
	}
}

func TestCompactionDeadVersions(t *testing.T) {
	l0CompactionTrigger := 2
	l1CompactionTrigger := 20
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

	prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
	prefix = append(prefix, []byte("prefix1_")...)

	tableName1 := uuid.New().String()
	smallestKey, largestKey := buildAndRegisterTableWithKeyRangeAndVersion(t, tableName1, 0, 9,
		lm.GetObjectStore(), false, prefix, 1000)
	addTableWithMinMaxVersion(t, lm, tableName1, smallestKey, largestKey, 1000, 1100)

	tableName2 := uuid.New().String()
	smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName2, 10, 19,
		lm.GetObjectStore(), false, prefix, 1000)
	addTableWithMinMaxVersion(t, lm, tableName2, smallestKey, largestKey, 1500, 1600)

	tableName3 := uuid.New().String()
	smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName3, 20, 29,
		lm.GetObjectStore(), false, prefix, 1000)
	addTableWithMinMaxVersion(t, lm, tableName3, smallestKey, largestKey, 1700, 1800)

	// Now register deadversions which includes 1500
	rng := VersionRange{
		VersionStart: 1500,
		VersionEnd:   1550,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)

	// Add a couple more tables to force the previous once out

	tableName4 := uuid.New().String()
	smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName4, 30, 39,
		lm.GetObjectStore(), false, prefix, 1000)
	addTableWithMinMaxVersion(t, lm, tableName4, smallestKey, largestKey, 10000, 20000)

	tableName5 := uuid.New().String()
	smallestKey, largestKey = buildAndRegisterTableWithKeyRangeAndVersion(t, tableName5, 40, 49,
		lm.GetObjectStore(), false, prefix, 1000)
	addTableWithMinMaxVersion(t, lm, tableName5, smallestKey, largestKey, 10000, 20000)

	err = lm.forceCompaction(0, 3)
	require.NoError(t, err)

	// Should be no data in range 10-19 (incl)
	keyStart := []byte(fmt.Sprintf("%skey%05d", string(prefix), 10))
	keyEnd := []byte(fmt.Sprintf("%skey%05d", string(prefix), 20))

	result, err := lm.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)
	require.Equal(t, 0, len(result.L0Results))
	require.Equal(t, 0, len(result.L1PlusResults))

	// Should be no dead versions
	for i := 0; i <= lm.getLastLevel(); i++ {
		iter, err := lm.LevelIterator(i)
		require.NoError(t, err)
		for {
			te, err := iter.Next()
			require.NoError(t, err)
			if te == nil {
				break
			}
			require.Nil(t, te.DeadVersionRanges)
		}
	}
}

func TestCompactionPrefixDeletions(t *testing.T) {
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

	var prefixes [][]byte
	for i := 0; i < 3; i++ {
		prefix := []byte("xxxxxxxxxxxxxxxx") // partition hash
		prefix = append(prefix, []byte(fmt.Sprintf("prefix%d_", i))...)
		prefixes = append(prefixes, prefix)
	}

	// Build and add tables with 3 different prefixes
	for tableCount < numTables {
		tableName := uuid.New().String()
		rangeEnd := rangeStart + numEntriesPerTable - 1
		smallestKey, largestKey := buildAndRegisterTableWithKeyRangeWithPrefix(t, tableName, rangeStart, rangeEnd,
			lm.GetObjectStore(), prefixes[tableCount%3])
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

	// Make sure L0 is empty
	err = lm.forceCompaction(0, 1)
	require.NoError(t, err)
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		l0Stats := lm.GetStats().LevelStats[0]
		return l0Stats.Tables == 0, nil
	}, 30*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	log.Debug("adding deletion bomb")

	// Now we'll add a deletion bomb
	si := &iteration.StaticIterator{}
	tombstone := common.ByteSliceCopy(prefixes[1])
	endMarker := append(common.IncBigEndianBytes(tombstone), 0)
	si.AddKV(encoding.EncodeVersion(tombstone, math.MaxUint64), nil)
	si.AddKV(encoding.EncodeVersion(endMarker, math.MaxUint64), []byte{'x'})
	log.Debugf("added tombstone:%s", tombstone)
	log.Debugf("added endmarker:%s", endMarker)

	table, smallestKey, largestKey, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, si)
	require.NoError(t, err)
	buff := table.Serialize()
	tableName := uuid.New().String()
	log.Debugf("deletion bomb table is %s", tableName)
	err = lm.GetObjectStore().Put(context.Background(), conf.DefaultBucketName, tableName, buff)
	require.NoError(t, err)
	addTable(t, lm, tableName, smallestKey, largestKey)

	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		lastLevel := lm.getLastLevel()
		for level := 0; level < lastLevel; level++ {
			// Force compaction at each level to let the delete bomb progress
			log.Debugf("forcing compaction at level %d", level)
			err = lm.forceCompaction(level, 1)
			require.NoError(t, err)

			// wait for compaction jobs to complete
			ok, err = testutils.WaitUntilWithError(func() (bool, error) {
				stats := lm.GetCompactionStats()
				return stats.QueuedJobs == 0 && stats.InProgressJobs == 0, nil
			}, 30*time.Second, 1*time.Millisecond)
			require.NoError(t, err)
			require.True(t, ok)

			log.Debugf("compaction complete at level %d", level)
		}

		// There should be no prefix1 in any levels
		endRange := common.IncBigEndianBytes(prefixes[1])
		for i := 0; i < lm.getLastLevel(); i++ {
			iter, err := lm.LevelIterator(i)
			require.NoError(t, err)
			for {
				te, err := iter.Next()
				require.NoError(t, err)
				if te == nil {
					break
				}
				if hasOverlap(prefixes[1], endRange, te.RangeStart, te.RangeEnd) {
					return false, nil
				}
			}
		}
		return true, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

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
	cws := NewCompactionWorkerService(cfg, lmClientFactory, tableCache, lm.GetObjectStore(), true)
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
	otids, err := lm.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)
	var allLevelsChainIters []iteration.Iterator
	var l0GroupIters []iteration.Iterator
	for _, groupResult := range otids.L0Results {
		var iters []iteration.Iterator
		for _, info := range groupResult {
			buff, err := lm.GetObjectStore().Get(context.Background(), conf.DefaultBucketName, string(info.ID))
			require.NoError(t, err)
			require.NotNil(t, buff)
			sstable := &sst.SSTable{}
			sstable.Deserialize(buff, 0)
			iter, err := sstable.NewIterator(nil, nil)
			require.NoError(t, err)
			iters = append(iters, iter)
		}
		mi, err := iteration.NewMergingIterator(iters, true, math.MaxUint64)
		require.NoError(t, err)
		l0GroupIters = append(l0GroupIters, mi)
	}
	allLevelsChainIters = append(allLevelsChainIters, iteration.NewChainingIterator(l0GroupIters))

	for _, levelTableResult := range otids.L1PlusResults {
		var iters []iteration.Iterator
		for _, info := range levelTableResult {
			buff, err := lm.GetObjectStore().Get(context.Background(), conf.DefaultBucketName, string(info.ID))
			require.NoError(t, err)
			require.NotNil(t, buff)
			sstable := &sst.SSTable{}
			sstable.Deserialize(buff, 0)
			iter, err := sstable.NewIterator(nil, nil)
			require.NoError(t, err)
			iters = append(iters, iter)
		}
		chainIter := iteration.NewChainingIterator(iters)
		allLevelsChainIters = append(allLevelsChainIters, chainIter)
	}
	mi, err := iteration.NewMergingIterator(allLevelsChainIters, false, math.MaxUint64)
	require.NoError(t, err)
	return mi
}

func buildAndRegisterTableWithKeyRange(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, false, nil, 0)
}

func buildAndRegisterTableWithKeyRangeWithPrefix(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client, keyPrefix []byte) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, false, keyPrefix, 0)
}

func buildAndRegisterTombstoneTableWithKeyRangeAndPrefix(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client, keyPrefix []byte) ([]byte, []byte) {
	return buildAndRegisterTableWithKeyRangeAndVersion(t, name, rangeStart, rangeEnd, cloudStore, true, keyPrefix, 0)
}

func buildAndRegisterTableWithKeyRangeAndVersion(t *testing.T, name string, rangeStart int, rangeEnd int,
	cloudStore objstore.Client, tombstones bool, keyPrefix []byte, version int) ([]byte, []byte) {
	si := &iteration.StaticIterator{}
	for i := rangeStart; i <= rangeEnd; i++ {
		key := common.ByteSliceCopy(keyPrefix)
		key = append(key, []byte(fmt.Sprintf("key%06d", i))...)
		var val []byte
		if !tombstones {
			val = []byte(fmt.Sprintf("val%06d", i))
		}
		si.AddKV(encoding.EncodeVersion(key, uint64(version)), val)
	}
	table, smallestKey, largestKey, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, si)
	require.NoError(t, err)
	buff := table.Serialize()
	err = cloudStore.Put(context.Background(), conf.DefaultBucketName, name, buff)
	require.NoError(t, err)
	return smallestKey, largestKey
}

func addTable(t *testing.T, lm *LevelManager, tableName string, rangeStart []byte, rangeEnd []byte) {
	addTableWithMinMaxVersion(t, lm, tableName, rangeStart, rangeEnd, 0, 0)
}

func addTableWithMinMaxVersion(t *testing.T, lm *LevelManager, tableName string, rangeStart []byte, rangeEnd []byte,
	minVersion int, maxVersion int) {
	addedTime := uint64(time.Now().UTC().UnixMilli())
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
				AddedTime:  addedTime,
			},
		},
	}
	validateRegBatch(regBatch, lm.GetObjectStore(), conf.DefaultBucketName)
	for {
		ch := make(chan error, 1)
		lm.RegisterL0Tables(regBatch, func(err error) {
			ch <- err
		})
		err := <-ch
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
