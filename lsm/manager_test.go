package lsm

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestAddAndGet_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	l0Pairs := []int{3, 5, 6, 7, 8, 12, //contiguous
		14, 17, 20, 21, 23, 30, //gaps between
		32, 40, 32, 40, 32, 40, //exactly overlapping
		42, 50, 45, 47, //one fully inside other
		51, 54, 52, 56, 54, 60}
	addedTableIDs := addTables(t, levelManager, 0, l0Pairs...) //each overlapping next one

	verifyTablesInLevel(t, levelManager, 0, l0Pairs)

	// Before data shouldn't find anything
	overlapTableIDs := getInRange(t, levelManager, 0, 1)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 0, 3)
	validateTabIds(t, nil, overlapTableIDs)

	// After data shouldn't find anything
	overlapTableIDs = getInRange(t, levelManager, 91, 93)
	validateTabIds(t, nil, overlapTableIDs)

	// Get entire contiguous block with exact range
	overlapTableIDs = getInRange(t, levelManager, 3, 13)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
		NonOverlappingTables{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get entire contiguous block with smaller range
	overlapTableIDs = getInRange(t, levelManager, 4, 10)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
		NonOverlappingTables{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get entire contiguous block with larger range
	overlapTableIDs = getInRange(t, levelManager, 2, 14)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
		NonOverlappingTables{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get exactly one from contiguous block
	overlapTableIDs = getInRange(t, levelManager, 6, 8)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[1]},
	}, overlapTableIDs)

	// Don't return anything from gap
	overlapTableIDs = getInRange(t, levelManager, 18, 20)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 18, 19)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 22, 23)
	validateTabIds(t, nil, overlapTableIDs)

	// Select entire block with gaps
	overlapTableIDs = getInRange(t, levelManager, 14, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[5]},
		NonOverlappingTables{addedTableIDs[4]},
		NonOverlappingTables{addedTableIDs[3]},
	}, overlapTableIDs)

	// Select subset with gaps
	overlapTableIDs = getInRange(t, levelManager, 18, 22)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[4]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 18, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[5]},
		NonOverlappingTables{addedTableIDs[4]},
	}, overlapTableIDs)

	// Exactly overlapping
	overlapTableIDs = getInRange(t, levelManager, 32, 41)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[8]},
		NonOverlappingTables{addedTableIDs[7]},
		NonOverlappingTables{addedTableIDs[6]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 31, 41)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[8]},
		NonOverlappingTables{addedTableIDs[7]},
		NonOverlappingTables{addedTableIDs[6]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 35, 35)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[8]},
		NonOverlappingTables{addedTableIDs[7]},
		NonOverlappingTables{addedTableIDs[6]},
	}, overlapTableIDs)

	// Fully inside
	overlapTableIDs = getInRange(t, levelManager, 42, 48)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[10]},
		NonOverlappingTables{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 41, 49)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[10]},
		NonOverlappingTables{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 45, 46)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[10]},
		NonOverlappingTables{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 41, 43)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 48, 49)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[9]},
	}, overlapTableIDs)

	// Select entire block overlapping next
	overlapTableIDs = getInRange(t, levelManager, 51, 61)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[13]},
		NonOverlappingTables{addedTableIDs[12]},
		NonOverlappingTables{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 53, 58)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[13]},
		NonOverlappingTables{addedTableIDs[12]},
		NonOverlappingTables{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 51, 53)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[12]},
		NonOverlappingTables{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 55, 56)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[13]},
		NonOverlappingTables{addedTableIDs[12]},
	}, overlapTableIDs)

	afterTest(t, levelManager)
}

func TestAddAndGet_L1(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	l1Pairs := []int{3, 5, 6, 8, 9, 11, 14, 15, 20, 30, 35, 50}

	addedTableIDs := addTables(t, levelManager, 1, l1Pairs...)
	verifyTablesInLevel(t, levelManager, 1, l1Pairs)

	// Before data shouldn't find anything
	overlapTableIDs := getInRange(t, levelManager, 0, 1)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 0, 3)
	validateTabIds(t, nil, overlapTableIDs)

	// After data shouldn't find anything
	overlapTableIDs = getInRange(t, levelManager, 51, 56)
	validateTabIds(t, nil, overlapTableIDs)

	// Get entire block with exact range
	overlapTableIDs = getInRange(t, levelManager, 3, 51)
	validateTabIds(t, OverlappingTables{addedTableIDs}, overlapTableIDs)

	// Get subset block with exact range
	overlapTableIDs = getInRange(t, levelManager, 6, 16)
	validateTabIds(t, OverlappingTables{addedTableIDs[1:4]}, overlapTableIDs)

	// Get entire block with larger range
	overlapTableIDs = getInRange(t, levelManager, 0, 1000)
	validateTabIds(t, OverlappingTables{addedTableIDs}, overlapTableIDs)

	// Get single table
	overlapTableIDs = getInRange(t, levelManager, 9, 11)
	validateTabIds(t, OverlappingTables{{addedTableIDs[2]}}, overlapTableIDs)

	afterTest(t, levelManager)
}

func TestAddAndGet_MultipleLevels(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	l0Pairs := []int{51, 54, 52, 56, 54, 60}
	addedTableIDs0 := addTables(t, levelManager, 0, l0Pairs...)
	verifyTablesInLevel(t, levelManager, 0, l0Pairs)

	l1Pairs := []int{48, 49, 52, 54, 55, 58, 59, 63}
	addedTableIDs1 := addTables(t, levelManager, 1, l1Pairs...)
	verifyTablesInLevel(t, levelManager, 1, l1Pairs)

	l2Pairs := []int{22, 31, 38, 48, 50, 52, 53, 65, 68, 70}
	addedTableIDs2 := addTables(t, levelManager, 2, l2Pairs...)
	verifyTablesInLevel(t, levelManager, 2, l2Pairs)

	// Get everything
	overlapTableIDs := getInRange(t, levelManager, 20, 70)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs0[2]},
		NonOverlappingTables{addedTableIDs0[1]},
		NonOverlappingTables{addedTableIDs0[0]},
		addedTableIDs1,
		addedTableIDs2,
	}, overlapTableIDs)

	// Get selection
	overlapTableIDs = getInRange(t, levelManager, 55, 58)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs0[2]},
		NonOverlappingTables{addedTableIDs0[1]},
		addedTableIDs1[2:3],
		addedTableIDs2[3:4],
	}, overlapTableIDs)

	afterTest(t, levelManager)
}

func TestAddAndRemove_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	l0Pairs := []int{2, 4, 5, 7, 14, 17, 20, 21, 23, 30}
	addedTableIDs := addTables(t, levelManager, 0, l0Pairs...)
	verifyTablesInLevel(t, levelManager, 0, l0Pairs)

	overlapTableIDs := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[4]},
		NonOverlappingTables{addedTableIDs[3]},
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
		NonOverlappingTables{addedTableIDs[0]},
	}, overlapTableIDs)

	deregEntry0 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[4][0].ID,
		KeyStart: createKey(2),
		KeyEnd:   createKey(4),
	}
	deregBatch := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{deregEntry0},
	}
	ok, err := levelManager.ApplyChanges(deregBatch, true)
	require.NoError(t, err)
	require.True(t, ok)

	overlapTableIDs2 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[4]},
		NonOverlappingTables{addedTableIDs[3]},
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
	}, overlapTableIDs2)
	remaining := []int{5, 7, 14, 17, 20, 21, 23, 30}
	verifyTablesInLevel(t, levelManager, 0, remaining)

	deregEntry2 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[2][0].ID,
		KeyStart: createKey(14),
		KeyEnd:   createKey(17),
	}

	deregEntry4 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[0][0].ID,
		KeyStart: createKey(23),
		KeyEnd:   createKey(30),
	}

	deregBatch2 := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{deregEntry2, deregEntry4},
	}
	ok, err = levelManager.ApplyChanges(deregBatch2, true)
	require.NoError(t, err)
	require.True(t, ok)

	overlapTableIDs3 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[3]},
		NonOverlappingTables{addedTableIDs[1]},
	}, overlapTableIDs3)

	remaining = []int{5, 7, 20, 21}
	verifyTablesInLevel(t, levelManager, 0, remaining)

	deregEntry1 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[3][0].ID,
		KeyStart: createKey(5),
		KeyEnd:   createKey(7),
	}

	deregEntry3 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[1][0].ID,
		KeyStart: createKey(20),
		KeyEnd:   createKey(21),
	}

	deregBatch3 := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{deregEntry1, deregEntry3},
	}
	ok, err = levelManager.ApplyChanges(deregBatch3, true)
	require.NoError(t, err)
	require.True(t, ok)

	verifyTablesInLevel(t, levelManager, 0, nil)

	afterTest(t, levelManager)
}

func TestAddRemove_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithConfigSetter(t, false, true, func(cfg *Conf) {
		cfg.L0MaxTablesBeforeBlocking = 100
	})
	defer tearDown(t)

	mr := levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(0), mr.version)
	require.Equal(t, 0, len(mr.levelEntries))

	// Add some table entries in level 0

	tableIDs1 := addTables(t, levelManager, 0,
		12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(1), mr.version)
	require.Equal(t, 1, len(mr.levelEntries))

	levEntry := mr.levelEntries[0]
	require.NotNil(t, levEntry)
	require.Equal(t, 6, len(levEntry.tableEntries))
	require.Equal(t, createKey(1), levEntry.rangeStart)
	require.Equal(t, createKey(30), levEntry.rangeEnd)

	// Add some more - now there should be 10

	tableIDs2 := addTables(t, levelManager, 0,
		11, 13, 3, 9, 0, 35, 7, 12)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(2), mr.version)
	require.Equal(t, 1, len(mr.levelEntries))

	levEntry = mr.levelEntries[0]
	require.NotNil(t, levEntry)
	require.Equal(t, 10, len(levEntry.tableEntries))
	require.Equal(t, createKey(0), levEntry.rangeStart)
	require.Equal(t, createKey(35), levEntry.rangeEnd)

	// Add some more

	tableIDs3 := addTables(t, levelManager, 0,
		15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(3), mr.version)

	levEntry = mr.levelEntries[0]
	require.NotNil(t, levEntry)
	require.Equal(t, 15, len(levEntry.tableEntries))
	require.Equal(t, createKey(0), levEntry.rangeStart)
	require.Equal(t, createKey(89), levEntry.rangeEnd)

	// Now delete some
	removeTables(t, levelManager, 0, tableIDs3, 15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(4), mr.version)

	levEntry = mr.levelEntries[0]
	require.NotNil(t, levEntry)
	require.Equal(t, 10, len(levEntry.tableEntries))
	require.Equal(t, createKey(0), levEntry.rangeStart)
	require.Equal(t, createKey(35), levEntry.rangeEnd)

	removeTables(t, levelManager, 0, tableIDs2, 11, 13, 3, 9, 0, 35, 7, 12)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(5), mr.version)

	levEntry = mr.levelEntries[0]
	require.NotNil(t, levEntry)
	require.Equal(t, 6, len(levEntry.tableEntries))
	require.Equal(t, createKey(1), levEntry.rangeStart)
	require.Equal(t, createKey(30), levEntry.rangeEnd)

	removeTables(t, levelManager, 0, tableIDs1, 12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	// Should be all gone
	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(6), mr.version)
	require.Equal(t, 0, len(mr.levelEntries[0].tableEntries))

	afterTest(t, levelManager)
}

func TestAddAndRemoveSameBatch(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	tableIDs := addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)

	newTableID1, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	newTableID2, err := uuid.New().MarshalBinary()
	require.NoError(t, err)

	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{{
			Level:    1,
			TableID:  newTableID1,
			KeyStart: createKey(15),
			KeyEnd:   createKey(19),
		}, {
			Level:    1,
			TableID:  newTableID2,
			KeyStart: createKey(36),
			KeyEnd:   createKey(39),
		}},
		DeRegistrations: []RegistrationEntry{{
			Level:    1,
			TableID:  tableIDs[1].ID,
			KeyStart: createKey(6),
			KeyEnd:   createKey(7),
		}},
	}
	ok, err := levelManager.ApplyChanges(regBatch, true)
	require.NoError(t, err)
	require.True(t, ok)

	oTabIDs, err := levelManager.QueryTablesInRange(createKey(3), createKey(1000))
	require.NoError(t, err)
	require.Equal(t, 1, len(oTabIDs))
	noTabIDs := oTabIDs[0]
	require.Equal(t, 4, len(noTabIDs))

	require.Equal(t, tableIDs[0], noTabIDs[0])
	require.Equal(t, tableIDs[2], noTabIDs[1])
	require.Equal(t, sst.SSTableID(newTableID1), noTabIDs[2].ID)
	require.Equal(t, sst.SSTableID(newTableID2), noTabIDs[3].ID)

	afterTest(t, levelManager)
}

func TestNilRangeStartAndEnd(t *testing.T) {
	// Range start of nil means start at the beginning
	testNilRangeStartAndEnd(t, nil, createKey(1000))
	// Range end of nil means right to the end
	testNilRangeStartAndEnd(t, createKey(0), nil)
	// Full range
	testNilRangeStartAndEnd(t, nil, nil)
}

func testNilRangeStartAndEnd(t *testing.T, rangeStart []byte, rangeEnd []byte) {
	t.Helper()
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)

	oTabIDs, err := levelManager.QueryTablesInRange(rangeStart, rangeEnd)
	require.NoError(t, err)
	require.Equal(t, 1, len(oTabIDs))
	noTabIDs := oTabIDs[0]
	require.Equal(t, 3, len(noTabIDs))
}

func TestAddRemoveL1_OrderedKeys(t *testing.T) {
	testAddRemoveNonOverlapping(t, true, 1, 0, 1)
	testAddRemoveNonOverlapping(t, true, 1, 1, 1)
	testAddRemoveNonOverlapping(t, true, 1, 0, 3)
	testAddRemoveNonOverlapping(t, true, 3, 0, 1)
}

func TestAddRemoveL1_RandomKeys(t *testing.T) {
	testAddRemoveNonOverlapping(t, false, 1, 0, 1)
	testAddRemoveNonOverlapping(t, false, 1, 1, 1)
	testAddRemoveNonOverlapping(t, false, 1, 0, 3)
	testAddRemoveNonOverlapping(t, false, 3, 0, 1)
}

func testAddRemoveNonOverlapping(t *testing.T, ordered bool, level int, rangeGap int, batchSize int) {
	t.Helper()
	numEntries := 1000
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	entries := prepareEntries(numEntries, 2, rangeGap)
	if !ordered {
		entries = shuffleEntries(entries)
	}

	var regBatch *RegistrationBatch
	entryNum := 0
	batchNum := 0
	for entryNum < len(entries) {
		if regBatch == nil {
			regBatch = &RegistrationBatch{}
		}
		ent := entries[entryNum]
		tableID, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		regBatch.Registrations = append(regBatch.Registrations, RegistrationEntry{
			Level:    level,
			TableID:  tableID,
			KeyStart: ent.keyStart,
			KeyEnd:   ent.keyEnd,
		})
		if len(regBatch.Registrations) == batchSize || entryNum == len(entries)-1 {
			ok, err := levelManager.ApplyChanges(*regBatch, true)
			require.NoError(t, err)
			require.True(t, ok)
			// Get the table ids to make sure they were added ok
			for i := 0; i < len(regBatch.Registrations); i++ {
				oIDs, err := levelManager.QueryTablesInRange(regBatch.Registrations[i].KeyStart,
					common.IncBigEndianBytes(regBatch.Registrations[i].KeyEnd))
				require.NoError(t, err)
				require.Equal(t, 1, len(oIDs))
				nIDs := oIDs[0]
				require.Equal(t, 1, len(nIDs))
				require.Equal(t, regBatch.Registrations[i].TableID, nIDs[0].ID)
				// We store the sstableid on the entry so we can use it later in the deregistration
				entries[entryNum+1-len(regBatch.Registrations)+i].tableID = nIDs[0].ID
			}
			checkState(t, levelManager, batchNum)
			batchNum++
			regBatch = nil
		}
		entryNum++
	}
	regBatch = nil

	// Now shuffle the entries and remove in batches
	// verify removed entries can't be getted
	if !ordered {
		entries = shuffleEntries(entries)
	}
	entryNum = 0
	for entryNum < len(entries) {
		if regBatch == nil {
			regBatch = &RegistrationBatch{}
		}
		ent := entries[entryNum]

		regBatch.DeRegistrations = append(regBatch.DeRegistrations, RegistrationEntry{
			Level:    level,
			TableID:  ent.tableID,
			KeyStart: ent.keyStart,
			KeyEnd:   ent.keyEnd,
		})
		if len(regBatch.DeRegistrations) == batchSize || entryNum == len(entries)-1 {
			ok, err := levelManager.ApplyChanges(*regBatch, true)
			require.NoError(t, err)
			require.True(t, ok)
			// Get the table ids to make sure they were removed ok
			for i := 0; i < len(regBatch.DeRegistrations); i++ {
				oIDs, err := levelManager.QueryTablesInRange(regBatch.DeRegistrations[i].KeyStart,
					common.IncBigEndianBytes(regBatch.DeRegistrations[i].KeyEnd))
				require.NoError(t, err)
				require.Equal(t, 0, len(oIDs))
			}
			checkState(t, levelManager, batchNum)
			batchNum++
			regBatch = nil
		}
		entryNum++
	}
}

func shuffleEntries(entries []entry) []entry {
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
	return entries
}

func checkState(t *testing.T, levelManager *Manager, batchNum int) {
	t.Helper()
	err := levelManager.Validate(false)
	require.NoError(t, err)
	mr := levelManager.getMasterRecord()
	require.NotNil(t, mr)
	require.Equal(t, batchNum+1, int(mr.version))
}

func prepareEntries(numEntries int, rangeSize int, rangeGap int) []entry {
	entries := make([]entry, 0, numEntries)
	start := 0
	for i := 0; i < numEntries; i++ {
		ks := createKey(start)
		ke := createKey(start + rangeSize - 1)
		start += rangeSize + rangeGap
		entries = append(entries, entry{
			keyStart: ks,
			keyEnd:   ke,
		})
	}
	return entries
}

type entry struct {
	keyStart []byte
	keyEnd   []byte
	tableID  sst.SSTableID
}

func afterTest(t *testing.T, levelManager *Manager) {
	t.Helper()
	err := levelManager.Validate(false)
	require.NoError(t, err)
}

func validateTabIds(t *testing.T, expectedTabIDs OverlappingTables, actualTabIDs OverlappingTables) {
	t.Helper()
	require.Equal(t, expectedTabIDs, actualTabIDs)
}

func getInRange(t *testing.T, levelManager *Manager, ks int, ke int) OverlappingTables {
	t.Helper()
	keyStart := createKey(ks)
	keyEnd := createKey(ke)
	overlapTabIDs, err := levelManager.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)
	return overlapTabIDs
}

func setupLevelManager(t *testing.T) (*Manager, func(t *testing.T)) {
	t.Helper()
	return setupLevelManagerWithConfigSetter(t, false, true, func(cfg *Conf) {})
}

func setupLevelManagerWithConfigSetter(t *testing.T, enableCompaction bool, validate bool,
	configSetter func(cfg *Conf)) (*Manager, func(t *testing.T)) {
	t.Helper()
	cfg := NewConf()
	configSetter(&cfg)
	cloudStore := &dev.InMemStore{}
	lm := NewManager(cloudStore, func() {}, enableCompaction, validate, cfg)
	mr := NewMasterRecord(common.MetadataFormatV1)
	err := lm.Start(mr.Serialize(nil))
	require.NoError(t, err)
	return lm, func(t *testing.T) {
		err := lm.Stop()
		require.NoError(t, err)
		err = cloudStore.Stop()
		require.NoError(t, err)
	}
}

func createKey(i int) []byte {
	// First 24 bytes is the [partition_hash, slab_id] - we just add a constant prefix
	prefix := make([]byte, 0, 24)
	prefix = append(prefix, []byte("xxxxxxxxxxxxxxxx")...) // partition hash
	prefix = encoding.AppendUint64ToBufferBE(prefix, 1234) // slab id
	prefix = append(prefix, []byte(fmt.Sprintf("key-%010d", i))...)
	return encoding.EncodeVersion(prefix, 0)
}

func removeTables(t *testing.T, levelManager *Manager, level int, tabIDs []QueryTableInfo, pairs ...int) {
	t.Helper()
	var regEntries []RegistrationEntry
	j := 0
	for i := 0; i < len(pairs); i++ {
		ks := pairs[i]
		i++
		ke := pairs[i]
		firstKey := createKey(ks)
		lastKey := createKey(ke)
		tabID := tabIDs[j]
		j++
		regEntry := RegistrationEntry{
			Level:    level,
			TableID:  tabID.ID,
			KeyStart: firstKey,
			KeyEnd:   lastKey,
		}
		regEntries = append(regEntries, regEntry)
	}
	regBatch := RegistrationBatch{
		DeRegistrations: regEntries,
	}
	ok, err := levelManager.ApplyChanges(regBatch, true)
	require.NoError(t, err)
	require.True(t, ok)
}

func addTables(t *testing.T, levelManager *Manager, level int, pairs ...int) []QueryTableInfo {
	t.Helper()
	addRegEntries, addTableIDs := createRegistrationEntries(t, level, pairs...)
	regBatch := RegistrationBatch{
		Registrations: addRegEntries,
	}
	ok, err := levelManager.ApplyChanges(regBatch, true)
	require.NoError(t, err)
	require.True(t, ok)
	return addTableIDs
}

func createRegistrationEntries(t *testing.T, level int, pairs ...int) ([]RegistrationEntry, []QueryTableInfo) {
	t.Helper()
	var regEntries []RegistrationEntry
	var tableIDs []QueryTableInfo
	for i := 0; i < len(pairs); i++ {
		ks := pairs[i]
		i++
		ke := pairs[i]
		firstKey := createKey(ks)
		lastKey := createKey(ke)
		tabID, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		regEntry := RegistrationEntry{
			Level:    level,
			TableID:  tabID,
			KeyStart: firstKey,
			KeyEnd:   lastKey,
		}
		regEntries = append(regEntries, regEntry)
		tableIDs = append(tableIDs, QueryTableInfo{ID: tabID})
	}
	return regEntries, tableIDs
}

func TestRegisterAndGetSlabRetentions(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	allRetentions := map[int]time.Duration{}
	for i := 0; i < 10; i++ {
		retention := time.Duration(i) * time.Minute
		err := levelManager.RegisterSlabRetention(i, retention)
		require.NoError(t, err)
		allRetentions[i] = retention
	}

	for slabID, retention := range allRetentions {
		ret, err := levelManager.GetSlabRetention(slabID)
		require.NoError(t, err)
		require.Equal(t, retention, ret)
	}

	err := levelManager.UnregisterSlabRetention(1)
	require.NoError(t, err)
	ret, err := levelManager.GetSlabRetention(1)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), ret)
}

func TestStats(t *testing.T) {
	lm, tearDown := setupLevelManager(t)
	defer tearDown(t)

	l0NumTables := 5
	l0TotTableSize := 0
	l0TotEntrySize := 0
	var registrations []RegistrationEntry
	for i := 0; i < l0NumTables; i++ {
		numEntries := rand.Intn(10000)
		tableSize := rand.Intn(10000000)
		reg := RegistrationEntry{
			Level:      0,
			TableID:    sst.SSTableID(uuid.New().String()),
			MinVersion: 100,
			MaxVersion: 200,
			KeyStart:   []byte(fmt.Sprintf("key-%05d", i)),
			KeyEnd:     []byte(fmt.Sprintf("key-%05d", i+1)),
			NumEntries: uint64(numEntries),
			TableSize:  uint64(tableSize),
		}
		l0TotEntrySize += numEntries
		l0TotTableSize += tableSize
		registrations = append(registrations, reg)
	}

	regBatch0 := RegistrationBatch{
		Registrations: registrations,
	}
	ok, err := lm.ApplyChanges(regBatch0, true)
	require.NoError(t, err)
	require.True(t, ok)

	stats := lm.GetStats()
	require.Equal(t, l0NumTables, stats.TablesIn)
	require.Equal(t, l0TotTableSize, stats.BytesIn)
	require.Equal(t, l0TotEntrySize, stats.EntriesIn)

	require.Equal(t, l0NumTables, stats.TotTables)
	require.Equal(t, l0TotTableSize, stats.TotBytes)
	require.Equal(t, l0TotEntrySize, stats.TotEntries)

	require.Equal(t, 1, len(stats.LevelStats))
	levStats0 := stats.LevelStats[0]
	require.Equal(t, l0NumTables, levStats0.Tables)
	require.Equal(t, l0TotTableSize, levStats0.Bytes)
	require.Equal(t, l0TotEntrySize, levStats0.Entries)

	l1NumTables := 10
	l1TotTableSize := 0
	l1TotEntrySize := 0
	registrations = nil
	for i := 0; i < l1NumTables; i++ {
		numEntries := rand.Intn(10000)
		tableSize := rand.Intn(10000000)
		reg := RegistrationEntry{
			Level:      1,
			TableID:    sst.SSTableID(uuid.New().String()),
			MinVersion: 100,
			MaxVersion: 200,
			KeyStart:   []byte(fmt.Sprintf("key-%05d", i)),
			KeyEnd:     []byte(fmt.Sprintf("key-%05d", i+1)),
			NumEntries: uint64(numEntries),
			TableSize:  uint64(tableSize),
		}
		l1TotEntrySize += numEntries
		l1TotTableSize += tableSize
		registrations = append(registrations, reg)
	}

	regBatch1 := RegistrationBatch{
		Registrations: registrations,
	}
	ok, err = lm.ApplyChanges(regBatch1, true)
	require.NoError(t, err)
	require.True(t, ok)

	stats = lm.GetStats()
	require.Equal(t, l0NumTables, stats.TablesIn)
	require.Equal(t, l0TotTableSize, stats.BytesIn)
	require.Equal(t, l0TotEntrySize, stats.EntriesIn)

	require.Equal(t, l0NumTables+l1NumTables, stats.TotTables)
	require.Equal(t, l0TotTableSize+l1TotTableSize, stats.TotBytes)
	require.Equal(t, l0TotEntrySize+l1TotEntrySize, stats.TotEntries)

	require.Equal(t, 2, len(stats.LevelStats))
	levStats0 = stats.LevelStats[0]
	require.Equal(t, l0NumTables, levStats0.Tables)
	require.Equal(t, l0TotTableSize, levStats0.Bytes)
	require.Equal(t, l0TotEntrySize, levStats0.Entries)

	levStats1 := stats.LevelStats[1]
	require.Equal(t, l1NumTables, levStats1.Tables)
	require.Equal(t, l1TotTableSize, levStats1.Bytes)
	require.Equal(t, l1TotEntrySize, levStats1.Entries)

	l2NumTables := 50
	l2TotTableSize := 0
	l2TotEntrySize := 0
	registrations = nil
	for i := 0; i < l2NumTables; i++ {
		numEntries := rand.Intn(10000)
		tableSize := rand.Intn(10000000)
		reg := RegistrationEntry{
			Level:      2,
			TableID:    sst.SSTableID(uuid.New().String()),
			MinVersion: 100,
			MaxVersion: 200,
			KeyStart:   []byte(fmt.Sprintf("key-%05d", i)),
			KeyEnd:     []byte(fmt.Sprintf("key-%05d", i+1)),
			NumEntries: uint64(numEntries),
			TableSize:  uint64(tableSize),
		}
		l2TotEntrySize += numEntries
		l2TotTableSize += tableSize
		registrations = append(registrations, reg)
	}

	regBatch2 := RegistrationBatch{
		Registrations: registrations,
	}
	ok, err = lm.ApplyChanges(regBatch2, true)
	require.NoError(t, err)
	require.True(t, ok)

	stats = lm.GetStats()
	require.Equal(t, l0NumTables, stats.TablesIn)
	require.Equal(t, l0TotTableSize, stats.BytesIn)
	require.Equal(t, l0TotEntrySize, stats.EntriesIn)

	require.Equal(t, l0NumTables+l1NumTables+l2NumTables, stats.TotTables)
	require.Equal(t, l0TotTableSize+l1TotTableSize+l2TotTableSize, stats.TotBytes)
	require.Equal(t, l0TotEntrySize+l1TotEntrySize+l2TotEntrySize, stats.TotEntries)

	require.Equal(t, 3, len(stats.LevelStats))
	levStats0 = stats.LevelStats[0]
	require.Equal(t, l0NumTables, levStats0.Tables)
	require.Equal(t, l0TotTableSize, levStats0.Bytes)
	require.Equal(t, l0TotEntrySize, levStats0.Entries)

	levStats1 = stats.LevelStats[1]
	require.Equal(t, l1NumTables, levStats1.Tables)
	require.Equal(t, l1TotTableSize, levStats1.Bytes)
	require.Equal(t, l1TotEntrySize, levStats1.Entries)

	levStats2 := stats.LevelStats[2]
	require.Equal(t, l2NumTables, levStats2.Tables)
	require.Equal(t, l2TotTableSize, levStats2.Bytes)
	require.Equal(t, l2TotEntrySize, levStats2.Entries)

	deregBatch0 := regBatch0.Registrations[0]
	deregBatch1 := regBatch1.Registrations[0]
	deregBatch2 := regBatch2.Registrations[0]

	deregBatch := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{
			deregBatch0,
			deregBatch1,
			deregBatch2,
		},
	}
	ok, err = lm.ApplyChanges(deregBatch, true)
	require.NoError(t, err)
	require.True(t, ok)

	stats = lm.GetStats()
	require.Equal(t, l0NumTables, stats.TablesIn)
	require.Equal(t, l0TotTableSize, stats.BytesIn)
	require.Equal(t, l0TotEntrySize, stats.EntriesIn)

	require.Equal(t, l0NumTables+l1NumTables+l2NumTables-3, stats.TotTables)
	require.Equal(t, l0TotTableSize+l1TotTableSize+l2TotTableSize-int(deregBatch0.TableSize)-int(deregBatch1.TableSize)-int(deregBatch2.TableSize), stats.TotBytes)
	require.Equal(t, l0TotEntrySize+l1TotEntrySize+l2TotEntrySize-int(deregBatch0.NumEntries)-int(deregBatch1.NumEntries)-int(deregBatch2.NumEntries), stats.TotEntries)

	require.Equal(t, 3, len(stats.LevelStats))
	levStats0 = stats.LevelStats[0]
	require.Equal(t, l0NumTables-1, levStats0.Tables)
	require.Equal(t, l0TotTableSize-int(deregBatch0.TableSize), levStats0.Bytes)
	require.Equal(t, l0TotEntrySize-int(deregBatch0.NumEntries), levStats0.Entries)

	levStats1 = stats.LevelStats[1]
	require.Equal(t, l1NumTables-1, levStats1.Tables)
	require.Equal(t, l1TotTableSize-int(deregBatch1.TableSize), levStats1.Bytes)
	require.Equal(t, l1TotEntrySize-int(deregBatch1.NumEntries), levStats1.Entries)

	levStats2 = stats.LevelStats[2]
	require.Equal(t, l2NumTables-1, levStats2.Tables)
	require.Equal(t, l2TotTableSize-int(deregBatch2.TableSize), levStats2.Bytes)
	require.Equal(t, l2TotEntrySize-int(deregBatch2.NumEntries), levStats2.Entries)
}

func TestRegisterDeadVersionRanges(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	// Add some entries in different levels
	regEntry01 := regTableInLevel(t, levelManager, 0, 0, 9, 310, 319)
	regEntry02 := regTableInLevel(t, levelManager, 0, 10, 19, 320, 329)
	regEntry03 := regTableInLevel(t, levelManager, 0, 20, 29, 330, 339)

	regEntry11 := regTableInLevel(t, levelManager, 1, 0, 9, 210, 219)
	regEntry12 := regTableInLevel(t, levelManager, 1, 10, 19, 220, 229)
	regEntry13 := regTableInLevel(t, levelManager, 1, 20, 29, 230, 239)

	regEntry21 := regTableInLevel(t, levelManager, 2, 0, 9, 110, 119)
	regEntry22 := regTableInLevel(t, levelManager, 2, 10, 19, 120, 129)
	regEntry23 := regTableInLevel(t, levelManager, 2, 20, 29, 130, 139)

	tables := getInRange(t, levelManager, 0, 30)
	require.Equal(t, 5, len(tables))
	// Make sure no dead version ranges
	for _, infos := range tables {
		for _, table := range infos {
			require.Nil(t, table.DeadVersions)
		}
	}

	// Ranges that overlap one entry
	rng1 := VersionRange{
		VersionStart: 220,
		VersionEnd:   225,
	}
	expected := []NonOverlappingTables{
		{
			{ID: regEntry03.TableID},
		},
		{
			{ID: regEntry02.TableID},
		},
		{
			{ID: regEntry01.TableID},
		},
		{
			{ID: regEntry11.TableID},
			{ID: regEntry12.TableID, DeadVersions: []VersionRange{rng1}},
			{ID: regEntry13.TableID},
		},
		{
			{ID: regEntry21.TableID},
			{ID: regEntry22.TableID},
			{ID: regEntry23.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng1, expected)

	rng2 := VersionRange{
		VersionStart: 223,
		VersionEnd:   227,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry03.TableID},
		},
		{
			{ID: regEntry02.TableID},
		},
		{
			{ID: regEntry01.TableID},
		},
		{
			{ID: regEntry11.TableID},
			{ID: regEntry12.TableID, DeadVersions: []VersionRange{rng1, rng2}},
			{ID: regEntry13.TableID},
		},
		{
			{ID: regEntry21.TableID},
			{ID: regEntry22.TableID},
			{ID: regEntry23.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng2, expected)

	rng3 := VersionRange{
		VersionStart: 224,
		VersionEnd:   229,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry03.TableID},
		},
		{
			{ID: regEntry02.TableID},
		},
		{
			{ID: regEntry01.TableID},
		},
		{
			{ID: regEntry11.TableID},
			{ID: regEntry12.TableID, DeadVersions: []VersionRange{rng1, rng2, rng3}},
			{ID: regEntry13.TableID},
		},
		{
			{ID: regEntry21.TableID},
			{ID: regEntry22.TableID},
			{ID: regEntry23.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng3, expected)

	rng4 := VersionRange{
		VersionStart: 125,
		VersionEnd:   325,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry03.TableID},
		},
		{
			{ID: regEntry02.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry01.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry11.TableID, DeadVersions: []VersionRange{rng4}},
			{ID: regEntry12.TableID, DeadVersions: []VersionRange{rng1, rng2, rng3, rng4}},
			{ID: regEntry13.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry21.TableID},
			{ID: regEntry22.TableID, DeadVersions: []VersionRange{rng4}},
			{ID: regEntry23.TableID, DeadVersions: []VersionRange{rng4}},
		},
	}
	validateDeadVersions(t, levelManager, rng4, expected)

	// No matches

	rng5 := VersionRange{
		VersionStart: 0,
		VersionEnd:   109,
	}
	validateDeadVersions(t, levelManager, rng5, expected)

	rng6 := VersionRange{
		VersionStart: 340,
		VersionEnd:   350,
	}
	validateDeadVersions(t, levelManager, rng6, expected)

	afterTest(t, levelManager)
}

func validateDeadVersions(t *testing.T, lm *Manager, rng VersionRange, expected []NonOverlappingTables) {
	err := lm.RegisterDeadVersionRange(rng)
	require.NoError(t, err)
	tables := getInRange(t, lm, 0, 30)
	require.Equal(t, OverlappingTables(expected), tables)
}

func regTableInLevel(t *testing.T, lm *Manager, level int, keyStart int, keyEnd int, minVersion int, maxVersion int) *RegistrationEntry {
	ks := createKey(keyStart)
	ke := createKey(keyEnd)
	return regTableInLevelWithStringKey(t, lm, level, string(ks), string(ke), minVersion, maxVersion)
}

func regTableInLevelWithStringKey(t *testing.T, lm *Manager, level int, keyStart string, keyEnd string, minVersion int, maxVersion int) *RegistrationEntry {
	tabID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regEntry := RegistrationEntry{
		Level:      level,
		TableID:    tabID,
		KeyStart:   []byte(keyStart),
		KeyEnd:     []byte(keyEnd),
		MinVersion: uint64(minVersion),
		MaxVersion: uint64(maxVersion),
	}
	regBatch := RegistrationBatch{Registrations: []RegistrationEntry{regEntry}}
	ok, err := lm.ApplyChanges(regBatch, true)
	require.NoError(t, err)
	require.True(t, ok)
	return &regEntry
}

func verifyTablesInLevel(t *testing.T, lm *Manager, level int, expectedTablePairs []int) {
	levEntry := lm.getLevelEntry(level)
	require.Equal(t, len(expectedTablePairs), 2*len(levEntry.tableEntries))
	pos := 0
	for i := 0; i < len(expectedTablePairs); i += 2 {
		start := expectedTablePairs[i]
		end := expectedTablePairs[i+1]
		expectedStart := createKey(start)
		expectedEnd := createKey(end)
		lte := levEntry.tableEntries[pos]
		te := getTableEntry(lm, lte, levEntry)
		require.Equal(t, expectedStart, te.RangeStart)
		require.Equal(t, expectedEnd, te.RangeEnd)
		pos++
	}
}

func TestGetTablesForHighestKeyWithPrefixInOrderedLevel(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	regTableInLevelWithStringKey(t, levelManager, 1, "part001-000", "part001-100", 0, 0)
	entry2 := regTableInLevelWithStringKey(t, levelManager, 1, "part001-101", "part002-030", 0, 0)
	entry3 := regTableInLevelWithStringKey(t, levelManager, 1, "part003-010", "part003-100", 0, 0)
	entry4 := regTableInLevelWithStringKey(t, levelManager, 1, "part004-000", "part006-075", 0, 0)
	regTableInLevelWithStringKey(t, levelManager, 1, "part006-076", "part006-080", 0, 0)
	regTableInLevelWithStringKey(t, levelManager, 1, "part006-081", "part006-099", 0, 0)
	entry7 := regTableInLevelWithStringKey(t, levelManager, 1, "part006-100", "part006-100", 0, 0)
	entry8 := regTableInLevelWithStringKey(t, levelManager, 1, "part007-000", "part007-000", 0, 0)
	entry9 := regTableInLevelWithStringKey(t, levelManager, 1, "part008-000", "part008-100", 0, 0)

	err := levelManager.Validate(false)
	require.NoError(t, err)

	// Before first entry
	tableIDs, err := levelManager.GetTablesForHighestKeyWithPrefix([]byte("part000"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tableIDs))

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part001"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry2.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part002"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry2.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part003"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry3.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part004"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry4.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part005"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry4.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part006"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry7.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part007"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry8.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part008"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry9.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part009"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tableIDs))
}

func TestGetTablesForHighestKeyWithPrefixInLevel0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	entry1 := regTableInLevelWithStringKey(t, levelManager, 0, "part001-000", "part001-100", 0, 0)
	entry2 := regTableInLevelWithStringKey(t, levelManager, 0, "part001-050", "part002-030", 0, 0)
	entry3 := regTableInLevelWithStringKey(t, levelManager, 0, "part001-010", "part004-100", 0, 0)
	entry4 := regTableInLevelWithStringKey(t, levelManager, 0, "part005-000", "part005-000", 0, 0)

	tableIDs, err := levelManager.GetTablesForHighestKeyWithPrefix([]byte("part000"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tableIDs))

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part001"))
	require.NoError(t, err)
	require.Equal(t, 3, len(tableIDs))
	require.Equal(t, entry3.TableID, tableIDs[0])
	require.Equal(t, entry2.TableID, tableIDs[1])
	require.Equal(t, entry1.TableID, tableIDs[2])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part002"))
	require.NoError(t, err)
	require.Equal(t, 2, len(tableIDs))
	require.Equal(t, entry3.TableID, tableIDs[0])
	require.Equal(t, entry2.TableID, tableIDs[1])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part003"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry3.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part004"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry3.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part005"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tableIDs))
	require.Equal(t, entry4.TableID, tableIDs[0])

	tableIDs, err = levelManager.GetTablesForHighestKeyWithPrefix([]byte("part006"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tableIDs))
}

func TestGetTablesForHighestKeyWithPrefixInOrderedLevelFuzz(t *testing.T) {
	for i := 0; i < 100; i++ {
		testGetTablesForHighestKeyWithPrefixInOrderedLevelFuzz(t)
	}
}

func testGetTablesForHighestKeyWithPrefixInOrderedLevelFuzz(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	// We maintain a slice containing an entry for each table registered, the entry is a map containing the last prefixes
	// in that table
	latestKeysForPrefixForTables := map[string]map[string]struct{}{}
	keysPerTable := 100
	numPartitions := 100
	keyCount := 0
	keyStart := ""

	lastPrefixes := map[string]struct{}{}
	for i := 0; i < numPartitions; i++ {
		numKeys := 1 + rand.Intn(100)
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("part-%04d-%04d", i, j)
			if keyStart == "" {
				keyStart = key
			}
			if j == numKeys-1 {
				prefix := key[:9]
				lastPrefixes[prefix] = struct{}{}
			}
			keyCount++
			if keyCount == keysPerTable || (i == numPartitions-1 && j == numKeys-1) {
				keyEnd := fmt.Sprintf("part-%04d-%04d", i, j)
				e := regTableInLevelWithStringKey(t, levelManager, 1, keyStart, keyEnd, 0, 0)
				keyStart = ""
				keyCount = 0
				latestKeysForPrefixForTables[string(e.TableID)] = lastPrefixes
				lastPrefixes = map[string]struct{}{}
			}
		}
	}

	for i := 0; i < numPartitions; i++ {
		prefix := []byte(fmt.Sprintf("part-%04d", i))
		tableIDs, err := levelManager.GetTablesForHighestKeyWithPrefix(prefix)
		require.NoError(t, err)
		require.Equal(t, 1, len(tableIDs))
		lp, ok := latestKeysForPrefixForTables[string(tableIDs[0])]
		require.True(t, ok)
		_, ok = lp[string(prefix)]
		require.True(t, ok)
	}
}
