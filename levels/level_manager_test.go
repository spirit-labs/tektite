package levels

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestAddAndGet_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	addedTableIDs := addTables(t, levelManager, 0,
		3, 5, 6, 7, 8, 12, //contiguous
		14, 17, 20, 21, 23, 30, //gaps between
		32, 40, 32, 40, 32, 40, //exactly overlapping
		42, 50, 45, 47, //one fully inside other
		51, 54, 52, 56, 54, 60) //each overlapping next one

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

	addedTableIDs := addTables(t, levelManager, 1,
		3, 5, 6, 8, 9, 11, 14, 15, 20, 30, 35, 50)

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

	addedTableIDs0 := addTables(t, levelManager, 0,
		51, 54, 52, 56, 54, 60)

	addedTableIDs1 := addTables(t, levelManager, 1,
		48, 49, 52, 54, 55, 58, 59, 63)

	addedTableIDs2 := addTables(t, levelManager, 2,
		22, 31, 38, 48, 50, 52, 53, 65, 68, 70)

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

func TestAddAndGetAll_MultipleSegments(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)

	numEntries := 1000
	var pairs []int
	ks := 0
	for i := 0; i < numEntries; i++ {
		pairs = append(pairs, ks, ks+1)
		ks += 3
	}
	addedTableIDs := addTables(t, levelManager, 1, pairs...)

	mr := levelManager.getMasterRecord()
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1].segmentEntries))

	// Now get them all, spanning multiple segments
	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(addedTableIDs), ids)
}

func TestAddAndGetMost_MultipleSegments(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)

	numEntries := 1000
	var pairs []int
	ks := 0
	for i := 0; i < numEntries; i++ {
		pairs = append(pairs, ks, ks+1)
		ks += 2
	}
	addedTableIDs := addTables(t, levelManager, 1, pairs...)

	mr := levelManager.getMasterRecord()
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1].segmentEntries))

	// Now get most of them, spanning multiple segments
	oids, err := levelManager.QueryTablesInRange(createKey(2), createKey(2*(numEntries-2)))
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	expected := addedTableIDs[1 : len(addedTableIDs)-2]
	require.Equal(t, NonOverlappingTables(expected), ids)
}

func TestAddAndRemove_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	addedTableIDs := addTables(t, levelManager, 0,
		2, 4, 5, 7, 14, 17, 20, 21, 23, 30)

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
	err := levelManager.ApplyChangesNoCheck(deregBatch)
	require.NoError(t, err)

	overlapTableIDs2 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[4]},
		NonOverlappingTables{addedTableIDs[3]},
		NonOverlappingTables{addedTableIDs[2]},
		NonOverlappingTables{addedTableIDs[1]},
	}, overlapTableIDs2)

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
	err = levelManager.ApplyChangesNoCheck(deregBatch2)
	require.NoError(t, err)

	overlapTableIDs3 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTables{
		NonOverlappingTables{addedTableIDs[3]},
		NonOverlappingTables{addedTableIDs[1]},
	}, overlapTableIDs3)

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
	err = levelManager.ApplyChangesNoCheck(deregBatch3)
	require.NoError(t, err)

	afterTest(t, levelManager)
}

func TestAddRemove_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 10)
	defer tearDown(t)

	mr := levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(0), mr.version)
	require.Equal(t, 0, len(mr.levelSegmentEntries))

	// Add some table entries in level 0

	tableIDs1 := addTables(t, levelManager, 0,
		12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(1), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries := mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries.segmentEntries))
	segEntry := segEntries.segmentEntries[0]
	require.Equal(t, createKey(1), segEntry.rangeStart)
	require.Equal(t, createKey(30), segEntry.rangeEnd)

	// Add some more - now there should be 10

	tableIDs2 := addTables(t, levelManager, 0,
		11, 13, 3, 9, 0, 35, 7, 12)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(2), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries.segmentEntries))
	segEntry = segEntries.segmentEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(35), segEntry.rangeEnd)

	// Add some more, should still be one segment as L0 only ever has one segment

	tableIDs3 := addTables(t, levelManager, 0,
		15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(3), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries.segmentEntries))
	segEntry = segEntries.segmentEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(89), segEntry.rangeEnd)

	// Now delete some

	removeTables(t, levelManager, 0, tableIDs3, 15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(4), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries.segmentEntries))
	segEntry = segEntries.segmentEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(35), segEntry.rangeEnd)

	removeTables(t, levelManager, 0, tableIDs2, 11, 13, 3, 9, 0, 35, 7, 12)

	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(5), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries.segmentEntries))
	segEntry = segEntries.segmentEntries[0]
	require.Equal(t, createKey(1), segEntry.rangeStart)
	require.Equal(t, createKey(30), segEntry.rangeEnd)

	removeTables(t, levelManager, 0, tableIDs1, 12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	// Should be all gone
	mr = levelManager.getMasterRecord()
	require.Equal(t, common.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(6), mr.version)
	require.Equal(t, 0, len(mr.levelSegmentEntries[0].segmentEntries))

	afterTest(t, levelManager)
}

func TestAddAndRemoveSameBatch(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 10)
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
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

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
	// Rqnge start of nil means start at the beginning
	testNilRangeStartAndEnd(t, nil, createKey(1000))
	// Range end of nil means right to the end
	testNilRangeStartAndEnd(t, createKey(0), nil)
	// Full range
	testNilRangeStartAndEnd(t, nil, nil)
}

func testNilRangeStartAndEnd(t *testing.T, rangeStart []byte, rangeEnd []byte) {
	t.Helper()
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 10)
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
	maxTableEntries := 100
	numEntries := 1000
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, maxTableEntries)
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
			err = levelManager.ApplyChangesNoCheck(*regBatch)
			require.NoError(t, err)
			// Get the table ids to make sure they were added ok
			for i := 0; i < len(regBatch.Registrations); i++ {
				oIDs, err := levelManager.QueryTablesInRange(regBatch.Registrations[i].KeyStart,
					common.IncrementBytesBigEndian(regBatch.Registrations[i].KeyEnd))
				require.NoError(t, err)
				require.Equal(t, 1, len(oIDs))
				nIDs := oIDs[0]
				require.Equal(t, 1, len(nIDs))
				require.Equal(t, regBatch.Registrations[i].TableID, nIDs[0].ID)
				// We store the sstableid on the entry so we can use it later in the deregistration
				entries[entryNum+1-len(regBatch.Registrations)+i].tableID = nIDs[0].ID
			}
			checkState(t, levelManager, entryNum+1, batchNum, level, maxTableEntries, ordered, false)
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
			err := levelManager.ApplyChangesNoCheck(*regBatch)
			require.NoError(t, err)
			// Get the table ids to make sure they were removed ok
			for i := 0; i < len(regBatch.DeRegistrations); i++ {
				oIDs, err := levelManager.QueryTablesInRange(regBatch.DeRegistrations[i].KeyStart,
					common.IncrementBytesBigEndian(regBatch.DeRegistrations[i].KeyEnd))
				require.NoError(t, err)
				require.Equal(t, 0, len(oIDs))
			}
			entriesLeft := len(entries) - entryNum - 1
			checkState(t, levelManager, entriesLeft, batchNum, level, maxTableEntries, ordered, true)
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

func checkState(t *testing.T, levelManager *LevelManager, numEntries int, batchNum int, level int, maxTableEntries int, ordered bool, deleting bool) {
	t.Helper()
	err := levelManager.Validate(false)
	require.NoError(t, err)
	mr := levelManager.getMasterRecord()
	require.NotNil(t, mr)
	require.Equal(t, batchNum+1, int(mr.version))

	segEntries := mr.levelSegmentEntries[level]
	if numEntries == 0 {
		require.Equal(t, 0, len(segEntries.segmentEntries))
		return
	}
	if deleting {
		// When deleting most of the segments hang around until the end, as that's when their last entry is deleted
		return
	}

	require.NotNil(t, segEntries)

	// perfectNumSegs is what we would expect if all entries are added in key order and segments fill up one by one
	perfectNumSegs := (numEntries-1)/maxTableEntries + 1

	// However in the general case:
	// Each seg could possibly have one segment in between
	// Consider the following
	// We attempt to insert an entry in a segment but it is already full, the next segment is full too
	// so we create a new segment between them
	// So, in the worst case, the upper bound of number of segments is:
	maxSegs := 2*perfectNumSegs - 1
	// With incrementing keys all segments will fill from left to right and this will give smallest number of segments
	// With keys arriving in random order the number of segments will be somewhere between perfectNumSegs and maxSegs
	// It works out that we need approx 30% extra segments than if they were all perfectly packed

	if ordered {
		require.LessOrEqual(t, len(segEntries.segmentEntries), perfectNumSegs)
	} else {
		require.LessOrEqual(t, len(segEntries.segmentEntries), maxSegs)
	}
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

func afterTest(t *testing.T, levelManager *LevelManager) {
	t.Helper()
	err := levelManager.Validate(false)
	require.NoError(t, err)
}

func validateTabIds(t *testing.T, expectedTabIDs OverlappingTables, actualTabIDs OverlappingTables) {
	t.Helper()
	require.Equal(t, expectedTabIDs, actualTabIDs)
}

func getInRange(t *testing.T, levelManager *LevelManager, ks int, ke int) OverlappingTables {
	t.Helper()
	keyStart := createKey(ks)
	keyEnd := createKey(ke)
	overlapTabIDs, err := levelManager.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)
	return overlapTabIDs
}

func setupLevelManager(t *testing.T) (*LevelManager, func(t *testing.T)) {
	t.Helper()
	return setupLevelManagerWithMaxEntries(t, 10000)
}

func setupLevelManagerWithMaxEntries(t *testing.T, maxSegmentTableEntries int) (*LevelManager, func(t *testing.T)) {
	return setupLevelManagerWithConfigSetter(t, false, func(cfg *conf.Config) {
		cfg.MaxRegistrySegmentTableEntries = maxSegmentTableEntries
	})
}

func setupLevelManagerWithConfigSetter(t *testing.T, enableCompaction bool, configSetter func(cfg *conf.Config)) (*LevelManager, func(t *testing.T)) {
	t.Helper()
	return setupLevelManagerWithDedup(t, enableCompaction, false, false, configSetter)
}

func setupLevelManagerWithDedup(t *testing.T, enableCompaction bool, validate bool, enabledDedup bool,
	configSetter func(cfg *conf.Config)) (*LevelManager, func(t *testing.T)) {
	t.Helper()
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	configSetter(&cfg)
	cloudStore := &dev.InMemStore{}
	tabCache, err := tabcache.NewTableCache(cloudStore, &cfg)
	require.NoError(t, err)
	bi := testCommandBatchIngestor{}
	lm := NewLevelManager(&cfg, cloudStore, tabCache, bi.ingest, enableCompaction, validate, enabledDedup)
	bi.lm = lm
	err = lm.Start(true)
	require.NoError(t, err)
	err = lm.Activate()
	require.NoError(t, err)
	return lm, func(t *testing.T) {
		err := lm.Stop()
		require.NoError(t, err)
		err = tabCache.Stop()
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

func removeTables(t *testing.T, levelManager *LevelManager, level int, tabIDs []QueryTableInfo, pairs ...int) {
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
	err := levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
}

func addTables(t *testing.T, levelManager *LevelManager, level int, pairs ...int) []QueryTableInfo {
	t.Helper()
	addRegEntries, addTableIDs := createRegistrationEntries(t, level, pairs...)
	regBatch := RegistrationBatch{
		Registrations: addRegEntries,
	}
	err := levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
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

func TestDataResetOnRestartWithoutFlush(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabIDs := addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)
	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(tabIDs), ids)

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	oids, err = levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Nil(t, oids)
}

func TestDataRestoredOnRestartWithFlush(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabIDs := addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)
	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(tabIDs), ids)

	_, _, err = levelManager.Flush(false)
	require.NoError(t, err)
	ver := levelManager.getMasterRecord().version

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	ver2 := levelManager.getMasterRecord().version
	require.Equal(t, ver, ver2)

	oids, err = levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, NonOverlappingTables(tabIDs), ids)
}

func TestFlushManyAdds(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithConfigSetter(t, false, func(cfg *conf.Config) {
		cfg.MaxRegistrySegmentTableEntries = 100
		cfg.LevelManagerFlushInterval = -1 // disable flush
	})
	defer tearDown(t)
	numAdds := 1000
	ks := 0
	var allTabIDs []QueryTableInfo
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}

	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(allTabIDs), ids)

	mr := levelManager.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1].segmentEntries))

	segsAdded, segsDeleted, err := levelManager.Flush(false)
	require.NoError(t, err)
	require.Equal(t, 10, segsAdded)
	require.Equal(t, 0, segsDeleted)

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	oids, err = levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, NonOverlappingTables(allTabIDs), ids)

	ver2 := levelManager.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestFlushManyAddsAndSomeDeletes(t *testing.T) {

	levelManager, tearDown := setupLevelManagerWithConfigSetter(t, false, func(cfg *conf.Config) {
		cfg.MaxRegistrySegmentTableEntries = 100
		cfg.LevelManagerFlushInterval = -1 // disable flush
	})
	defer tearDown(t)
	numAdds := 1000
	ks := 0
	var allTabIDs []QueryTableInfo
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}
	mr := levelManager.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1].segmentEntries))

	// Now we delete enough to reduce number of segments to 9
	numDels := 100
	var deregEntries []RegistrationEntry
	ks = 0
	for i := 0; i < numDels; i++ {
		deregEntries = append(deregEntries, RegistrationEntry{
			Level:    1,
			TableID:  allTabIDs[i].ID,
			KeyStart: createKey(ks),
			KeyEnd:   createKey(ks + 1),
		})
		ks += 3
	}
	err := levelManager.ApplyChangesNoCheck(RegistrationBatch{
		DeRegistrations: deregEntries,
	})
	require.NoError(t, err)

	mr = levelManager.getMasterRecord()
	// Should be 9 segments
	require.Equal(t, 9, len(mr.levelSegmentEntries[1].segmentEntries))

	ver = mr.version
	require.Equal(t, numAdds+1, int(ver))

	// Even though we created 10 and deleted one segment, overall we just pushed 9 adds
	segsAdded, segsDeleted, err := levelManager.Flush(false)
	require.NoError(t, err)

	require.Equal(t, 9, segsAdded)
	require.Equal(t, 0, segsDeleted)

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(allTabIDs[100:]), ids)

	ver2 := levelManager.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestFlushManyDeletes(t *testing.T) {
	// First add a bunch
	levelManager, tearDown := setupLevelManagerWithConfigSetter(t, false, func(cfg *conf.Config) {
		cfg.MaxRegistrySegmentTableEntries = 100
		cfg.LevelManagerFlushInterval = -1 // disable flush
	})
	defer tearDown(t)
	numAdds := 1000
	ks := 0
	var allTabIDs []QueryTableInfo
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}
	mr := levelManager.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1].segmentEntries))
	// Now Flush
	segsAdded, segsDeleted, err := levelManager.Flush(false)
	require.NoError(t, err)
	require.Equal(t, 10, segsAdded)
	require.Equal(t, 0, segsDeleted)

	// Now we delete enough to reduce number of segments to 9
	numDels := 200
	var deregEntries []RegistrationEntry
	ks = 0
	for i := 0; i < numDels; i++ {
		deregEntries = append(deregEntries, RegistrationEntry{
			Level:    1,
			TableID:  allTabIDs[i].ID,
			KeyStart: createKey(ks),
			KeyEnd:   createKey(ks + 1),
		})
		ks += 3
	}
	err = levelManager.ApplyChangesNoCheck(RegistrationBatch{
		DeRegistrations: deregEntries,
	})
	require.NoError(t, err)

	mr = levelManager.getMasterRecord()
	ver = mr.version
	require.Equal(t, numAdds+1, int(ver))
	// Should be 8 segments
	require.Equal(t, 8, len(mr.levelSegmentEntries[1].segmentEntries))

	segsAdded, segsDeleted, err = levelManager.Flush(false)
	require.NoError(t, err)
	require.Equal(t, 0, segsAdded)
	require.Equal(t, 2, segsDeleted)

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	oids, err := levelManager.QueryTablesInRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, NonOverlappingTables(allTabIDs[200:]), ids)

	ver2 := levelManager.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestMultipleFlushes(t *testing.T) {
	// First add a bunch
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)
	numAdds := 1000

	ks := 0
	var allTabIDs []QueryTableInfo
	for i := 0; i < numAdds; i++ {

		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)

		mr := levelManager.getMasterRecord()
		ver := mr.version
		require.Equal(t, i+1, int(ver))

		oids, err := levelManager.QueryTablesInRange(nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids := oids[0]
		require.Equal(t, NonOverlappingTables(allTabIDs), ids)

		_, _, err = levelManager.Flush(false)
		require.NoError(t, err)

		err = levelManager.Stop()
		require.NoError(t, err)

		levelManager.reset()
		err = levelManager.Start(true)
		require.NoError(t, err)
		err = levelManager.Activate()
		require.NoError(t, err)

		mr = levelManager.getMasterRecord()
		ver = mr.version
		require.Equal(t, i+1, int(ver))

		oids, err = levelManager.QueryTablesInRange(nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids = oids[0]
		require.Equal(t, NonOverlappingTables(allTabIDs), ids)
	}
}

func TestRegisterAndGetSlabRetentions(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)

	allRetentions := map[int]time.Duration{}
	for i := 0; i < 10; i++ {
		retention := time.Duration(i) * time.Minute
		err := levelManager.RegisterSlabRetention(i, retention, false, 0)
		require.NoError(t, err)
		allRetentions[i] = retention
	}

	for slabID, retention := range allRetentions {
		ret, err := levelManager.GetSlabRetention(slabID)
		require.NoError(t, err)
		require.Equal(t, retention, ret)
	}

	err := levelManager.UnregisterSlabRetention(1, false, 0)
	require.NoError(t, err)
	ret, err := levelManager.GetSlabRetention(1)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), ret)

	// Now flush and restart - slab retentions should still be there
	_, _, err = levelManager.Flush(false)
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	for slabID, retention := range allRetentions {
		if slabID == 1 {
			continue
		}
		ret, err := levelManager.GetSlabRetention(slabID)
		require.NoError(t, err)
		require.Equal(t, retention, ret)
	}
}

func TestAddIdempotency(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regEntry1 := RegistrationEntry{
		Level:    0,
		TableID:  tabID,
		KeyStart: createKey(0),
		KeyEnd:   createKey(10),
	}
	// Adding tables must be idempotent as same batch can be re-applied on reprocessing or on resubmission from client
	// after transient error (e.g. network error)
	regBatch := RegistrationBatch{Registrations: []RegistrationEntry{regEntry1}}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	overlapTableIDs := getInRange(t, levelManager, 0, 10)
	expectedTabIDs := []NonOverlappingTables{[]QueryTableInfo{{ID: tabID}}}
	validateTabIds(t, expectedTabIDs, overlapTableIDs)

	// Now apply again

	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, expectedTabIDs, overlapTableIDs)

	afterTest(t, levelManager)
}

func TestRemoveIdempotency(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabID1, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regEntry1 := RegistrationEntry{
		Level:    0,
		TableID:  tabID1,
		KeyStart: createKey(0),
		KeyEnd:   createKey(10),
	}
	tabID2, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regEntry2 := RegistrationEntry{
		Level:    0,
		TableID:  tabID2,
		KeyStart: createKey(0),
		KeyEnd:   createKey(10),
	}
	regBatch := RegistrationBatch{Registrations: []RegistrationEntry{regEntry1, regEntry2}}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs := getInRange(t, levelManager, 0, 10)
	validateTabIds(t, []NonOverlappingTables{[]QueryTableInfo{{ID: tabID2}}, []QueryTableInfo{{ID: tabID1}}}, overlapTableIDs)

	regBatch = RegistrationBatch{DeRegistrations: []RegistrationEntry{regEntry1}}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, []NonOverlappingTables{[]QueryTableInfo{{ID: tabID2}}}, overlapTableIDs)

	// Removing tables must be idempotent as same batch can be re-applied on reprocessing or on resubmission from client
	// after transient error (e.g. network error)

	// Now apply again

	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, []NonOverlappingTables{[]QueryTableInfo{{ID: tabID2}}}, overlapTableIDs)

	// Now remove the other entry
	regBatch = RegistrationBatch{DeRegistrations: []RegistrationEntry{regEntry2}}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, nil, overlapTableIDs)

	// And remove again

	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, nil, overlapTableIDs)

	afterTest(t, levelManager)
}

func TestVersionOnApplyChanges(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	clusterName := "test_cluster"

	tabID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regBatch := RegistrationBatch{
		ClusterName:    clusterName,
		ClusterVersion: 100,
		Registrations: []RegistrationEntry{{
			Level:    0,
			TableID:  tabID,
			KeyStart: createKey(0),
			KeyEnd:   createKey(10),
		}},
	}
	err = callRegisterL0Tables(levelManager, regBatch)
	require.NoError(t, err)

	// higher version
	tabID, err = uuid.New().MarshalBinary()
	require.NoError(t, err)
	regBatch = RegistrationBatch{
		ClusterName:    clusterName,
		ClusterVersion: 101,
		Registrations: []RegistrationEntry{{
			Level:    0,
			TableID:  tabID,
			KeyStart: createKey(0),
			KeyEnd:   createKey(10),
		}},
	}
	err = callRegisterL0Tables(levelManager, regBatch)
	require.NoError(t, err)

	// Now with older version
	tabID, err = uuid.New().MarshalBinary()
	require.NoError(t, err)
	regBatch = RegistrationBatch{
		ClusterName:    clusterName,
		ClusterVersion: 99,
		Registrations: []RegistrationEntry{{
			Level:    0,
			TableID:  tabID,
			KeyStart: createKey(0),
			KeyEnd:   createKey(10),
		}},
	}
	err = callRegisterL0Tables(levelManager, regBatch)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))

	afterTest(t, levelManager)
}

func callRegisterL0Tables(lm *LevelManager, regBatch RegistrationBatch) error {
	ch := make(chan error, 1)
	lm.RegisterL0Tables(regBatch, func(err error) {
		ch <- err
	})
	return <-ch
}

func TestFlushRetriedWhenObjStoreUnavailable(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.LevelManagerFlushInterval = 1 * time.Second
	cloudStore := &dev.InMemStore{}
	bi := testCommandBatchIngestor{}
	tabCache, err := tabcache.NewTableCache(cloudStore, &cfg)
	require.NoError(t, err)
	levelManager := NewLevelManager(&cfg, cloudStore, tabCache, bi.ingest, false, true, false)
	bi.lm = levelManager
	err = levelManager.Start(true)
	require.NoError(t, err)
	cloudStore.SetUnavailable(true)

	initialSize := cloudStore.Size()

	tabID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regBatch := RegistrationBatch{
		ClusterName:    "test_cluster",
		ClusterVersion: 100,
		Registrations: []RegistrationEntry{{
			Level:    0,
			TableID:  tabID,
			KeyStart: createKey(0),
			KeyEnd:   createKey(10),
		}},
	}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	require.Equal(t, initialSize, cloudStore.Size())

	// Wait until a flush occurs
	time.Sleep(2 * cfg.LevelManagerFlushInterval)

	// Should not have flushed
	require.Equal(t, initialSize, cloudStore.Size())

	// Make it available
	cloudStore.SetUnavailable(false)

	// Wait a bit longer
	time.Sleep(2 * time.Second)

	require.Greater(t, cloudStore.Size(), initialSize)

	afterTest(t, levelManager)
}

type testCommandBatchIngestor struct {
	lm *LevelManager
}

func (tc *testCommandBatchIngestor) ingest(buff []byte, complFunc func(error)) {
	regBatch := RegistrationBatch{}
	regBatch.Deserialize(buff, 1)
	go func() {
		err := tc.lm.ApplyChanges(regBatch, false, 0)
		complFunc(err)
	}()
}

func TestLastFlushedVersion(t *testing.T) {
	lm, tearDown := setupLevelManager(t)
	defer tearDown(t)

	lfv, err := lm.LoadLastFlushedVersion()
	require.NoError(t, err)
	require.Equal(t, int64(-1), lfv)

	for i := 0; i < 10; i++ {
		err = lm.StoreLastFlushedVersion(int64(i), false, 0)
		require.NoError(t, err)
		lfv, err := lm.LoadLastFlushedVersion()
		require.NoError(t, err)
		require.Equal(t, int64(i), lfv)
	}

	// flush, stop and restart Level manager
	_, _, err = lm.Flush(false)
	require.NoError(t, err)
	err = lm.Stop()
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)
	err = lm.Activate()
	require.NoError(t, err)

	lfv, err = lm.LoadLastFlushedVersion()
	require.NoError(t, err)
	require.Equal(t, int64(9), lfv)
}

func TestDedupApplyChanges(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	err := lm.ApplyChanges(createBatch(0), false, 0)
	require.NoError(t, err)
	err = lm.ApplyChanges(createBatch(1), false, 1)
	require.NoError(t, err)
	err = lm.ApplyChanges(createBatch(2), false, 2)
	require.NoError(t, err)

	require.Equal(t, 3, lm.GetLevelTableCounts()[0])

	_, _, err = lm.Flush(false)
	require.NoError(t, err)

	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	// These should be ignored
	err = lm.ApplyChanges(createBatch(0), true, 0)
	require.NoError(t, err)
	err = lm.ApplyChanges(createBatch(1), true, 1)
	require.NoError(t, err)
	err = lm.ApplyChanges(createBatch(2), true, 2)
	require.NoError(t, err)

	require.Equal(t, 3, lm.GetLevelTableCounts()[0])

	err = lm.Activate()
	require.NoError(t, err)

	err = lm.ApplyChanges(createBatch(3), false, 3)
	require.NoError(t, err)
	err = lm.ApplyChanges(createBatch(4), false, 4)
	require.NoError(t, err)

	require.Equal(t, 5, lm.GetLevelTableCounts()[0])
}

func TestDedupRegisterSlabRetention(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	err := lm.RegisterSlabRetention(10, 5*time.Hour, false, 5)
	require.NoError(t, err)
	ret, err := lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, 5*time.Hour, ret)

	_, _, err = lm.Flush(false)
	require.NoError(t, err)

	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	err = lm.RegisterSlabRetention(10, 10*time.Hour, true, 5)
	require.NoError(t, err)

	err = lm.Activate()
	require.NoError(t, err)

	ret, err = lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, 5*time.Hour, ret)

	err = lm.RegisterSlabRetention(10, 6*time.Hour, false, 6)
	require.NoError(t, err)
	ret, err = lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, 6*time.Hour, ret)
}

func TestDedupUnregisterSlabRetentions(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	err := lm.RegisterSlabRetention(10, 5*time.Hour, false, 5)
	require.NoError(t, err)
	ret, err := lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, 5*time.Hour, ret)

	err = lm.UnregisterSlabRetention(10, false, 6)
	require.NoError(t, err)
	ret, err = lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), ret)

	_, _, err = lm.Flush(false)
	require.NoError(t, err)

	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	err = lm.UnregisterSlabRetention(10, true, 6)
	require.NoError(t, err)

	err = lm.Activate()
	require.NoError(t, err)

	ret, err = lm.GetSlabRetention(10)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), ret)
}

func TestDedupStoreLastFlushedVersion(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	err := lm.StoreLastFlushedVersion(1000, false, 9)
	require.NoError(t, err)
	lfv, err := lm.LoadLastFlushedVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1000), lfv)

	_, _, err = lm.Flush(false)
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	err = lm.StoreLastFlushedVersion(1001, true, 9)
	require.NoError(t, err)

	err = lm.Activate()
	require.NoError(t, err)

	lfv, err = lm.LoadLastFlushedVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1000), lfv)

	err = lm.StoreLastFlushedVersion(1001, false, 10)
	require.NoError(t, err)
	lfv, err = lm.LoadLastFlushedVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1001), lfv)
}

func TestLastProcessedReplSeqResetOnShutdown(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	err := lm.ApplyChanges(createBatch(0), false, 7)
	require.NoError(t, err)

	require.Equal(t, 7, lm.GetLastProcessedReplSeq())

	_, _, err = lm.Flush(false)
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	err = lm.Activate()
	require.NoError(t, err)

	// replseq should be preserved between flush (not shutdown flush)

	require.Equal(t, 7, lm.GetLastProcessedReplSeq())

	// But reset on shutdown
	_, _, err = lm.Flush(true)
	require.NoError(t, err)
	err = lm.Stop()
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	require.Equal(t, -1, lm.GetLastProcessedReplSeq())
}

func createBatch(i int) RegistrationBatch {
	return RegistrationBatch{
		Registrations: []RegistrationEntry{{
			Level:      0,
			TableID:    []byte(fmt.Sprintf("sst-%d", i)),
			MinVersion: 10,
			MaxVersion: 12,
			KeyStart:   encoding.EncodeVersion([]byte("key000"), 0),
			KeyEnd:     encoding.EncodeVersion([]byte("key100"), 0),
		}},
	}
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
	err := lm.ApplyChangesNoCheck(regBatch0)
	require.NoError(t, err)

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
	err = lm.ApplyChangesNoCheck(regBatch1)
	require.NoError(t, err)

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
	err = lm.ApplyChangesNoCheck(regBatch2)
	require.NoError(t, err)

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
	err = lm.ApplyChangesNoCheck(deregBatch)
	require.NoError(t, err)

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
	regEntry0_1 := regTableInLevel(t, levelManager, 0, 0, 9, 310, 319, 0)
	regEntry0_2 := regTableInLevel(t, levelManager, 0, 10, 19, 320, 329, 1)
	regEntry0_3 := regTableInLevel(t, levelManager, 0, 20, 29, 330, 339, 2)

	regEntry1_1 := regTableInLevel(t, levelManager, 1, 0, 9, 210, 219, 0)
	regEntry1_2 := regTableInLevel(t, levelManager, 1, 10, 19, 220, 229, 0)
	regEntry1_3 := regTableInLevel(t, levelManager, 1, 20, 29, 230, 239, 0)

	regEntry2_1 := regTableInLevel(t, levelManager, 2, 0, 9, 110, 119, 0)
	regEntry2_2 := regTableInLevel(t, levelManager, 2, 10, 19, 120, 129, 0)
	regEntry2_3 := regTableInLevel(t, levelManager, 2, 20, 29, 130, 139, 0)

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
			{ID: regEntry0_3.TableID},
		},
		{
			{ID: regEntry0_2.TableID},
		},
		{
			{ID: regEntry0_1.TableID},
		},
		{
			{ID: regEntry1_1.TableID},
			{ID: regEntry1_2.TableID, DeadVersions: []VersionRange{rng1}},
			{ID: regEntry1_3.TableID},
		},
		{
			{ID: regEntry2_1.TableID},
			{ID: regEntry2_2.TableID},
			{ID: regEntry2_3.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng1, expected)

	rng2 := VersionRange{
		VersionStart: 223,
		VersionEnd:   227,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry0_3.TableID},
		},
		{
			{ID: regEntry0_2.TableID},
		},
		{
			{ID: regEntry0_1.TableID},
		},
		{
			{ID: regEntry1_1.TableID},
			{ID: regEntry1_2.TableID, DeadVersions: []VersionRange{rng1, rng2}},
			{ID: regEntry1_3.TableID},
		},
		{
			{ID: regEntry2_1.TableID},
			{ID: regEntry2_2.TableID},
			{ID: regEntry2_3.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng2, expected)

	rng3 := VersionRange{
		VersionStart: 224,
		VersionEnd:   229,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry0_3.TableID},
		},
		{
			{ID: regEntry0_2.TableID},
		},
		{
			{ID: regEntry0_1.TableID},
		},
		{
			{ID: regEntry1_1.TableID},
			{ID: regEntry1_2.TableID, DeadVersions: []VersionRange{rng1, rng2, rng3}},
			{ID: regEntry1_3.TableID},
		},
		{
			{ID: regEntry2_1.TableID},
			{ID: regEntry2_2.TableID},
			{ID: regEntry2_3.TableID},
		},
	}
	validateDeadVersions(t, levelManager, rng3, expected)

	rng4 := VersionRange{
		VersionStart: 125,
		VersionEnd:   325,
	}
	expected = []NonOverlappingTables{
		{
			{ID: regEntry0_3.TableID},
		},
		{
			{ID: regEntry0_2.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry0_1.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry1_1.TableID, DeadVersions: []VersionRange{rng4}},
			{ID: regEntry1_2.TableID, DeadVersions: []VersionRange{rng1, rng2, rng3, rng4}},
			{ID: regEntry1_3.TableID, DeadVersions: []VersionRange{rng4}},
		},
		{
			{ID: regEntry2_1.TableID},
			{ID: regEntry2_2.TableID, DeadVersions: []VersionRange{rng4}},
			{ID: regEntry2_3.TableID, DeadVersions: []VersionRange{rng4}},
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

	// Now flush and restart
	_, _, err := levelManager.Flush(false)
	require.NoError(t, err)
	err = levelManager.Stop()
	require.NoError(t, err)
	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	tables = getInRange(t, levelManager, 0, 30)
	require.Equal(t, OverlappingTables(expected), tables)

	afterTest(t, levelManager)
}

func validateDeadVersions(t *testing.T, lm *LevelManager, rng VersionRange, expected []NonOverlappingTables) {
	err := lm.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)
	tables := getInRange(t, lm, 0, 30)
	require.Equal(t, OverlappingTables(expected), tables)
}

func regTableInLevel(t *testing.T, lm *LevelManager, level int, keyStart int, keyEnd int, minVersion int, maxVersion int, processorID int) *RegistrationEntry {
	tabID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	regEntry := RegistrationEntry{
		Level:      level,
		TableID:    tabID,
		KeyStart:   createKey(keyStart),
		KeyEnd:     createKey(keyEnd),
		MinVersion: uint64(minVersion),
		MaxVersion: uint64(maxVersion),
	}
	regBatch := RegistrationBatch{Registrations: []RegistrationEntry{regEntry}, ProcessorID: processorID}
	err = lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	return &regEntry
}

func TestRegisterDeadVersionRangeIdempotency(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	regEntry := regTableInLevel(t, levelManager, 0, 0, 9, 75, 90, 0)

	rng := VersionRange{
		VersionStart: 50,
		VersionEnd:   100,
	}

	err := levelManager.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)
	tables := getInRange(t, levelManager, 0, 30)

	expected := []NonOverlappingTables{
		{
			{ID: regEntry.TableID, DeadVersions: []VersionRange{rng}},
		},
	}
	require.Equal(t, OverlappingTables(expected), tables)

	// Applying again should not add version range again
	err = levelManager.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)
	tables = getInRange(t, levelManager, 0, 30)

	require.Equal(t, OverlappingTables(expected), tables)
}

func TestRegisterDeadVersionRangeDedup(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	regEntry := regTableInLevel(t, levelManager, 0, 0, 9, 75, 90, 0)
	rng := VersionRange{
		VersionStart: 50,
		VersionEnd:   100,
	}

	err := levelManager.RegisterDeadVersionRange(rng, "test_cluster", 0, false, 0)
	require.NoError(t, err)
	tables := getInRange(t, levelManager, 0, 30)
	expected := []NonOverlappingTables{
		{
			{ID: regEntry.TableID, DeadVersions: []VersionRange{rng}},
		},
	}
	require.Equal(t, OverlappingTables(expected), tables)

	_, _, err = levelManager.Flush(false)
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)

	err = levelManager.RegisterDeadVersionRange(rng, "test_cluster", 0, true, 0)
	require.NoError(t, err)

	err = levelManager.Activate()
	require.NoError(t, err)

	tables = getInRange(t, levelManager, 0, 30)

	require.Equal(t, OverlappingTables(expected), tables)
}
func TestContainsTable(t *testing.T) {
	populatedSeg := &segment{
		format: 26,
		tableEntries: []*TableEntry{
			{
				SSTableID:  []byte("sstableid1"),
				RangeStart: []byte("rangestart1"),
				RangeEnd:   []byte("rangeend1"),
			},
			{
				SSTableID:  []byte("sstableid2"),
				RangeStart: []byte("rangestart2"),
				RangeEnd:   []byte("rangeend2"),
			},
		},
	}
	emptySeg := &segment{
		format:       26,
		tableEntries: []*TableEntry{},
	}
	var testCases = []struct {
		name     string
		seg      *segment
		tabID    []byte
		expected bool
	}{
		{
			name:     "sstableid1",
			seg:      populatedSeg,
			tabID:    []byte("sstableid1"),
			expected: true,
		},
		{
			name:     "sstableid2",
			seg:      populatedSeg,
			tabID:    []byte("sstableid2"),
			expected: true,
		},
		{
			name:     "non-existing table id",
			seg:      populatedSeg,
			tabID:    []byte("sstableid3"),
			expected: false,
		},
		{
			name:     "empty table id",
			seg:      emptySeg,
			tabID:    []byte(""),
			expected: false,
		},
		{
			name:     "table id less than first entry",
			seg:      populatedSeg,
			tabID:    []byte("aaaaa"),
			expected: false,
		},
		{
			name:     "table id greater than last entry",
			seg:      populatedSeg,
			tabID:    []byte("zzzzz"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := containsTable(tc.seg, tc.tabID)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestGetSegmentForRegistration(t *testing.T) {
	segmentEntries := []segmentEntry{
		{rangeStart: []byte("aaa"), rangeEnd: []byte("ccc")},
		{rangeStart: []byte("ddd"), rangeEnd: []byte("fff")},
		{rangeStart: []byte("ggg"), rangeEnd: []byte("iii")},
	}

	// Test cases
	testCases := []struct {
		name         string
		registration RegistrationEntry
		expected     int
	}{
		{name: "no valid insertion point", registration: RegistrationEntry{KeyStart: []byte("b"), KeyEnd: []byte("e")}, expected: -1},
		{name: "find segment at the head", registration: RegistrationEntry{KeyStart: []byte("a"), KeyEnd: []byte("aa")}, expected: 0},
		{name: "find segment between first and second segment", registration: RegistrationEntry{KeyStart: []byte("ccd"), KeyEnd: []byte("ccdd")}, expected: 0},
		{name: "find segment between second and third segment", registration: RegistrationEntry{KeyStart: []byte("ffg"), KeyEnd: []byte("ffgg")}, expected: 1},
		{name: "find segment at the end", registration: RegistrationEntry{KeyStart: []byte("iiii"), KeyEnd: []byte("j")}, expected: 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			index := getSegmentForRegistration(segmentEntries, tc.registration)
			if index != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, index)
			}
		})
	}
}

func TestGetSegmentEntryForDeregistration(t *testing.T) {
	segmentEntries := []segmentEntry{
		{rangeStart: []byte("aaa"), rangeEnd: []byte("ccc")},
		{rangeStart: []byte("ddd"), rangeEnd: []byte("fff")},
		{rangeStart: []byte("ggg"), rangeEnd: []byte("iii")},
	}

	testCases := []struct {
		name           string
		segmentEntries []segmentEntry
		deRegistration RegistrationEntry
		expected       int
	}{
		{
			name:           "exact match first segment",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("aaa"), KeyEnd: []byte("ccc")},
			expected:       0,
		},
		{
			name:           "within first segment",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("bbb"), KeyEnd: []byte("bbb")},
			expected:       0,
		},
		{
			name:           "overlapping two segments",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("ccc"), KeyEnd: []byte("ddd")},
			expected:       -1,
		},
		{
			name:           "before first segment",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("000"), KeyEnd: []byte("999")},
			expected:       -1,
		},
		{
			name:           "after last segment",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("jjj"), KeyEnd: []byte("zzz")},
			expected:       -1,
		},
		{
			name:           "larger than any segment",
			segmentEntries: segmentEntries,
			deRegistration: RegistrationEntry{KeyStart: []byte("aaa"), KeyEnd: []byte("zzz")},
			expected:       -1,
		},
		{
			name:           "empty segment entries",
			segmentEntries: []segmentEntry{},
			deRegistration: RegistrationEntry{KeyStart: []byte("aaa"), KeyEnd: []byte("bbb")},
			expected:       -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getSegmentEntryForDeregistration(tc.segmentEntries, tc.deRegistration)
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}
