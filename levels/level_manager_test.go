package levels

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/stretchr/testify/require"
	"math/rand"
	"reflect"
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
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get entire contiguous block with smaller range
	overlapTableIDs = getInRange(t, levelManager, 4, 10)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get entire contiguous block with larger range
	overlapTableIDs = getInRange(t, levelManager, 2, 14)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[0]},
	}, overlapTableIDs)

	// Get exactly one from contiguous block
	overlapTableIDs = getInRange(t, levelManager, 6, 8)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[1]},
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
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[5]},
		NonoverlappingTableIDs{addedTableIDs[4]},
		NonoverlappingTableIDs{addedTableIDs[3]},
	}, overlapTableIDs)

	// Select subset with gaps
	overlapTableIDs = getInRange(t, levelManager, 18, 22)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[4]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 18, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[5]},
		NonoverlappingTableIDs{addedTableIDs[4]},
	}, overlapTableIDs)

	// Exactly overlapping
	overlapTableIDs = getInRange(t, levelManager, 32, 41)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[8]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[6]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 31, 41)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[8]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[6]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 35, 35)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[8]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[6]},
	}, overlapTableIDs)

	// Fully inside
	overlapTableIDs = getInRange(t, levelManager, 42, 48)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[10]},
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 41, 49)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[10]},
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 45, 46)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[10]},
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 41, 43)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 48, 49)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)

	// Select entire block overlapping next
	overlapTableIDs = getInRange(t, levelManager, 51, 61)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[13]},
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 53, 58)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[13]},
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 51, 53)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[11]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, levelManager, 55, 56)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[13]},
		NonoverlappingTableIDs{addedTableIDs[12]},
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
	validateTabIds(t, OverlappingTableIDs{addedTableIDs}, overlapTableIDs)

	// Get subset block with exact range
	overlapTableIDs = getInRange(t, levelManager, 6, 16)
	validateTabIds(t, OverlappingTableIDs{addedTableIDs[1:4]}, overlapTableIDs)

	// Get entire block with larger range
	overlapTableIDs = getInRange(t, levelManager, 0, 1000)
	validateTabIds(t, OverlappingTableIDs{addedTableIDs}, overlapTableIDs)

	// Get single table
	overlapTableIDs = getInRange(t, levelManager, 9, 11)
	validateTabIds(t, OverlappingTableIDs{{addedTableIDs[2]}}, overlapTableIDs)

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
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs0[2]},
		NonoverlappingTableIDs{addedTableIDs0[1]},
		NonoverlappingTableIDs{addedTableIDs0[0]},
		addedTableIDs1,
		addedTableIDs2,
	}, overlapTableIDs)

	// Get selection
	overlapTableIDs = getInRange(t, levelManager, 55, 58)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs0[2]},
		NonoverlappingTableIDs{addedTableIDs0[1]},
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
	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, addedTableIDs, []sst.SSTableID(ids))
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
	oids, _, _, err := levelManager.GetTableIDsForRange(createKey(2), createKey(2*(numEntries-2)))
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	expected := addedTableIDs[1 : len(addedTableIDs)-2]
	require.Equal(t, expected, []sst.SSTableID(ids))
}

func TestAddAndRemove_L0(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)

	addedTableIDs := addTables(t, levelManager, 0,
		2, 4, 5, 7, 14, 17, 20, 21, 23, 30)

	overlapTableIDs := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[4]},
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[0]},
	}, overlapTableIDs)

	deregEntry0 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[4][0],
		KeyStart: createKey(2),
		KeyEnd:   createKey(4),
	}
	deregBatch := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{deregEntry0},
	}
	err := levelManager.ApplyChangesNoCheck(deregBatch)
	require.NoError(t, err)

	overlapTableIDs2 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[4]},
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[1]},
	}, overlapTableIDs2)

	deregEntry2 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[2][0],
		KeyStart: createKey(14),
		KeyEnd:   createKey(17),
	}

	deregEntry4 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[0][0],
		KeyStart: createKey(23),
		KeyEnd:   createKey(30),
	}

	deregBatch2 := RegistrationBatch{
		DeRegistrations: []RegistrationEntry{deregEntry2, deregEntry4},
	}
	err = levelManager.ApplyChangesNoCheck(deregBatch2)
	require.NoError(t, err)

	overlapTableIDs3 := getInRange(t, levelManager, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[1]},
	}, overlapTableIDs3)

	deregEntry1 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[3][0],
		KeyStart: createKey(5),
		KeyEnd:   createKey(7),
	}

	deregEntry3 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[1][0],
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
			TableID:  tableIDs[1],
			KeyStart: createKey(6),
			KeyEnd:   createKey(7),
		}},
	}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	oTabIDs, _, _, err := levelManager.GetTableIDsForRange(createKey(3), createKey(1000))
	require.NoError(t, err)
	require.Equal(t, 1, len(oTabIDs))
	noTabIDs := oTabIDs[0]
	require.Equal(t, 4, len(noTabIDs))

	require.Equal(t, tableIDs[0], noTabIDs[0])
	require.Equal(t, tableIDs[2], noTabIDs[1])
	require.Equal(t, sst.SSTableID(newTableID1), noTabIDs[2])
	require.Equal(t, sst.SSTableID(newTableID2), noTabIDs[3])

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

	oTabIDs, _, _, err := levelManager.GetTableIDsForRange(rangeStart, rangeEnd)
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
				oIDs, _, _, err := levelManager.GetTableIDsForRange(regBatch.Registrations[i].KeyStart,
					common.IncrementBytesBigEndian(regBatch.Registrations[i].KeyEnd))
				require.NoError(t, err)
				require.Equal(t, 1, len(oIDs))
				nIDs := oIDs[0]
				require.Equal(t, 1, len(nIDs))
				require.Equal(t, regBatch.Registrations[i].TableID, nIDs[0])
				// We store the sstableid on the entry so we can use it later in the deregistration
				entries[entryNum+1-len(regBatch.Registrations)+i].tableID = nIDs[0]
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
				oIDs, _, _, err := levelManager.GetTableIDsForRange(regBatch.DeRegistrations[i].KeyStart,
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

func validateTabIds(t *testing.T, expectedTabIDs OverlappingTableIDs, actualTabIDs OverlappingTableIDs) {
	t.Helper()
	require.Equal(t, expectedTabIDs, actualTabIDs)
}

func getInRange(t *testing.T, levelManager *LevelManager, ks int, ke int) OverlappingTableIDs {
	t.Helper()
	keyStart := createKey(ks)
	keyEnd := createKey(ke)
	overlapTabIDs, _, _, err := levelManager.GetTableIDsForRange(keyStart, keyEnd)
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
	return encoding.EncodeVersion([]byte(fmt.Sprintf("prefix/key-%010d", i)), 0)
}

func removeTables(t *testing.T, levelManager *LevelManager, level int, tabIDs []sst.SSTableID, pairs ...int) {
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
			TableID:  tabID,
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

func addTables(t *testing.T, levelManager *LevelManager, level int, pairs ...int) []sst.SSTableID {
	t.Helper()
	addRegEntries, addTableIDs := createRegistrationEntries(t, level, pairs...)
	regBatch := RegistrationBatch{
		Registrations: addRegEntries,
	}
	err := levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	return addTableIDs
}

func createRegistrationEntries(t *testing.T, level int, pairs ...int) ([]RegistrationEntry, []sst.SSTableID) {
	t.Helper()
	var regEntries []RegistrationEntry
	var tableIDs []sst.SSTableID
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
		tableIDs = append(tableIDs, tabID)
	}
	return regEntries, tableIDs
}

func TestDataResetOnRestartWithoutFlush(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabIDs := addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)
	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))

	err = levelManager.Stop()
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	oids, _, _, err = levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Nil(t, oids)
}

func TestDataRestoredOnRestartWithFlush(t *testing.T) {
	levelManager, tearDown := setupLevelManager(t)
	defer tearDown(t)
	tabIDs := addTables(t, levelManager, 1, 3, 5, 6, 7, 8, 12)
	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))

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

	oids, _, _, err = levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))
}

func TestFlushManyAdds(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithConfigSetter(t, false, func(cfg *conf.Config) {
		cfg.MaxRegistrySegmentTableEntries = 100
		cfg.LevelManagerFlushInterval = -1 // disable flush
	})
	defer tearDown(t)
	numAdds := 1000
	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}

	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs, []sst.SSTableID(ids))

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

	oids, _, _, err = levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, allTabIDs, []sst.SSTableID(ids))

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
	var allTabIDs []sst.SSTableID
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
			TableID:  allTabIDs[i],
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

	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs[100:], []sst.SSTableID(ids))

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
	var allTabIDs []sst.SSTableID
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
			TableID:  allTabIDs[i],
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

	oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs[200:], []sst.SSTableID(ids))

	ver2 := levelManager.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestMultipleFlushes(t *testing.T) {
	// First add a bunch
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)
	numAdds := 1000

	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {

		tabIDs := addTables(t, levelManager, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)

		mr := levelManager.getMasterRecord()
		ver := mr.version
		require.Equal(t, i+1, int(ver))

		oids, _, _, err := levelManager.GetTableIDsForRange(nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids := oids[0]
		require.Equal(t, allTabIDs, []sst.SSTableID(ids))

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

		oids, _, _, err = levelManager.GetTableIDsForRange(nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids = oids[0]
		require.Equal(t, allTabIDs, []sst.SSTableID(ids))
	}
}

func TestRegisterAndGetPrefixRetentions(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 100)
	defer tearDown(t)

	allPrefixRetentions := map[string]uint64{}
	var ret uint64
	for i := 0; i < 10; i++ {
		var prefixes []retention.PrefixRetention
		for j := 0; j < 10; j++ {
			prefix := fmt.Sprintf("prefix-%d-%d", i, j)
			prefixes = append(prefixes, retention.PrefixRetention{Prefix: []byte(prefix), Retention: ret})
			allPrefixRetentions[prefix] = ret
			ret++
		}
		err := levelManager.RegisterPrefixRetentions(prefixes, false, 0)
		require.NoError(t, err)
	}

	prefRets, err := levelManager.GetPrefixRetentions()
	require.NoError(t, err)
	require.NotNil(t, prefRets)
	require.Equal(t, len(allPrefixRetentions), len(prefRets))
	for _, prefix := range prefRets {
		_, ok := allPrefixRetentions[string(prefix.Prefix)]
		require.True(t, ok)
	}

	// Now flush and restart - prefixRetentions should still be there
	_, _, err = levelManager.Flush(false)
	require.NoError(t, err)

	levelManager.reset()
	err = levelManager.Start(true)
	require.NoError(t, err)
	err = levelManager.Activate()
	require.NoError(t, err)

	prefRets, err = levelManager.GetPrefixRetentions()
	require.NoError(t, err)
	require.NotNil(t, prefRets)
	require.Equal(t, len(allPrefixRetentions), len(prefRets))
	for _, prefRet := range prefRets {
		ret, ok := allPrefixRetentions[string(prefRet.Prefix)]
		require.True(t, ok)
		require.Equal(t, ret, prefRet.Retention)
	}
}

func TestPrefixRetentionsAreRemovedWhenNoData(t *testing.T) {
	prefixRetentionRemoveCheckPeriod := 100 * time.Millisecond
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = 1
		cfg.LevelMultiplier = 1
		cfg.PrefixRetentionRemoveCheckInterval = prefixRetentionRemoveCheckPeriod
	})
	defer tearDown(t)

	delPrefix1 := []byte("prefix1")
	delPrefix2 := []byte("prefix2")
	delPrefix3 := []byte("prefix3")
	delPrefix4 := []byte("prefix4")

	// add data in LSM for prefixRetentions 1 and 3 but not the other two

	sst1_1 := TableEntry{
		SSTableID:  []byte("sst1_1"),
		RangeStart: append(delPrefix1, []byte("key00010")...),
		RangeEnd:   append(delPrefix1, []byte("key00020")...),
	}
	sst1_2 := TableEntry{
		SSTableID:  []byte("sst1_2"),
		RangeStart: append(delPrefix3, []byte("key00010")...),
		RangeEnd:   append(delPrefix3, []byte("key00020")...),
	}
	populateLevel(t, lm, 1, sst1_1, sst1_2)

	// Set up 4 prefixRetentions

	delPrefixes := []retention.PrefixRetention{{Prefix: delPrefix1}, {Prefix: delPrefix2}, {Prefix: delPrefix3}, {Prefix: delPrefix4}}
	delPrefixesMap := map[string]struct{}{
		string(delPrefix1): {},
		string(delPrefix2): {},
		string(delPrefix3): {},
		string(delPrefix4): {},
	}

	err := lm.RegisterPrefixRetentions(delPrefixes, false, 0)
	require.NoError(t, err)

	prefixes, err := lm.GetPrefixRetentions()
	require.NoError(t, err)
	require.Equal(t, len(delPrefixesMap), len(prefixes))
	for _, prefix := range prefixes {
		_, ok := delPrefixesMap[string(prefix.Prefix)]
		require.True(t, ok)
	}

	time.Sleep(2 * prefixRetentionRemoveCheckPeriod)

	delete(delPrefixesMap, string(delPrefix2))
	delete(delPrefixesMap, string(delPrefix4))

	prefixes, err = lm.GetPrefixRetentions()
	require.NoError(t, err)
	require.Equal(t, len(delPrefixesMap), len(prefixes))
	for _, prefix := range prefixes {
		_, ok := delPrefixesMap[string(prefix.Prefix)]
		require.True(t, ok)
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
	expectedTabIDs := []NonoverlappingTableIDs{[]sst.SSTableID{tabID}}
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
	validateTabIds(t, []NonoverlappingTableIDs{[]sst.SSTableID{tabID2}, []sst.SSTableID{tabID1}}, overlapTableIDs)

	regBatch = RegistrationBatch{DeRegistrations: []RegistrationEntry{regEntry1}}
	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, []NonoverlappingTableIDs{[]sst.SSTableID{tabID2}}, overlapTableIDs)

	// Removing tables must be idempotent as same batch can be re-applied on reprocessing or on resubmission from client
	// after transient error (e.g. network error)

	// Now apply again

	err = levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	overlapTableIDs = getInRange(t, levelManager, 0, 10)
	validateTabIds(t, []NonoverlappingTableIDs{[]sst.SSTableID{tabID2}}, overlapTableIDs)

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

func TestGetTableIDsInRangeReturnsDeadVersions(t *testing.T) {
	levelManager, tearDown := setupLevelManagerWithMaxEntries(t, 10)
	defer tearDown(t)

	regEntry := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst1"),
		MinVersion: 0,
		MaxVersion: 100,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry},
	}
	err := levelManager.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	rng1 := VersionRange{
		VersionStart: 12,
		VersionEnd:   23,
	}
	rng2 := VersionRange{
		VersionStart: 34,
		VersionEnd:   67,
	}
	rng3 := VersionRange{
		VersionStart: 34,
		VersionEnd:   78,
	}
	err = levelManager.RegisterDeadVersionRange(rng1, "cluster1", 0, false, 0)
	require.NoError(t, err)
	err = levelManager.RegisterDeadVersionRange(rng2, "cluster1", 0, false, 1)
	require.NoError(t, err)
	err = levelManager.RegisterDeadVersionRange(rng3, "cluster2", 0, false, 2)
	require.NoError(t, err)

	_, _, deadVersions, err := levelManager.GetTableIDsForRange(nil, nil)
	require.NoError(t, err)
	require.Equal(t, []VersionRange{rng1, rng2, rng3}, deadVersions)
}

func TestDeadVersionsRemovedOnStartup(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = 10
	})
	defer tearDown(t)

	regEntry := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst1"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	// Apply a lock to prevent compaction happening straight away
	lm.lockTable("sst1")
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)
	lm.unlockTable("sst1")

	stats := lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)

	// Now stop and start the level manager
	_, _, err = lm.Flush(true)
	require.NoError(t, err)
	err = lm.Stop()
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	// Job should be created

	job, err := getJob(lm)
	require.NoError(t, err)
	require.False(t, job.isMove)

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job.tables[0][0].deadVersionRanges)
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

func TestDedupRegisterDeadVersionRange(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	// Add a batch to prevent dead version range being immediately removed
	err := lm.ApplyChanges(createBatch(0), false, 0)
	require.NoError(t, err)

	deadRange1 := VersionRange{
		VersionStart: 10,
		VersionEnd:   12,
	}
	err = lm.RegisterDeadVersionRange(deadRange1, "", 0, false, 7)
	require.NoError(t, err)

	deadRanges := lm.getDeadVersions()
	require.Equal(t, []VersionRange{deadRange1}, deadRanges)

	_, _, err = lm.Flush(false)
	require.NoError(t, err)
	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	deadRange2 := VersionRange{
		VersionStart: 20,
		VersionEnd:   23,
	}

	err = lm.RegisterDeadVersionRange(deadRange2, "", 0, true, 7)
	require.NoError(t, err)

	deadRanges = lm.getDeadVersions()
	require.Equal(t, []VersionRange{deadRange1}, deadRanges)

	err = lm.Activate()
	require.NoError(t, err)

	err = lm.RegisterDeadVersionRange(deadRange2, "", 0, false, 8)
	require.NoError(t, err)

	deadRanges = lm.getDeadVersions()
	require.Equal(t, []VersionRange{deadRange1, deadRange2}, deadRanges)
}

func TestDedupRegisterPrefixRetentions(t *testing.T) {
	lm, tearDown := setupLevelManagerWithDedup(t, false, false, true, func(cfg *conf.Config) {
	})
	defer tearDown(t)

	prefs1 := []retention.PrefixRetention{{
		Prefix:    []byte("pref1"),
		Retention: 123,
	}}
	err := lm.RegisterPrefixRetentions(prefs1, false, 5)
	require.NoError(t, err)
	res, err := lm.GetPrefixRetentions()
	require.NoError(t, err)
	require.Equal(t, prefs1, res)

	_, _, err = lm.Flush(false)
	require.NoError(t, err)

	lm.reset()
	err = lm.Start(true)
	require.NoError(t, err)

	err = lm.RegisterPrefixRetentions(prefs1, true, 5)
	require.NoError(t, err)

	err = lm.Activate()
	require.NoError(t, err)

	res, err = lm.GetPrefixRetentions()
	require.NoError(t, err)
	require.Equal(t, prefs1, res)

	prefs2 := []retention.PrefixRetention{{
		Prefix:    []byte("pref2"),
		Retention: 345,
	}}
	err = lm.RegisterPrefixRetentions(prefs2, false, 6)
	require.NoError(t, err)

	res, err = lm.GetPrefixRetentions()
	require.NoError(t, err)
	requirePrefsInAnyOrder(t, append(prefs1, prefs2...), res)
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

func requirePrefsInAnyOrder(t *testing.T, expected []retention.PrefixRetention, actual []retention.PrefixRetention) {
	require.Equal(t, len(expected), len(actual))
	for _, e := range expected {
		found := false
		for _, a := range actual {
			if reflect.DeepEqual(e, a) {
				found = true
				break
			}
		}
		require.True(t, found)
	}
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
