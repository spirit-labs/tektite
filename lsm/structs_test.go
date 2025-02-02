package lsm

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestSerializeDeserializeOverlappingTableIDs(t *testing.T) {
	var overlapping OverlappingTables
	for i := 0; i < 10; i++ {
		var notOverlapping []QueryTableInfo
		for j := 0; j < 10; j++ {
			u, err := uuid.NewUUID()
			require.NoError(t, err)
			bytes, err := u.MarshalBinary()
			require.NoError(t, err)
			var dvs []VersionRange
			dvs = append(dvs, VersionRange{
				VersionStart: 111,
				VersionEnd:   222,
			})
			dvs = append(dvs, VersionRange{
				VersionStart: 333,
				VersionEnd:   555,
			})
			dvs = append(dvs, VersionRange{
				VersionStart: 777,
				VersionEnd:   888,
			})
			notOverlapping = append(notOverlapping, QueryTableInfo{
				ID:           bytes,
				DeadVersions: dvs,
			})
		}
		overlapping = append(overlapping, notOverlapping)
	}

	var buff []byte
	buff = append(buff, 1, 2, 3)
	require.NotNil(t, overlapping)
	buff = overlapping.Serialize(buff)
	overlappingAfter, _ := DeserializeOverlappingTables(buff, 3)
	require.Equal(t, overlapping, overlappingAfter)
}

func TestSerializeDeserializeRegistrationEntry(t *testing.T) {
	regEntry := &RegistrationEntry{
		Level:       23,
		TableID:     sst.SSTableID("sometableid"),
		KeyStart:    []byte("keystart"),
		KeyEnd:      []byte("keyend"),
		MinVersion:  2536353,
		MaxVersion:  2353653,
		DeleteRatio: 0.25,
		AddedTime:   12345,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = regEntry.Serialize(buff)

	regEntryAfter := &RegistrationEntry{}
	regEntryAfter.Deserialize(buff, 3)

	require.Equal(t, regEntry, regEntryAfter)
}

func TestSerializeDeserializeRegistrationBatch(t *testing.T) {
	regBatch := &RegistrationBatch{
		Compaction: true,
		JobID:      "job-12345",
		Registrations: []RegistrationEntry{{
			Level:      23,
			TableID:    sst.SSTableID("sometableid1"),
			MaxVersion: 100001,
			KeyStart:   []byte("keystart1"),
			KeyEnd:     []byte("keyend2"),
		}, {
			Level:      12,
			TableID:    sst.SSTableID("sometableid2"),
			MaxVersion: 200002,
			KeyStart:   []byte("keystart1"),
			KeyEnd:     []byte("keyend2"),
		}},
		DeRegistrations: []RegistrationEntry{{
			Level:    23,
			TableID:  sst.SSTableID("sometableid11"),
			KeyStart: []byte("keystart11"),
			KeyEnd:   []byte("keyend12"),
		}, {
			Level:    27,
			TableID:  sst.SSTableID("sometableid12"),
			KeyStart: []byte("keystart21"),
			KeyEnd:   []byte("keyend22"),
		}},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = regBatch.Serialize(buff)

	regBatchAfter := &RegistrationBatch{}
	regBatchAfter.Deserialize(buff, 3)

	require.Equal(t, regBatch, regBatchAfter)
}

func TestSerializeDeserializeTableEntry(t *testing.T) {
	te := &TableEntry{
		SSTableID:        []byte("sstableid1"),
		RangeStart:       []byte("rangestart1"),
		RangeEnd:         []byte("rangeend1"),
		MinVersion:       7653595,
		MaxVersion:       7654321,
		DeleteRatio:      1234.23,
		AddedTime:        uint64(time.Now().UnixMilli()),
		NumEntries:       47464,
		Size:             696686,
		NumPrefixDeletes: 36363,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = te.serialize(buff)

	teAfter := &TableEntry{}
	teAfter.deserialize(buff, 3)

	require.Equal(t, te, teAfter)
}

func TestSerializeDeserializeTableEntryWithDeadVersionRanges(t *testing.T) {
	te := &TableEntry{
		SSTableID:  []byte("sstableid1"),
		RangeStart: []byte("rangestart1"),
		RangeEnd:   []byte("rangeend1"),
		MinVersion: 7653595,
		MaxVersion: 7654321,
		NumEntries: 47464,
		Size:       696686,
		DeadVersionRanges: []VersionRange{
			{VersionStart: 111, VersionEnd: 222},
			{VersionStart: 333, VersionEnd: 777},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = te.serialize(buff)

	teAfter := &TableEntry{}
	teAfter.deserialize(buff, 3)

	require.Equal(t, te, teAfter)
}

func serializeLevelEntryWithBuffSizeCheck(t *testing.T, le *levelEntry) []byte {
	bse := le.serializedSize()
	buff := le.Serialize(nil)
	require.Equal(t, bse, len(buff))
	return buff
}

func TestSerializeDeserializeLevelEntry(t *testing.T) {
	le1 := levelEntry{}
	numTableEntries := 10
	var expectedEntries []*TableEntry
	for i := 0; i < numTableEntries; i++ {
		te := createTabEntry(i)
		expectedEntries = append(expectedEntries, te)
		le1.InsertAt(i, te)
	}

	// Verify table entries before Serialize
	verifyTableEntries(t, expectedEntries, &le1)

	buff := serializeLevelEntryWithBuffSizeCheck(t, &le1)

	// Verify after Serialize (levelEntry internal state changes after serialization)
	verifyTableEntries(t, expectedEntries, &le1)

	// Deserialize
	le2 := levelEntry{}
	le2.Deserialize(buff, 0)

	// Verify same after deserialization
	require.Equal(t, le1.rangeStart, le2.rangeStart)
	require.Equal(t, le1.rangeEnd, le2.rangeEnd)
	require.Equal(t, le1.maxVersion, le2.maxVersion)
	verifyTableEntries(t, expectedEntries, &le2)

	// Now insert some more
	te10 := createTabEntry(10)
	le2.InsertAt(3, te10)
	expectedEntries = insertInSlice(expectedEntries, 3, te10)

	te11 := createTabEntry(11)
	le2.InsertAt(7, te11)
	expectedEntries = insertInSlice(expectedEntries, 7, te11)

	verifyTableEntries(t, expectedEntries, &le2)

	// Now delete a couple
	le2.RemoveAt(5)
	expectedEntries = append(expectedEntries[:5], expectedEntries[6:]...)

	// Remove the last one
	le2.RemoveAt(10)
	expectedEntries = expectedEntries[:10]

	verifyTableEntries(t, expectedEntries, &le2)

	// Serialize again
	buff = serializeLevelEntryWithBuffSizeCheck(t, &le2)
	// Verify after Serialize
	verifyTableEntries(t, expectedEntries, &le2)

	// Deserialize
	le3 := levelEntry{}
	le3.Deserialize(buff, 0)

	// Verify same after deserialization
	require.Equal(t, le1.rangeStart, le3.rangeStart)
	require.Equal(t, le1.rangeEnd, le3.rangeEnd)
	require.Equal(t, le1.maxVersion, le3.maxVersion)
	verifyTableEntries(t, expectedEntries, &le3)

	// Serialize/Deserialize again with no changes
	buff = serializeLevelEntryWithBuffSizeCheck(t, &le3)
	// Verify after Serialize
	verifyTableEntries(t, expectedEntries, &le2)

	le4 := levelEntry{}
	le4.Deserialize(buff, 0)

	// Verify same after deserialization
	require.Equal(t, le1.rangeStart, le4.rangeStart)
	require.Equal(t, le1.rangeEnd, le4.rangeEnd)
	require.Equal(t, le1.maxVersion, le4.maxVersion)
	verifyTableEntries(t, expectedEntries, &le4)
}

// TestSerializeDeserializeLevelEntryRandomUpdates does a bunch of random inserts and deletes and serializes/deserializes
// making sure state is correctly preserved
func TestSerializeDeserializeLevelEntryRandomUpdates(t *testing.T) {
	le := levelEntry{}
	numTableEntries := 10
	var expectedEntries []*TableEntry
	for i := 0; i < numTableEntries; i++ {
		te := createTabEntry(i)
		expectedEntries = append(expectedEntries, te)
		le.InsertAt(i, te)
	}

	seq := numTableEntries
	iters := 100
	updates := 4
	for i := 0; i < iters; i++ {
		for j := 0; j < updates; j++ {
			r := rand.Intn(3)
			if len(expectedEntries) == 0 || r == 0 {
				// insert
				te := createTabEntry(seq)
				seq++
				index := rand.Intn(len(expectedEntries) + 1)
				le.InsertAt(index, te)
				expectedEntries = insertInSlice(expectedEntries, index, te)
			} else if r == 1 {
				// Set
				te := createTabEntry(seq)
				seq++
				index := rand.Intn(len(expectedEntries))
				le.SetAt(index, te)
				expectedEntries[index] = te
			} else {
				// Remove
				index := rand.Intn(len(expectedEntries))
				le.RemoveAt(index)
				expectedEntries = append(expectedEntries[:index], expectedEntries[index+1:]...)
			}
		}
		buff := serializeLevelEntryWithBuffSizeCheck(t, &le)
		verifyTableEntries(t, expectedEntries, &le)

		le2 := levelEntry{}
		le2.Deserialize(buff, 0)
		require.Equal(t, le.rangeStart, le2.rangeStart)
		require.Equal(t, le.rangeEnd, le2.rangeEnd)
		require.Equal(t, le.maxVersion, le2.maxVersion)
		verifyTableEntries(t, expectedEntries, &le2)

		le = le2
	}
}

func createTabEntry(i int) *TableEntry {
	return &TableEntry{
		SSTableID:  []byte(fmt.Sprintf("sstableid%d", i)),
		RangeStart: []byte(fmt.Sprintf("rangestart%d", i)),
		RangeEnd:   []byte(fmt.Sprintf("rangeend%d", i)),
		MinVersion: uint64(7653595 + i),
		MaxVersion: uint64(7654321 + i),
		NumEntries: uint64(47464 + i),
		Size:       uint64(696686 + i),
		AddedTime:  uint64(time.Now().UnixMilli()),
		DeadVersionRanges: []VersionRange{
			{VersionStart: uint64(111 + i), VersionEnd: uint64(222 + i)},
			{VersionStart: uint64(333 + i), VersionEnd: uint64(777 + i)},
		},
	}
}

func verifyTableEntries(t *testing.T, tableEntries []*TableEntry, levEntry *levelEntry) {
	for i, te := range tableEntries {
		lte := levEntry.tableEntries[i]
		te2 := lte.Get(levEntry)
		require.Equal(t, te, te2)
	}
}

func TestSerializeDeserializeMasterRecord(t *testing.T) {
	// Note we test the level entries elsewhere
	mr := &MasterRecord{
		format:  21,
		version: 12345,
		levelTableCounts: map[int]int{
			0: 2, 1: 2,
		},
		slabRetentions: map[uint64]uint64{
			10: 1000,
			11: 2000,
			12: 3000,
		},
		lastFlushedVersion: 1234,
		stats: &Stats{
			TotBytes:   23232,
			TotEntries: 2132,
			TotTables:  43,
			BytesIn:    67657,
			EntriesIn:  453,
			TablesIn:   23,
			LevelStats: map[int]*LevelStats{
				0: {
					Bytes:   4565,
					Entries: 454,
					Tables:  67,
				},
				1: {
					Bytes:   5756,
					Entries: 343,
					Tables:  66,
				},
				2: {
					Bytes:   456456,
					Entries: 456,
					Tables:  234234,
				},
			},
		},
	}
	buffSize := mr.SerializedSize()
	buff := mr.Serialize(nil)
	require.Equal(t, buffSize, len(buff))

	mrAfter := &MasterRecord{}
	mrAfter.Deserialize(buff, 0)
	mrAfter.levelEntries = nil

	require.Equal(t, mr, mrAfter)
}
