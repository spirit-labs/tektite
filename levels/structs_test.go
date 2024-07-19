package levels

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserializeOverlappingTableIDs(t *testing.T) {
	var otids OverlappingTables
	for i := 0; i < 10; i++ {
		var notids []QueryTableInfo
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
			notids = append(notids, QueryTableInfo{
				ID:           bytes,
				DeadVersions: dvs,
			})
		}
		otids = append(otids, notids)
	}

	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = otids.Serialize(buff)

	otidsAfter := DeserializeOverlappingTables(buff, 3)
	require.Equal(t, otids, otidsAfter)
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
	buff = regEntry.serialize(buff)

	regEntryAfter := &RegistrationEntry{}
	regEntryAfter.deserialize(buff, 3)

	require.Equal(t, regEntry, regEntryAfter)
}

func TestSerializeDeserializeRegistrationBatch(t *testing.T) {
	regBatch := &RegistrationBatch{
		ClusterName:    "test_cluster",
		ClusterVersion: 23,
		Compaction:     true,
		JobID:          "job-12345",
		ProcessorID:    534343,
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
		SSTableID:  []byte("sstableid1"),
		RangeStart: []byte("rangestart1"),
		RangeEnd:   []byte("rangeend1"),
		MinVersion: 7653595,
		MaxVersion: 7654321,
		NumEntries: 47464,
		Size:       696686,
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

func TestSerializeDeserializeSegment(t *testing.T) {
	seg := &segment{
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
	buff := seg.serialize(nil)

	segAfter := &segment{}
	segAfter.deserialize(buff)

	require.Equal(t, seg, segAfter)
}

func TestSerializeDeserializeSegmentEntry(t *testing.T) {
	se := &segmentEntry{
		format:     76,
		segmentID:  []byte("segmentid1"),
		rangeStart: []byte("rangestart1"),
		rangeEnd:   []byte("rangeend1"),
	}
	buff := se.serialize(nil)

	seAfter := &segmentEntry{}
	seAfter.deserialize(buff, 0)

	require.Equal(t, se, seAfter)
}

func TestSerializeDeserializeMasterRecord(t *testing.T) {
	mr := &masterRecord{
		format:  21,
		version: 12345,
		levelTableCounts: map[int]int{
			0: 2, 1: 2,
		},
		levelSegmentEntries: []levelEntries{
			{
				maxVersion: 23,
				segmentEntries: []segmentEntry{{
					format:     76,
					segmentID:  []byte("segmentid1"),
					rangeStart: []byte("rangestart1"),
					rangeEnd:   []byte("rangeend1"),
				},
					{
						format:     34,
						segmentID:  []byte("segmentid2"),
						rangeStart: []byte("rangestart2"),
						rangeEnd:   []byte("rangeend2"),
					},
				}},

			{
				maxVersion: 31,
				segmentEntries: []segmentEntry{{
					format:     76,
					segmentID:  []byte("segmentid1"),
					rangeStart: []byte("rangestart1"),
					rangeEnd:   []byte("rangeend1"),
				},
					{
						format:     34,
						segmentID:  []byte("segmentid2"),
						rangeStart: []byte("rangestart2"),
						rangeEnd:   []byte("rangeend2"),
					},
				}},

			{
				maxVersion: 31,
				segmentEntries: []segmentEntry{{
					format:     23,
					segmentID:  []byte("segmentid3"),
					rangeStart: []byte("rangestart3"),
					rangeEnd:   []byte("rangeend3"),
				},
					{
						format:     87,
						segmentID:  []byte("segmentid4"),
						rangeStart: []byte("rangestart4"),
						rangeEnd:   []byte("rangeend4"),
					}},
			},
		},
		slabRetentions: map[uint64]uint64{
			10: 1000,
			11: 2000,
			12: 3000,
		},
		lastFlushedVersion:   1234,
		lastProcessedReplSeq: 5432,
		stats: &Stats{
			TotBytes:       23232,
			TotEntries:     2132,
			TotTables:      43,
			BytesIn:        67657,
			EntriesIn:      453,
			TablesIn:       23,
			TotCompactions: 45,
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
			},
		},
	}
	buff := mr.serialize(nil)

	mrAfter := &masterRecord{}
	mrAfter.deserialize(buff, 0)

	require.Equal(t, mr, mrAfter)
}
