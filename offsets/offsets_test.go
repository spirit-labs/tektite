package offsets

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"testing"
)

func TestOffsetsCacheNotStarted(t *testing.T) {
	oc := setupCache(t)
	_, _, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{{TopicID: 0, PartitionInfos: []GenerateOffsetPartitionInfo{{NumOffsets: 100}}}})
	require.Error(t, err)
	require.Equal(t, "offsets cache not started", err.Error())
}

// Get an offset with a previously stored non-zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetNonZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offs, seq, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 100, int(offs[0].PartitionInfos[0].Offset))

	require.Equal(t, 1, int(seq))

	offs, seq, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 100+33, int(offs[0].PartitionInfos[0].Offset))

	require.Equal(t, 2, int(seq))

	offs, seq, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 100+33+33, int(offs[0].PartitionInfos[0].Offset))

	require.Equal(t, 3, int(seq))
}

// Get an offset with a previous stored zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, _, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3556, int(offsets[0].PartitionInfos[0].Offset))

	offsets, _, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3556+33, int(offsets[0].PartitionInfos[0].Offset))

	offsets, _, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3556+33+33, int(offsets[0].PartitionInfos[0].Offset))
}

// Get an offset with a previous unstored value
func TestOffsetsCacheGetSingleNotStored(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, _, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 99, int(offsets[0].PartitionInfos[0].Offset))

	offsets, _, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 99+33, int(offsets[0].PartitionInfos[0].Offset))

	offsets, _, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 99+33+33, int(offsets[0].PartitionInfos[0].Offset))
}

func TestOffsetsCacheGetMultiple(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, _, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  100,
				},
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
				{
					PartitionID: 2,
					NumOffsets:  300,
				},
				{
					PartitionID: 3,
					NumOffsets:  400,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  150,
				},
				{
					PartitionID: 1,
					NumOffsets:  250,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(offsets))
	require.Equal(t, 7, offsets[0].TopicID)
	require.Equal(t, 4, len(offsets[0].PartitionInfos))

	require.Equal(t, 0, offsets[0].PartitionInfos[0].PartitionID)
	require.Equal(t, 1234+100, int(offsets[0].PartitionInfos[0].Offset))

	require.Equal(t, 1, offsets[0].PartitionInfos[1].PartitionID)
	require.Equal(t, 3456+200, int(offsets[0].PartitionInfos[1].Offset))

	require.Equal(t, 2, offsets[0].PartitionInfos[2].PartitionID)
	require.Equal(t, 300, int(offsets[0].PartitionInfos[2].Offset))

	require.Equal(t, 3, offsets[0].PartitionInfos[3].PartitionID)
	require.Equal(t, 399, int(offsets[0].PartitionInfos[3].Offset))

	require.Equal(t, 8, offsets[1].TopicID)
	require.Equal(t, 2, len(offsets[1].PartitionInfos))

	require.Equal(t, 0, offsets[1].PartitionInfos[0].PartitionID)
	require.Equal(t, 5678+150, int(offsets[1].PartitionInfos[0].Offset))

	require.Equal(t, 1, offsets[1].PartitionInfos[1].PartitionID)
	require.Equal(t, 3456+250, int(offsets[1].PartitionInfos[1].Offset))

	offsets, _, err = oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  110,
				},
				{
					PartitionID: 1,
					NumOffsets:  220,
				},
				{
					PartitionID: 2,
					NumOffsets:  330,
				},
				{
					PartitionID: 3,
					NumOffsets:  440,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  155,
				},
				{
					PartitionID: 1,
					NumOffsets:  255,
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, 2, len(offsets))
	require.Equal(t, 7, offsets[0].TopicID)
	require.Equal(t, 4, len(offsets[0].PartitionInfos))

	require.Equal(t, 0, offsets[0].PartitionInfos[0].PartitionID)
	require.Equal(t, 1234+100+110, int(offsets[0].PartitionInfos[0].Offset))

	require.Equal(t, 1, offsets[0].PartitionInfos[1].PartitionID)
	require.Equal(t, 3456+200+220, int(offsets[0].PartitionInfos[1].Offset))

	require.Equal(t, 2, offsets[0].PartitionInfos[2].PartitionID)
	require.Equal(t, 300+330, int(offsets[0].PartitionInfos[2].Offset))

	require.Equal(t, 3, offsets[0].PartitionInfos[3].PartitionID)
	require.Equal(t, 399+440, int(offsets[0].PartitionInfos[3].Offset))

	require.Equal(t, 8, offsets[1].TopicID)
	require.Equal(t, 2, len(offsets[1].PartitionInfos))

	require.Equal(t, 0, offsets[1].PartitionInfos[0].PartitionID)
	require.Equal(t, 5678+150+155, int(offsets[1].PartitionInfos[0].Offset))

	require.Equal(t, 1, offsets[1].PartitionInfos[1].PartitionID)
	require.Equal(t, 3456+250+255, int(offsets[1].PartitionInfos[1].Offset))
}

func TestOffsetsCacheUnknownTopicID(t *testing.T) {
	oc := setupAndStartCache(t)

	_, _, err := oc.GenerateOffsets([]GenerateOffsetTopicInfo{
		{
			TopicID: 2323,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  1,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, "generate offsets: unknown topic: 2323", err.Error())
}

func TestMembershipChanged(t *testing.T) {
	oc := setupAndStartCache(t)

	infos := []GenerateOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
				{
					PartitionID: 2,
					NumOffsets:  150,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GenerateOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
			},
		},
	}
	offs, seq, err := oc.GenerateOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// We haven't updated written offsets yet so should be last written
	highestReadable, exists, err := oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 3456, int(highestReadable))

	highestReadable, exists, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 0, int(highestReadable))

	highestReadable, exists, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 3456, int(highestReadable))

	// Membership change should trigger reset of highestReadable to be nextOffset - 1
	oc.MembershipChanged()

	highestReadable, exists, err = oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 3456+100, int(highestReadable))

	highestReadable, exists, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 0+150, int(highestReadable))

	highestReadable, exists, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 3456+200, int(highestReadable))

	// Now any attempt to register with this sequence should not release anything

	tableID := sst.SSTableID(sst.CreateSSTableId())
	offs, tabID, err := oc.MaybeReleaseOffsets(seq, tableID)
	require.Error(t, err)
	require.Nil(t, offs)
	require.Nil(t, tabID)
	require.True(t, common.IsUnavailableError(err))

	// Get new offsets
	offs, seq, err = oc.GenerateOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// Register should now work
	tableID = sst.SSTableID(sst.CreateSSTableId())
	releasedOffs, tabIDs, err := oc.MaybeReleaseOffsets(seq, tableID)
	require.NoError(t, err)
	require.Equal(t, []sst.SSTableID{tableID}, tabIDs)
	require.Equal(t, offs, releasedOffs)

	// check that last readable was updated
	highestReadable, exists, err = oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, offs[0].PartitionInfos[0].Offset, highestReadable)

	highestReadable, exists, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, offs[0].PartitionInfos[1].Offset, highestReadable)

	highestReadable, exists, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, offs[1].PartitionInfos[0].Offset, highestReadable)
}

func TestGetLastReadableTopicDoesNotExist(t *testing.T) {
	oc := setupAndStartCache(t)
	highestReadable, exists, err := oc.GetLastReadableOffset(23, 1)
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, 0, int(highestReadable))
}

type testTopicMetaProvider struct {
	infos map[int]topicmeta.TopicInfo
}

func (t *testTopicMetaProvider) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error) {
	info, ok := t.infos[topicID]
	if !ok {
		return topicmeta.TopicInfo{}, false, nil
	}
	return info, true, nil
}

var testTopicProvider = &testTopicMetaProvider{
	infos: map[int]topicmeta.TopicInfo{
		7: {
			Name:           "topic1",
			ID:             7,
			PartitionCount: 4,
		},
		8: {
			Name:           "topic2",
			ID:             8,
			PartitionCount: 2,
		},
	},
}

type testLsmHolder struct {
	tableID sst.SSTableID
}

func (t *testLsmHolder) GetTablesForHighestKeyWithPrefix(_ []byte) ([]sst.SSTableID, error) {
	return []sst.SSTableID{t.tableID}, nil
}

func createDataEntry(t *testing.T, topicID int, partitionID int, offset int) common.KV {
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	prefix, err := partHashes.GetPartitionHash(topicID, partitionID)
	require.NoError(t, err)
	var key []byte
	key = append(key, prefix...)
	key = append(key, common.EntryTypeTopicData)
	key = encoding.KeyEncodeInt(key, int64(offset))
	key = encoding.EncodeVersion(key, 0)
	recordBatch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(offset, 1)
	return common.KV{
		Key:   key,
		Value: recordBatch,
	}
}

func setupInitialOffsets(t *testing.T, objStore objstore.Client, dataBucketName string) sst.SSTableID {
	var kvs []common.KV
	kvs = append(kvs, createDataEntry(t, 7, 0, 1234))
	kvs = append(kvs, createDataEntry(t, 7, 1, 3456))
	kvs = append(kvs, createDataEntry(t, 7, 2, 0))
	kvs = append(kvs, createDataEntry(t, 8, 0, 5678))
	kvs = append(kvs, createDataEntry(t, 8, 1, 3456))
	sort.SliceStable(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	iter := common.NewKvSliceIterator(kvs)
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1,
		0, 0, iter)
	require.NoError(t, err)
	tableID := sst.CreateSSTableId()
	// Push sstable to object store
	tableData, err := table.ToStorageBytes(compress.CompressionTypeNone)
	require.NoError(t, err)
	err = objStore.Put(context.Background(), dataBucketName, tableID, tableData)
	require.NoError(t, err)
	return []byte(tableID)
}

func setupCache(t *testing.T) *Cache {
	objStore := dev.NewInMemStore(0)
	bucketName := "test-bucket"
	tableID := setupInitialOffsets(t, objStore, bucketName)
	oc, err := NewOffsetsCache(testTopicProvider, &testLsmHolder{
		tableID: tableID,
	}, objStore, bucketName)
	require.NoError(t, err)
	return oc
}

func setupAndStartCache(t *testing.T) *Cache {
	oc := setupCache(t)
	err := oc.Start()
	require.NoError(t, err)
	return oc
}

func TestWrittenOffsetsHeap(t *testing.T) {
	// Create a bunch of written offsets
	numOffsets := 100
	var wos []seqHolder
	var tabIDs []sst.SSTableID
	for i := 0; i < numOffsets; i++ {
		tabID := sst.SSTableID(fmt.Sprintf("table-%d", i))
		tabIDs = append(tabIDs, tabID)
		wos = append(wos, seqHolder{
			seq:     int64(i),
			tableID: tabID,
		})
	}
	// Shuffle them
	rand.Shuffle(numOffsets, func(i, j int) {
		wos[i], wos[j] = wos[j], wos[i]
	})
	// Apply them to the heap
	var woh seqHeap
	for _, wo := range wos {
		heap.Push(&woh, wo)
	}
	// Now they should be peekable and poppable in order
	for i := 0; i < numOffsets; i++ {
		peeked := woh.Peek()
		require.Equal(t, i, int(peeked.seq))
		require.Equal(t, tabIDs[i], peeked.tableID)
		popped := heap.Pop(&woh).(seqHolder)
		require.Equal(t, peeked, popped)
	}
}

func TestMergeInfosNonOverlapping(t *testing.T) {

	infos1 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
			},
		},
	}

	infos2 := []OffsetTopicInfo{
		{
			TopicID: 35,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 45,
					Offset:      45564,
				},
			},
		},
	}

	res := mergeTopicInfos(infos1, infos2)

	expected := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
			},
		},
		{
			TopicID: 35,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 45,
					Offset:      45564,
				},
			},
		},
	}

	require.Equal(t, expected, res)

}

func TestMergeInfosSortTopics(t *testing.T) {

	infos1 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
			},
		},
		{
			TopicID: 34,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 34,
					Offset:      123,
				},
			},
		},
		{
			TopicID: 56,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 43,
					Offset:      3455,
				},
			},
		},
	}

	infos2 := []OffsetTopicInfo{
		{
			TopicID: 2,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 345,
					Offset:      34545,
				},
			},
		},
		{
			TopicID: 5,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 3454,
					Offset:      34535,
				},
			},
		},
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 345,
					Offset:      2344,
				},
			},
		},
		{
			TopicID: 27,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 34534,
					Offset:      345345,
				},
			},
		},
		{
			TopicID: 38,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 32423,
					Offset:      23423,
				},
			},
		},
		{
			TopicID: 72,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 3434,
					Offset:      2342,
				},
			},
		},
		{
			TopicID: 99,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 345,
					Offset:      345345,
				},
			},
		},
	}

	res := mergeTopicInfos(infos1, infos2)

	expected := []OffsetTopicInfo{
		{
			TopicID: 2,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 345,
					Offset:      34545,
				},
			},
		},
		{
			TopicID: 5,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 3454,
					Offset:      34535,
				},
			},
		},
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
				{
					PartitionID: 345,
					Offset:      2344,
				},
			},
		},
		{
			TopicID: 27,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 34534,
					Offset:      345345,
				},
			},
		},
		{
			TopicID: 34,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 34,
					Offset:      123,
				},
			},
		},
		{
			TopicID: 38,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 32423,
					Offset:      23423,
				},
			},
		},
		{
			TopicID: 56,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 43,
					Offset:      3455,
				},
			},
		},
		{
			TopicID: 72,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 3434,
					Offset:      2342,
				},
			},
		},
		{
			TopicID: 99,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 345,
					Offset:      345345,
				},
			},
		},
	}

	require.Equal(t, expected, res)

}

func TestMergeInfosSortPartitions(t *testing.T) {

	infos1 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
				{
					PartitionID: 34,
					Offset:      45456,
				},
				{
					PartitionID: 567,
					Offset:      5675677,
				},
			},
		},
	}

	infos2 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 21,
					Offset:      234324,
				},
				{
					PartitionID: 26,
					Offset:      4545,
				},
				{
					PartitionID: 37,
					Offset:      23234,
				},
				{
					PartitionID: 678,
					Offset:      34555,
				},
			},
		},
	}

	res := mergeTopicInfos(infos1, infos2)

	expected := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
				{
					PartitionID: 21,
					Offset:      234324,
				},
				{
					PartitionID: 26,
					Offset:      4545,
				},
				{
					PartitionID: 34,
					Offset:      45456,
				},
				{
					PartitionID: 37,
					Offset:      23234,
				},
				{
					PartitionID: 567,
					Offset:      5675677,
				},
				{
					PartitionID: 678,
					Offset:      34555,
				},
			},
		},
	}

	require.Equal(t, expected, res)

}

func TestMergeInfosTakeMaxOffset(t *testing.T) {

	infos1 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      23,
				},
				{
					PartitionID: 34,
					Offset:      56,
				},
				{
					PartitionID: 567,
					Offset:      2344,
				},
			},
		},
	}

	infos2 := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
				{
					PartitionID: 34,
					Offset:      45456,
				},
				{
					PartitionID: 567,
					Offset:      5675677,
				},
			},
		},
	}

	res := mergeTopicInfos(infos1, infos2)

	expected := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
				{
					PartitionID: 34,
					Offset:      45456,
				},
				{
					PartitionID: 567,
					Offset:      5675677,
				},
			},
		},
	}

	require.Equal(t, expected, res)

}

func TestMergeInfosIntoEmpty(t *testing.T) {

	infos := []OffsetTopicInfo{
		{
			TopicID: 23,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 12,
					Offset:      1234,
				},
			},
		},
		{
			TopicID: 34,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 34,
					Offset:      123,
				},
			},
		},
	}
	require.Equal(t, infos, mergeTopicInfos(infos, nil))
	require.Equal(t, infos, mergeTopicInfos(nil, infos))

}

func TestMaybeReleaseOffsetsInOrder(t *testing.T) {
	testMaybeReleaseOffsets(t, false)
}

func TestMaybeReleaseOffsetsUnordered(t *testing.T) {
	testMaybeReleaseOffsets(t, true)
}

func testMaybeReleaseOffsets(t *testing.T, shuffle bool) {
	oc, err := NewOffsetsCache(testTopicProvider, nil, nil, "")
	require.NoError(t, err)
	err = oc.Start()
	require.NoError(t, err)

	infos := []OffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []OffsetPartitionInfo{
				{
					PartitionID: 1,
					Offset:      234,
				},
			},
		},
	}
	oc.offsetsMap[1] = infos

	type tabEntry struct {
		sequence int64
		tableID  string
	}

	numTables := 1000
	var tabEntries []tabEntry
	for i := 0; i < numTables; i++ {
		tabID := sst.CreateSSTableId()
		seq := int64(i + 1)
		tabEntries = append(tabEntries, tabEntry{
			sequence: seq,
			tableID:  tabID,
		})
		oc.offsetsMap[seq] = infos
	}

	toSend := make([]tabEntry, numTables)
	copy(toSend, tabEntries)

	if shuffle {
		// shuffle them and test the sad (unordered) path
		rand.Shuffle(numTables, func(i, j int) {
			toSend[i], toSend[j] = toSend[j], toSend[i]
		})
	}

	var receivedTables []sst.SSTableID
	for _, entry := range toSend {
		_, tables, err := oc.MaybeReleaseOffsets(entry.sequence, sst.SSTableID(entry.tableID))
		require.NoError(t, err)
		receivedTables = append(receivedTables, tables...)
	}

	// Make sure they are released in order
	require.Equal(t, numTables, len(receivedTables))
	for i, entry := range tabEntries {
		require.Equal(t, entry.tableID, string(receivedTables[i]))
	}
}

func TestResizePartitionCount(t *testing.T) {
	oc := setupAndStartCache(t)

	for partitionID := 4; partitionID < 100; partitionID++ {
		_, _, err := oc.GetLastReadableOffset(7, partitionID)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.PartitionOutOfRange))
	}

	ok, err := oc.ResizePartitionCount(7, 100)
	require.NoError(t, err)
	require.True(t, ok)

	for partitionID := 4; partitionID < 100; partitionID++ {
		lro, ok, err := oc.GetLastReadableOffset(7, 50)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, -1, int(lro))
	}
}
