package offsets

import (
	"bytes"
	"container/heap"
	"context"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
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
	_, err := oc.GetOffsets([]GetOffsetTopicInfo{{TopicID: 0, PartitionInfos: []GetOffsetPartitionInfo{{NumOffsets: 100}}}})
	require.Error(t, err)
	require.Equal(t, "offsets cache not started", err.Error())
}

// Get an offset with a previous stored non-zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetNonZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offs, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 1, int(offs[0]))

	offs, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 1+100, int(offs[0]))

	offs, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 1+100+33, int(offs[0]))
}

// Get an offset with a previous stored zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1+100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1+100+33, int(offsets[0]))
}

// Get an offset with a previous unstored value
func TestOffsetsCacheGetSingleNotStored(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 0, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  33,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 100+33, int(offsets[0]))
}

func TestOffsetsCacheGetMultiple(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  100,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  300,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  400,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  150,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  250,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 6, len(offsets))

	require.Equal(t, 1234+1, int(offsets[0]))
	require.Equal(t, 3456+1, int(offsets[1]))
	require.Equal(t, 0+1, int(offsets[2]))
	require.Equal(t, 0, int(offsets[3]))
	require.Equal(t, 5678+1, int(offsets[4]))
	require.Equal(t, 3456+1, int(offsets[5]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  100,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  300,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 3,
					NumOffsets:  400,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  150,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  250,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 6, len(offsets))

	require.Equal(t, 1234+1+100, int(offsets[0]))
	require.Equal(t, 3456+1+200, int(offsets[1]))
	require.Equal(t, 0+1+300, int(offsets[2]))
	require.Equal(t, 0+400, int(offsets[3]))
	require.Equal(t, 5678+1+150, int(offsets[4]))
	require.Equal(t, 3456+1+250, int(offsets[5]))
}

func TestOffsetsCacheUnknownTopicID(t *testing.T) {
	oc := setupAndStartCache(t)

	_, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 2323,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 0,
					NumOffsets:  1,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, "unknown topic id: 2323", err.Error())
}

func TestOffsetsCacheEmptyInfos(t *testing.T) {
	oc := setupAndStartCache(t)

	_, err := oc.GetOffsets(nil)
	require.Error(t, err)
	require.Equal(t, "empty infos", err.Error())
}

func TestOffsetsCacheUpdateWrittenOffsetsSimple(t *testing.T) {
	oc := setupAndStartCache(t)
	offs, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
			},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(offs))
	require.Equal(t, 3456+1, int(offs[0]))

	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 7,

			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: 3456 + 1,
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)

	off, err := oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, 3457+99, int(off))
}

func TestOffsetsCacheUpdateMultipleWrittenOffsetsOrdered(t *testing.T) {
	testOffsetsCacheUpdateMultipleWrittenOffsets(t, true)
}

func TestOffsetsCacheUpdateMultipleWrittenOffsetsUnordered(t *testing.T) {
	testOffsetsCacheUpdateMultipleWrittenOffsets(t, false)
}

func testOffsetsCacheUpdateMultipleWrittenOffsets(t *testing.T, ordered bool) {
	oc := setupAndStartCache(t)

	numInfos := 5
	var infos []GetOffsetTopicInfo
	for i := 0; i < numInfos; i++ {
		numOffsets := 1 + rand.Intn(100)
		infos = append(infos, GetOffsetTopicInfo{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  numOffsets,
				},
			},
		})
	}

	offs, err := oc.GetOffsets(infos)
	require.NoError(t, err)

	require.Equal(t, numInfos, len(offs))
	offset := 3456 + 1
	for i := 0; i < numInfos; i++ {
		off := offs[i]
		require.Equal(t, offset, int(off))
		info := infos[i]
		offset += info.PartitionInfos[0].NumOffsets
	}

	var writtenOffsets []UpdateWrittenOffsetTopicInfo
	for i := 0; i < numInfos; i++ {
		writtenOffsets = append(writtenOffsets, UpdateWrittenOffsetTopicInfo{
			TopicID: 7,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[i],
					NumOffsets:  infos[i].PartitionInfos[0].NumOffsets,
				},
			},
		})
	}

	if !ordered {
		// Shuffle them
		rand.Shuffle(numInfos, func(i, j int) {
			writtenOffsets[i], writtenOffsets[j] = writtenOffsets[j], writtenOffsets[i]
		})
	}

	_, err = oc.UpdateWrittenOffsets(writtenOffsets)
	require.NoError(t, err)

	off, err := oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, offs[numInfos-1]+int64(infos[numInfos-1].PartitionInfos[0].NumOffsets)-1, off)
}

func TestMembershipChanged(t *testing.T) {
	oc := setupAndStartCache(t)

	infos := []GetOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  100,
				},
			},
		},
		{
			TopicID: 7,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 2,
					NumOffsets:  150,
				},
			},
		},
		{
			TopicID: 8,
			PartitionInfos: []GetOffsetPartitionInfo{
				{
					PartitionID: 1,
					NumOffsets:  200,
				},
			},
		},
	}
	offs, err := oc.GetOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// We haven't updated written offsets yet so should be last written
	highestReadable, err := oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, 3456, int(highestReadable))

	highestReadable, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, 0, int(highestReadable))

	highestReadable, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, 3456, int(highestReadable))

	// Membership change should trigger reset of highestReadable to be nextOffset - 1
	oc.MembershipChanged()

	highestReadable, err = oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, 3456+100, int(highestReadable))

	highestReadable, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, 0+150, int(highestReadable))

	highestReadable, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, 3456+200, int(highestReadable))

	// Now any attempt to updateWrittenOffsets below this should fail
	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[0],
					NumOffsets:  100,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 2,
					OffsetStart: offs[1],
					NumOffsets:  150,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 8,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[2],
					NumOffsets:  200,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	// Get new offsets
	offs, err = oc.GetOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// Updating written offsets should now work
	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[0],
					NumOffsets:  100,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 7,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 2,
					OffsetStart: offs[1],
					NumOffsets:  150,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetTopicInfo{
		{
			TopicID: 8,
			PartitionInfos: []UpdateWrittenOffsetPartitionInfo{
				{
					PartitionID: 1,
					OffsetStart: offs[2],
					NumOffsets:  200,
				},
			},
		},
	})
	require.NoError(t, err)

	highestReadable, err = oc.GetLastReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, offs[0]+100-1, highestReadable)

	highestReadable, err = oc.GetLastReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, offs[1]+150-1, highestReadable)

	highestReadable, err = oc.GetLastReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, offs[2]+200-1, highestReadable)
}

type testTopicMetaProvider struct {
	infos map[int]topicmeta.TopicInfo
}

func (t *testTopicMetaProvider) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, error) {
	info, ok := t.infos[topicID]
	if !ok {
		return topicmeta.TopicInfo{}, common.NewTektiteErrorf(common.TopicDoesNotExist, "unknown topic id: %d", topicID)
	}
	return info, nil
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

func (t *testLsmHolder) GetTablesForHighestKeyWithPrefix(prefix []byte) ([]sst.SSTableID, error) {
	return []sst.SSTableID{t.tableID}, nil
}

func createDataEntry(t *testing.T, topicID int, partitionID int, offset int) common.KV {
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	prefix, err := partHashes.GetPartitionHash(topicID, partitionID)
	require.NoError(t, err)
	key := encoding.KeyEncodeInt(prefix, int64(offset))
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
	tableData := table.Serialize()
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
	numWrittenOffsets := 100
	var wos []writtenOffset
	offset := 0
	for i := 0; i < numWrittenOffsets; i++ {
		numOffsets := 1 + rand.Intn(1000)
		wos = append(wos, writtenOffset{
			offsetStart: int64(offset),
			numOffsets:  int32(numOffsets),
		})
		offset += numOffsets
	}
	// Shuffle them
	rand.Shuffle(numWrittenOffsets, func(i, j int) {
		wos[i], wos[j] = wos[j], wos[i]
	})
	// Apply them to the heap
	var woh writtenOffsetHeap
	for _, wo := range wos {
		heap.Push(&woh, wo)
	}
	// Now they should be peekable and poppable in order
	offset = 0
	for i := 0; i < numWrittenOffsets; i++ {
		peeked := woh.Peek()
		require.Equal(t, offset, int(peeked.offsetStart))
		require.Greater(t, peeked.numOffsets, int32(0))
		popped := heap.Pop(&woh).(writtenOffset)
		require.Equal(t, peeked, popped)
		offset += int(peeked.numOffsets)
	}
}
