package offsets

import (
	"container/heap"
	"errors"
	"github.com/spirit-labs/tektite/streammeta"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestOffsetsCacheNotStarted(t *testing.T) {
	topicProvider := &streammeta.SimpleTopicInfoProvider{}
	partitionLoader := &testPartitionOffsetLoader{}

	oc := NewOffsetsCache(topicProvider, partitionLoader)
	_, err := oc.GetOffsets([]GetOffsetTopicInfo{{NumOffsets: 10}})
	require.Error(t, err)
	require.Equal(t, "not started", err.Error())
}

// Get an offset with a previous stored non-zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetNonZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offs, err := oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 1, int(offs[0]))

	offs, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offs))
	require.Equal(t, 1+100, int(offs[0]))

	offs, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  33,
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
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1+100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  33,
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
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 0, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  33,
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
			TopicID:     7,
			PartitionID: 0,
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  200,
		},
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  300,
		},
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  400,
		},
		{
			TopicID:     8,
			PartitionID: 0,
			NumOffsets:  150,
		},
		{
			TopicID:     8,
			PartitionID: 1,
			NumOffsets:  250,
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
			TopicID:     7,
			PartitionID: 0,
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  200,
		},
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  300,
		},
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  400,
		},
		{
			TopicID:     8,
			PartitionID: 0,
			NumOffsets:  150,
		},
		{
			TopicID:     8,
			PartitionID: 1,
			NumOffsets:  250,
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
			TopicID:     2323,
			PartitionID: 0,
			NumOffsets:  1,
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
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(offs))
	require.Equal(t, 3456+1, int(offs[0]))

	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			OffsetStart: 3456 + 1,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)

	off, err := oc.GetHighestReadableOffset(7, 1)
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
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  numOffsets,
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
		offset += int(info.NumOffsets)
	}

	var writtenOffsets []UpdateWrittenOffsetInfo
	for i := 0; i < numInfos; i++ {
		writtenOffsets = append(writtenOffsets, UpdateWrittenOffsetInfo{
			TopicID:     7,
			PartitionID: 1,
			OffsetStart: offs[i],
			NumOffsets:  infos[i].NumOffsets,
		})
	}

	if !ordered {
		// Shuffle them
		rand.Shuffle(numInfos, func(i, j int) {
			writtenOffsets[i], writtenOffsets[j] = writtenOffsets[j], writtenOffsets[i]
		})
	}

	err = oc.UpdateWrittenOffsets(writtenOffsets)
	require.NoError(t, err)

	off, err := oc.GetHighestReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, offs[numInfos-1]+int64(infos[numInfos-1].NumOffsets)-1, off)
}

func TestMembershipChanged(t *testing.T) {
	oc := setupAndStartCache(t)

	infos := []GetOffsetTopicInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  100,
		},
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  150,
		},
		{
			TopicID:     8,
			PartitionID: 1,
			NumOffsets:  200,
		},
	}
	offs, err := oc.GetOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// We haven't updated written offsets yet so should be last written
	highestReadable, err := oc.GetHighestReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, 3456, int(highestReadable))

	highestReadable, err = oc.GetHighestReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, 0, int(highestReadable))

	highestReadable, err = oc.GetHighestReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, 3456, int(highestReadable))

	// Membership change should trigger reset of highestReadable to be nextOffset - 1
	oc.MembershipChanged()

	highestReadable, err = oc.GetHighestReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, 3456+100, int(highestReadable))

	highestReadable, err = oc.GetHighestReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, 0+150, int(highestReadable))

	highestReadable, err = oc.GetHighestReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, 3456+200, int(highestReadable))

	// Now any attempt to updateWrittenOffsets below this should fail
	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			OffsetStart: offs[0],
			NumOffsets:  100,
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			OffsetStart: offs[1],
			NumOffsets:  150,
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     8,
			PartitionID: 1,
			OffsetStart: offs[2],
			NumOffsets:  200,
		},
	})
	require.Error(t, err)
	require.Equal(t, "Cannot update written offsets - membership change has occurred", err.Error())

	// Get new offsets
	offs, err = oc.GetOffsets(infos)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(offs))

	// Updating written offsets should now work
	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			OffsetStart: offs[0],
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)

	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			OffsetStart: offs[1],
			NumOffsets:  150,
		},
	})
	require.NoError(t, err)

	err = oc.UpdateWrittenOffsets([]UpdateWrittenOffsetInfo{
		{
			TopicID:     8,
			PartitionID: 1,
			OffsetStart: offs[2],
			NumOffsets:  200,
		},
	})
	require.NoError(t, err)

	highestReadable, err = oc.GetHighestReadableOffset(7, 1)
	require.NoError(t, err)
	require.Equal(t, offs[0]+100-1, highestReadable)

	highestReadable, err = oc.GetHighestReadableOffset(7, 2)
	require.NoError(t, err)
	require.Equal(t, offs[1]+150-1, highestReadable)

	highestReadable, err = oc.GetHighestReadableOffset(8, 1)
	require.NoError(t, err)
	require.Equal(t, offs[2]+200-1, highestReadable)
}

var testTopicProvider = &streammeta.SimpleTopicInfoProvider{
	Infos: map[string]streammeta.TopicInfo{
		"topic1": {
			TopicID:        7,
			PartitionCount: 4,
		},
		"topic2": {
			TopicID:        8,
			PartitionCount: 2,
		},
	},
}

var testOffsetLoader = &testPartitionOffsetLoader{
	topicOffsets: map[int]map[int]int64{
		7: {
			0: 1234,
			1: 3456,
			2: 0,
			3: -1,
		},
		8: {
			0: 5678,
			1: 3456,
		},
	},
}

func setupAndStartCache(t *testing.T) *Cache {
	oc := NewOffsetsCache(testTopicProvider, testOffsetLoader)
	err := oc.Start()
	require.NoError(t, err)
	return oc
}

type testPartitionOffsetLoader struct {
	topicOffsets map[int]map[int]int64
}

func (t *testPartitionOffsetLoader) LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error) {
	to, ok := t.topicOffsets[topicID]
	if !ok {
		return 0, errors.New("topic not found")
	}
	po, ok := to[partitionID]
	if !ok {
		return 0, errors.New("partition not found")
	}
	return po, nil
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
