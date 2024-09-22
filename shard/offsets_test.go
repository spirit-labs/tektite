package shard

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOffsetsCacheNotStarted(t *testing.T) {
	topicProvider := &testTopicInfoProvider{}
	partitionLoader := &testPartitionOffsetLoader{}

	oc := NewOffsetsCache(23, topicProvider, partitionLoader)
	_, err := oc.GetOffsets([]GetOffsetInfo{{NumOffsets: 10}})
	require.Error(t, err)
	require.Equal(t, "not started", err.Error())
}

// Get an offset with a previous stored non zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetNonZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, err := oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 1, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 1+100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 2,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 1+100+33, int(offsets[0]))
}

// Get an offset with a previous stored zero value
func TestOffsetsCacheGetSingleAlreadyStoredOffsetZero(t *testing.T) {
	oc := setupAndStartCache(t)

	offsets, err := oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 1,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 3456+1+100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
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

	offsets, err := oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  100,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 0, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
		{
			TopicID:     7,
			PartitionID: 3,
			NumOffsets:  33,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(offsets))
	require.Equal(t, 100, int(offsets[0]))

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
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

	offsets, err := oc.GetOffsets([]GetOffsetInfo{
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

	offsets, err = oc.GetOffsets([]GetOffsetInfo{
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

	_, err := oc.GetOffsets([]GetOffsetInfo{
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

var testTopicProvider = &testTopicInfoProvider{
	infos: []TopicInfo{
		{
			TopicID:        7,
			PartitionCount: 4,
		},
		{
			TopicID:        8,
			PartitionCount: 2,
		},
	},
}

var testOffsetLoader = &testPartitionOffsetLoader{
	offsets: []StoredOffset{
		{
			topicID:     7,
			partitionID: 0,
			offset:      1234,
		},
		{
			topicID:     7,
			partitionID: 1,
			offset:      3456,
		},
		{
			topicID:     7,
			partitionID: 2,
			offset:      0,
		},
		{
			topicID:     7,
			partitionID: 3,
			offset:      -1,
		},
		{
			topicID:     8,
			partitionID: 0,
			offset:      5678,
		},
		{
			topicID:     8,
			partitionID: 1,
			offset:      3456,
		},
	},
}

func setupAndStartCache(t *testing.T) *OffsetsCache {
	oc := NewOffsetsCache(23, testTopicProvider, testOffsetLoader)
	err := oc.Start()
	require.NoError(t, err)
	return oc
}
