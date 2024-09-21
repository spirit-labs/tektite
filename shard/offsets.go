package shard

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

/*
OffsetsCache caches next available offset for a topic partition in memory. Before an agent can write topic data to object storage
it must first obtain partition offsets for the data it's writing. It does this by requesting the shard controller for a
number of offsets for each partition that's being written.
The actual last stored offset is stored by the agent in the same batch that the topic data resides in. When the
OffsetCache loads it loads the actual last offset from there.
*/
type OffsetsCache struct {
	lock                  sync.RWMutex
	started               bool
	shardID               int
	topicOffsets          map[int][]int64
	topicInfoProvider     topicInfoProvider
	partitionOffsetLoader partitionOffsetLoader
}

func NewOffsetsCache(shardID int, provider topicInfoProvider, loader partitionOffsetLoader) *OffsetsCache {
	return &OffsetsCache{
		shardID:               shardID,
		topicInfoProvider:     provider,
		partitionOffsetLoader: loader,
		topicOffsets:          make(map[int][]int64),
	}
}

type GetOffsetInfo struct {
	TopicID     int
	PartitionID int
	NumOffsets  int
}

type TopicInfo struct {
	TopicID        int
	PartitionCount int
}

type topicInfoProvider interface {
	GetTopicsForShard(shardID int) ([]TopicInfo, error)
}

type partitionOffsetLoader interface {
	LoadOffsetsForShard(shardID int) ([]StoredOffset, error)
}

type StoredOffset struct {
	topicID     int
	partitionID int
	offset      int64
}

func (o *OffsetsCache) Start() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.started {
		return nil
	}
	topicInfos, err := o.topicInfoProvider.GetTopicsForShard(o.shardID)
	if err != nil {
		return err
	}
	for _, topicInfo := range topicInfos {
		o.topicOffsets[topicInfo.TopicID] = make([]int64, topicInfo.PartitionCount)
	}

	offsets, err := o.partitionOffsetLoader.LoadOffsetsForShard(o.shardID)
	if err != nil {
		return err
	}
	for _, offset := range offsets {
		offsets, ok := o.topicOffsets[offset.topicID]
		if !ok {
			return errors.Errorf("unknown topic id: %d", offset.topicID)
		}
		if offset.partitionID >= len(offsets) {
			return errors.Errorf("partition offset out of range: %d", offset.partitionID)
		}
		offsets[offset.partitionID] = offset.offset + 1
	}
	o.started = true
	return nil
}

// GetOffsets returns an offset for each of the provider GetOffsetInfo instances
func (o *OffsetsCache) GetOffsets(infos []GetOffsetInfo) ([]int64, error) {
	if len(infos) == 0 {
		return nil, errors.New("empty infos")
	}
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return nil, errors.New("not started")
	}
	res := make([]int64, len(infos))
	for i, id := range infos {
		off, err := o.getOffset(id)
		if err != nil {
			return nil, err
		}
		res[i] = off
	}
	return res, nil
}

func (o *OffsetsCache) getOffset(info GetOffsetInfo) (int64, error) {
	if info.NumOffsets < 1 {
		// OK to panic as would be programming error
		panic(fmt.Sprintf("invalid value for NumOffsets: %d", info.NumOffsets))
	}
	offsets, ok := o.topicOffsets[info.TopicID]
	if !ok {
		return 0, errors.Errorf("unknown topic id: %d", info.TopicID)
	}
	numOffsets := int64(info.NumOffsets)
	// We increment the next offset count and return the previous offset
	offset := atomic.AddInt64(&offsets[info.PartitionID], numOffsets) - numOffsets
	return offset, nil
}
