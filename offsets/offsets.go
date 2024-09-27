package offsets

import (
	"container/heap"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/streammeta"
	"sync"
)

/*
Cache caches next available offset for a topic partition in memory. Before an agent can write topic data to object storage
it must first obtain partition offsets for the data it's writing. It does this by requesting the offset cache for a
number of offsets for each partition that's being written.

The Cache also caches highestReadOffset for each partition. Any offsets greater than this might exist in storage but
cannot be read. As offsets are obtained before the SSTable is written to object storage and the SSTable is registered
with the LSM it's possible that two different agents who are writing for the same partition can register a SSTable
containing later offsets before earlier offsets. If we made all offsets immediately readable then a consumer could
read the later offsets (Kafka consumers tolerate gaps in offsets), thus skipping the data for the earlier offsets.
To prevent this behaviour, when an SStable is registered after pushing to object storage the caller also provides the
offsets for each partition in the table. We then add the offsets to a min heap on each partition, and then pop offsets
as long as they are no gaps. Any consumer can only read offsets < highestReadOffset so this ensures consumers do not
miss any data.

In case of failure of an agent, the agent may have obtained offsets but failed before registering the SSTable. In this
case there would be a gap in written offsets and highestReadOffset wouldn't advance thus stalling consumers. To
prevent this, the cache responds to a change in cluster membership and sets highestReadOffset to the value of
nextWriteOffset - 1, effectively making all offsets obtained, readable and allowing consumers to advance, potentially
with gaps in the offset sequence. However, it cannot be allowed for registrations to occur for the offsets in the gap
after this change has occurred otherwise that data would be skipped past by consumers. Therefore we maintain a field
lowestAcceptableWrittenOffset which is updated to be nextWriteOffset at the point of cluster membership change.
When addWrittenOffsets is called we reject any registration where the offset is less than this value.
*/
type Cache struct {
	lock                  sync.RWMutex
	started               bool
	topicOffsets          map[int][]partitionOffsets
	topicInfoProvider     streammeta.TopicInfoProvider
	partitionOffsetLoader PartitionOffsetLoader
}

type partitionOffsets struct {
	lock                          sync.Mutex
	nextWriteOffset               int64
	highestReadableOffset         int64
	lowestAcceptableWrittenOffset int64
	writtenHeap                   writtenOffsetHeap
	loaded                        bool
}

func (p *partitionOffsets) clusterVersionChanged() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.highestReadableOffset = p.nextWriteOffset - 1
	p.lowestAcceptableWrittenOffset = p.nextWriteOffset
	p.writtenHeap = p.writtenHeap[:0]
}

func (p *partitionOffsets) updateWrittenOffsets(wo UpdateWrittenOffsetInfo) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.loaded {
		panic("partitionOffsets has not been loaded")
	}
	if wo.OffsetStart < p.lowestAcceptableWrittenOffset {
		return false
	}
	lastOffset := wo.OffsetStart + int64(wo.NumOffsets) - 1
	if lastOffset >= p.nextWriteOffset {
		panic("invalid written offset")
	}
	if wo.OffsetStart == p.highestReadableOffset+1 {
		p.highestReadableOffset += int64(wo.NumOffsets)
		// We pop offsets from the heap as long as there are no gaps in written offsets
		for len(p.writtenHeap) > 0 {
			minOffset := p.writtenHeap.Peek()
			if minOffset.offsetStart == p.highestReadableOffset+1 {
				p.highestReadableOffset = minOffset.offsetStart + int64(minOffset.numOffsets) - 1
				heap.Pop(&p.writtenHeap)
			} else {
				break
			}
		}
	} else {
		// Not in order - push to heap
		heap.Push(&p.writtenHeap, writtenOffset{
			offsetStart: wo.OffsetStart,
			numOffsets:  int32(wo.NumOffsets),
		})
	}
	return true
}

func (p *partitionOffsets) getNextOffset(numOffsets int) (int64, bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.loaded {
		return 0, false
	}
	offset := p.nextWriteOffset
	p.nextWriteOffset += int64(numOffsets)
	return offset, true
}

func (p *partitionOffsets) load(topicID int, partitionID int, loader PartitionOffsetLoader) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.loaded {
		return nil
	}
	off, err := loader.LoadHighestOffsetForPartition(topicID, partitionID)
	if err != nil {
		return err
	}
	p.nextWriteOffset = off + 1
	p.highestReadableOffset = off
	p.loaded = true
	return nil
}

func (p *partitionOffsets) getHighestReadableOffset() int64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.highestReadableOffset
}

func NewOffsetsCache(provider streammeta.TopicInfoProvider, loader PartitionOffsetLoader) *Cache {
	return &Cache{
		topicInfoProvider:     provider,
		partitionOffsetLoader: loader,
	}
}

type GetOffsetTopicInfo struct {
	TopicID     int
	PartitionID int
	NumOffsets  int
}

type UpdateWrittenOffsetInfo struct {
	TopicID     int
	PartitionID int
	OffsetStart int64
	NumOffsets  int
}

type PartitionOffsetLoader interface {
	LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error)
}

func (o *Cache) Start() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.started {
		return nil
	}
	topicInfos, err := o.topicInfoProvider.GetAllTopics()
	if err != nil {
		return err
	}
	o.topicOffsets = make(map[int][]partitionOffsets, len(topicInfos))
	for _, topicInfo := range topicInfos {
		offsetsSlice := make([]partitionOffsets, topicInfo.PartitionCount)
		for i := 0; i < topicInfo.PartitionCount; i++ {
			offsetsSlice[i].nextWriteOffset = 0
		}
		o.topicOffsets[topicInfo.TopicID] = offsetsSlice
	}
	o.started = true
	return nil
}

// GetOffsets returns an offset for each of the provider GetOffsetTopicInfo instances
func (o *Cache) GetOffsets(infos []GetOffsetTopicInfo) ([]int64, error) {
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

func (o *Cache) GetHighestReadableOffset(topicID int, partitionID int) (int64, error) {
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return 0, errors.New("not started")
	}
	offs, err := o.getTopicOffsets(topicID)
	if err != nil {
		return 0, err
	}
	if err := checkPartitionOffsetInRange(partitionID, len(offs)); err != nil {
		return 0, err
	}
	return offs[partitionID].getHighestReadableOffset(), nil
}

func (o *Cache) MembershipChanged() {
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return
	}
	for _, offsets := range o.topicOffsets {
		for i := 0; i < len(offsets); i++ {
			offsets[i].clusterVersionChanged()
		}
	}
}

func (o *Cache) getOffset(info GetOffsetTopicInfo) (int64, error) {
	if info.NumOffsets < 1 {
		// OK to panic as would be programming error
		panic(fmt.Sprintf("invalid value for NumOffsets: %d", info.NumOffsets))
	}
	offsets, err := o.getTopicOffsets(info.TopicID)
	if err != nil {
		return 0, err
	}
	if err := checkPartitionOffsetInRange(info.PartitionID, len(offsets)); err != nil {
		return 0, err
	}
	offs := &offsets[info.PartitionID]
	for {
		offset, loaded := offs.getNextOffset(info.NumOffsets)
		if loaded {
			return offset, nil
		}
		// lazy load the offset.
		if err := offs.load(info.TopicID, info.PartitionID, o.partitionOffsetLoader); err != nil {
			return 0, err
		}
	}
}

func checkPartitionOffsetInRange(partitionID int, numPartitions int) error {
	if partitionID >= numPartitions {
		return errors.Errorf("partition offset out of range: %d", partitionID)
	}
	return nil
}

func (o *Cache) getTopicOffsets(topicID int) ([]partitionOffsets, error) {
	offsets, ok := o.topicOffsets[topicID]
	if !ok {
		return nil, errors.Errorf("unknown topic id: %d", topicID)
	}
	return offsets, nil
}

func (o *Cache) UpdateWrittenOffsets(writtenOffsetInfos []UpdateWrittenOffsetInfo) error {
	prevOk := false
	for _, writtenOffsetInfo := range writtenOffsetInfos {
		offsets, ok := o.topicOffsets[writtenOffsetInfo.TopicID]
		if !ok {
			return errors.Errorf("unknown topic id: %d", writtenOffsetInfo.TopicID)
		}
		ok = offsets[writtenOffsetInfo.PartitionID].updateWrittenOffsets(writtenOffsetInfo)
		if !ok {
			if prevOk {
				// If any of the WrittenOffsets fail, they will all fail so there will be no partial state applied.
				// Cannot occur - sanity check invariant
				panic("all or none written offsets should fail")
			}
			// Attempting to update written offsets failed - this will occur if a membership change happened
			// which causes any attempts to update written offsets for offsets that were got before the membership
			// change to fail.
			// We send back an unavailable error and the table pusher will close it's connection then retry with
			// new offets and a new table
			return common.NewTektiteErrorf(common.Unavailable, "Cannot update written offsets - membership change has occurred")
		} else {
			prevOk = true
		}
	}
	return nil
}

type writtenOffset struct {
	offsetStart int64
	numOffsets  int32
}

type writtenOffsetHeap []writtenOffset

func (h *writtenOffsetHeap) Len() int {
	return len(*h)
}

func (h *writtenOffsetHeap) Less(i, j int) bool {
	hh := *h
	return hh[i].offsetStart < hh[j].offsetStart
}

func (h *writtenOffsetHeap) Swap(i, j int) {
	hh := *h
	hh[i], hh[j] = hh[j], hh[i]
}

func (h *writtenOffsetHeap) Push(x interface{}) {
	*h = append(*h, x.(writtenOffset))
}

func (h *writtenOffsetHeap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n-1]
	*h = (*h)[:n-1]
	return x
}

func (h *writtenOffsetHeap) Peek() writtenOffset {
	return (*h)[0]
}
