package offsets

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"sync"
	"sync/atomic"
	"time"
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
	lock              sync.RWMutex
	started           bool
	topicOffsets      map[int][]partitionOffsets
	topicMetaProvider topicMetaProvider
	lsm               lsmHolder
	partitionHashes   *parthash.PartitionHashes
	objStore          objstore.Client
	dataBucketName    string
	stopping          atomic.Bool
}

type topicMetaProvider interface {
	GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, error)
}
type lsmHolder interface {
	GetTablesForHighestKeyWithPrefix(prefix []byte) ([]sst.SSTableID, error)
}

const (
	objectStoreCallTimeout   = 5 * time.Second
	unavailabilityRetryDelay = 1 * time.Second
)

func NewOffsetsCache(topicProvider topicMetaProvider, lsm lsmHolder, objStore objstore.Client, dataBucketName string) (*Cache, error) {
	// We don't cache as loader only loads once
	partHashes, err := parthash.NewPartitionHashes(0)
	if err != nil {
		return nil, err
	}
	return &Cache{
		topicMetaProvider: topicProvider,
		topicOffsets:      make(map[int][]partitionOffsets),
		lsm:               lsm,
		objStore:          objStore,
		dataBucketName:    dataBucketName,
		partitionHashes:   partHashes,
	}, nil
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

func (o *Cache) Start() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.started {
		return nil
	}
	o.started = true
	return nil
}

func (o *Cache) Stop() {
	o.stopping.Store(true)
}

// GetOffsets returns an offset for each of the provider GetOffsetTopicInfo instances
func (o *Cache) GetOffsets(infos []GetOffsetTopicInfo) ([]int64, error) {
	if len(infos) == 0 {
		return nil, errors.New("empty infos")
	}
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return nil, errors.New("offsets cache not started")
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
		return 0, errors.New("offsets cache not started")
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

func (o *Cache) loadTopicInfo(topicID int) ([]partitionOffsets, error) {
	// Upgrade the lock
	o.lock.RUnlock()
	o.lock.Lock()
	defer func() {
		o.lock.Unlock()
		o.lock.RLock()
	}()
	offsets, ok := o.topicOffsets[topicID]
	if ok {
		return offsets, nil
	}
	info, err := o.topicMetaProvider.GetTopicInfoByID(topicID)
	if err != nil {
		return nil, err
	}
	offsets = make([]partitionOffsets, info.PartitionCount)
	o.topicOffsets[topicID] = offsets
	return offsets, nil
}

func (o *Cache) getOffset(info GetOffsetTopicInfo) (int64, error) {
	if info.NumOffsets < 1 {
		// OK to panic as would be programming error
		panic(fmt.Sprintf("invalid value for NumOffsets: %d", info.NumOffsets))
	}
	offsets, ok := o.topicOffsets[info.TopicID]
	if !ok {
		var err error
		offsets, err = o.loadTopicInfo(info.TopicID)
		if err != nil {
			return 0, err
		}
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
		if err := offs.load(info.TopicID, info.PartitionID, o); err != nil {
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

func (o *Cache) LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error) {
	prefix, err := o.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return 0, err
	}
	tables, err := o.lsm.GetTablesForHighestKeyWithPrefix(prefix)
	if err != nil {
		return 0, err
	}
	for _, tableID := range tables {
		buff, err := o.getWithRetry(tableID)
		if err != nil {
			return 0, err
		}
		if len(buff) == 0 {
			return 0, errors.Errorf("ssttable %s not found", tableID)
		}
		var table sst.SSTable
		table.Deserialize(buff, 0)
		iter, err := table.NewIterator(prefix, nil)
		if err != nil {
			return 0, err
		}
		var offset int64 = -1
		for {
			ok, kv, err := iter.Next()
			if err != nil {
				return 0, err
			}
			if !ok {
				break
			}
			if bytes.Equal(prefix, kv.Key[:len(prefix)]) {
				baseOffset, _ := encoding.KeyDecodeInt(kv.Key, 16)
				numRecords := binary.BigEndian.Uint32(kv.Value[57:])
				offset = baseOffset + int64(numRecords) - 1
			} else {
				break
			}
		}
		return offset, nil
	}
	return -1, nil
}

func (o *Cache) getWithRetry(tableID sst.SSTableID) ([]byte, error) {
	for {
		buff, err := objstore.GetWithTimeout(o.objStore, o.dataBucketName, string(tableID), objectStoreCallTimeout)
		if err == nil {
			return buff, nil
		}
		if o.stopping.Load() {
			return nil, errors.New("offsetloader is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load offset from object storage due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
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

func (p *partitionOffsets) load(topicID int, partitionID int, o *Cache) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.loaded {
		return nil
	}
	off, err := o.LoadHighestOffsetForPartition(topicID, partitionID)
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
