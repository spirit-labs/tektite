package offsets

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

/*
Cache caches next available and least readable offset for a topic partition in memory. It also re-orders table
registrations by offset, so tables notifications are always released by the controller in offset order.
Before an agent can write topic data to object storage it must first obtain partition offsets for the data it's writing.
It does this by calliong GenerateOffsets to request offsets for the partitions in the ssTable it wants to write. The offsets
cache allocates offsets while locking each partition being requested. It then returns the offsets along with a sequence
number. Requests to get offsets are always ordered by [topic_id, partition_id] so deadlock cannot occur between
concurrent requests for overlapping sets of partitions.

The Cache also maintains lastReadOffset for each partition. Any offsets greater than this might exist in storage but
cannot be read. As offsets are obtained before the SSTable is written to object storage and the SSTable is registered
with the LSM it's possible that two different agents who are writing for the same partition can register a SSTable
containing later offsets before earlier offsets. If we made all offsets immediately readable then a consumer could
read the later offsets (Kafka consumers tolerate gaps in offsets), thus skipping the data for the earlier offsets.
To prevent this behaviour, when an SStable is registered after pushing to object storage the caller also provides the
sequence number that was returned in the call to GenerateOffsets, we know that sequence is in offset order. A min heap
is maintained which then re-orders the registrations by sequence, and we pop entries from the heap as long as sequence
is contiguous, and maintain lastReadableOffset from the last entry popped.

In case of failure of an agent, the agent may have obtained offsets but failed before registering the SSTable. In this
case there would be a gap in written offsets and lastReadOffset wouldn't advance thus stalling consumers. To
prevent this, the cache responds to a change in cluster membership and sets lastReadOffset to the value of
nextWriteOffset - 1, effectively making all offsets obtained, readable and allowing consumers to advance, potentially
with gaps in the offset sequence. However, it cannot be allowed for registrations to occur for the offsets in the gap
after this change has occurred otherwise that data would be skipped past by consumers. Therefore we maintain a field
lowestAcceptableSequence which is updated to be current sequence at the point of cluster membership change.
When MaybeReleaseOffsets is called we reject any attempts where the offset is less than this value.
*/
type Cache struct {
	lock                     sync.RWMutex
	started                  bool
	topicOffsets             map[int][]partitionOffsets
	topicMetaProvider        topicMetaProvider
	querier                  querier
	partitionHashes          *parthash.PartitionHashes
	objStore                 objstore.Client
	dataBucketName           string
	stopping                 atomic.Bool
	offsetsSeq               int64
	reorderLock              sync.RWMutex
	offsHeap                 seqHeap
	offsetsMap               map[int64][]OffsetTopicInfo
	lastReleasedSequence     int64
	lowestAcceptableSequence int64
}

type topicMetaProvider interface {
	GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error)
}

type querier interface {
	GetTablesForHighestKeyWithPrefix(prefix []byte) ([]sst.SSTableID, error)
}

const (
	objectStoreCallTimeout   = 5 * time.Second
	unavailabilityRetryDelay = 1 * time.Second
)

func NewOffsetsCache(topicProvider topicMetaProvider, lsm querier, objStore objstore.Client, dataBucketName string) (*Cache, error) {
	// We don't cache as loader only loads once
	partHashes, err := parthash.NewPartitionHashes(0)
	if err != nil {
		return nil, err
	}
	return &Cache{
		topicMetaProvider:        topicProvider,
		topicOffsets:             make(map[int][]partitionOffsets),
		querier:                  lsm,
		objStore:                 objStore,
		dataBucketName:           dataBucketName,
		partitionHashes:          partHashes,
		offsetsMap:               make(map[int64][]OffsetTopicInfo),
		lowestAcceptableSequence: 1,
	}, nil
}

type GenerateOffsetTopicInfo struct {
	TopicID        int
	PartitionInfos []GenerateOffsetPartitionInfo
}

type GenerateOffsetPartitionInfo struct {
	PartitionID int
	NumOffsets  int
}

type GetOffsetTopicInfo struct {
	TopicID      int
	PartitionIDs []int
}

type LastReadableOffsetUpdatedTopicInfo struct {
	TopicID        int
	PartitionInfos []LastReadableOffsetUpdatedPartitionInfo
}

type LastReadableOffsetUpdatedPartitionInfo struct {
	PartitionID        int
	LastReadableOffset int64
}

type OffsetTopicInfo struct {
	TopicID        int
	PartitionInfos []OffsetPartitionInfo
}

type OffsetPartitionInfo struct {
	PartitionID int
	Offset      int64
}

func (c *Cache) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	c.started = true
	return nil
}

func (c *Cache) Stop() {
	c.stopping.Store(true)
}

// GenerateOffsets generates offsets for the provided GenerateOffsetTopicInfo instances. infos must be provided in
// [topic id, partition id] order to avoid deadlock
// Note, that the offset number returned is the *last* allocated offset for the partition.
func (c *Cache) GenerateOffsets(infos []GenerateOffsetTopicInfo) ([]OffsetTopicInfo, int64, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.started {
		return nil, 0, errors.New("offsets cache not started")
	}
	res, seq, err := c.generateOffsets0(infos)
	if err != nil {
		return nil, 0, err
	}
	// reorderLock must be taken after partition locks have been unlocked, to avoid deadlock
	c.reorderLock.Lock()
	defer c.reorderLock.Unlock()
	c.offsetsMap[seq] = res
	return res, seq, nil
}

func (c *Cache) generateOffsets0(infos []GenerateOffsetTopicInfo) ([]OffsetTopicInfo, int64, error) {
	// First we gather all the partition offsets, and obtain all the locks before we get any offsets. This is
	// essential to ensure that all offsets got for a particular sequence are higher than offsets got for a lower
	// sequence. We need this guarantee so that when we re-order registrations in sequence order we only output
	// tables with offsets in ascending order.
	// Note, that infos will always be provided in [topic id, partition id] order - this ensures deadlock is impossible
	// with concurrent calls to PrePush for overlapping sets of partitions
	var partOffs []*partitionOffsets
	defer func() {
		for _, off := range partOffs {
			off.lock.Unlock()
		}
	}()
	for _, topicInfo := range infos {
		topicOffsets, exists, err := c.getTopicOffsets(topicInfo.TopicID)
		if err != nil {
			return nil, 0, err
		}
		if !exists {
			return nil, 0, common.NewTektiteErrorf(common.TopicDoesNotExist, "generate offsets: unknown topic: %d", topicInfo.TopicID)
		}
		for _, partitionInfo := range topicInfo.PartitionInfos {
			if partitionInfo.NumOffsets < 1 {
				// OK to panic as would be programming error
				panic(fmt.Sprintf("invalid value for NumOffsets: %d", partitionInfo.NumOffsets))
			}
			if err := checkPartitionOffsetInRange(partitionInfo.PartitionID, len(topicOffsets)); err != nil {
				return nil, 0, err
			}
			partitionOff := &topicOffsets[partitionInfo.PartitionID]
			partitionOff.lock.Lock()
			partOffs = append(partOffs, partitionOff)
		}
	}
	// Now we can get the actual offsets
	offInfos := make([]OffsetTopicInfo, len(infos))
	index := 0
	for i, topicInfo := range infos {
		topicOffInfo := OffsetTopicInfo{
			TopicID:        topicInfo.TopicID,
			PartitionInfos: make([]OffsetPartitionInfo, len(topicInfo.PartitionInfos)),
		}
		for j, partitionInfo := range topicInfo.PartitionInfos {
			partOff := partOffs[index]
			index++
			offset, err := partOff.getNextOffset(partitionInfo.NumOffsets, topicInfo.TopicID, partitionInfo.PartitionID, c)
			if err != nil {
				return nil, 0, err
			}
			topicOffInfo.PartitionInfos[j] = OffsetPartitionInfo{
				PartitionID: partitionInfo.PartitionID,
				Offset:      offset + int64(partitionInfo.NumOffsets) - 1, // The last offset given out
			}
		}
		offInfos[i] = topicOffInfo
	}
	// We must get the sequence with the partition locks held
	seq := atomic.AddInt64(&c.offsetsSeq, 1) // sequence must start at 1 as initial lastReleasedSequence is 0
	return offInfos, seq, nil
}

func (c *Cache) GetLastReadableOffset(topicID int, partitionID int) (int64, bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.started {
		return 0, false, errors.New("offsets cache not started")
	}
	offs, exists, err := c.getTopicOffsets(topicID)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return 0, false, nil
	}
	if err := checkPartitionOffsetInRange(partitionID, len(offs)); err != nil {
		return 0, false, err
	}
	off, err := offs[partitionID].getLastReadableOffset(topicID, partitionID, c)
	if err != nil {
		return 0, false, err
	}
	return off, true, nil
}

func (c *Cache) ResizePartitionCount(topicID, partitionCount int) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.started {
		return false, errors.New("offsets cache not started")
	}
	offs, exists, err := c.getTopicOffsets(topicID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if partitionCount == len(offs) {
		// Nothing to do
		return true, nil
	}
	if partitionCount < len(offs) {
		return false, common.NewTektiteErrorf(common.PartitionOutOfRange, "cannot reduce partition count")
	}
	newOffs := make([]partitionOffsets, partitionCount)
	copy(newOffs, offs)
	if partitionCount > len(offs) {
		// grow partitions
		for i := len(offs); i < partitionCount; i++ {
			newOffs[i].loaded = true
			newOffs[i].lastReadableOffset = -1
			newOffs[i].nextWriteOffset = 0
		}
	}
	c.topicOffsets[topicID] = newOffs
	return true, nil
}

func (c *Cache) ResetOffsets() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return
	}
	// cluster membership has changed, so a pusher might have crashed leaving sequences in the heap which would
	// otherwise delay releasing of offsets.
	// We release all waiting offsets
	c.reorderLock.Lock()
	defer c.reorderLock.Unlock()
	// Now release any waiting offsets
	var topicInfos []OffsetTopicInfo
	for c.offsHeap.Len() > 0 {
		h := heap.Pop(&c.offsHeap)
		seq := h.(int64)
		infos, ok := c.offsetsMap[seq]
		if !ok {
			panic("cannot find info in map")
		}
		topicInfos = append(topicInfos, infos...)
	}
	if err := c.updateLastReadable(topicInfos); err != nil {
		log.Warnf("failed to update last readable offsets: %v", err)
	}
	// reset all the waiting infos and heap
	c.offsetsMap = map[int64][]OffsetTopicInfo{}
	c.offsHeap = nil
	// Needs to be atomic as updated without lock held
	c.lastReleasedSequence = atomic.LoadInt64(&c.offsetsSeq)
	// We update lowestAcceptableSequence so we don't accept any tables being pushed with a sequence given out before
	// this membership change
	c.lowestAcceptableSequence = c.lastReleasedSequence + 1
}

func (c *Cache) resetOffsets() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return
	}
}

func (c *Cache) loadTopicInfo(topicID int) ([]partitionOffsets, bool, error) {
	// Upgrade the lock
	c.lock.RUnlock()
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		c.lock.RLock()
	}()
	offsets, ok := c.topicOffsets[topicID]
	if ok {
		return offsets, true, nil
	}
	info, exists, err := c.topicMetaProvider.GetTopicInfoByID(topicID)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	offsets = make([]partitionOffsets, info.PartitionCount)
	c.topicOffsets[topicID] = offsets
	return offsets, true, nil
}

func checkPartitionOffsetInRange(partitionID int, numPartitions int) error {
	if partitionID >= numPartitions {
		return common.NewTektiteErrorf(common.PartitionOutOfRange, "partition %d out of range - partition count %d", partitionID, numPartitions)
	}
	return nil
}

func (c *Cache) getTopicOffsets(topicID int) ([]partitionOffsets, bool, error) {
	offsets, ok := c.topicOffsets[topicID]
	if !ok {
		var err error
		var exists bool
		offsets, exists, err = c.loadTopicInfo(topicID)
		if err != nil {
			return nil, false, err
		}
		if !exists {
			return nil, false, nil
		}
	}
	return offsets, true, nil
}

func (c *Cache) MaybeReleaseOffsets(sequence int64) ([]OffsetTopicInfo, error) {
	if sequence == -1 {
		return nil, nil
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.started {
		return nil, errors.New("offsets cache not started")
	}
	c.reorderLock.Lock()
	defer c.reorderLock.Unlock()
	if sequence < c.lowestAcceptableSequence {
		// Ignore - offsets will already have been released -this is OK, the locking ensures that the table must have
		// been pushed *before* the offsets were released
		return nil, nil
	}
	var infos []OffsetTopicInfo
	if sequence == c.lastReleasedSequence+1 && len(c.offsHeap) == 0 {
		// happy path - avoid heap
		var ok bool
		infos, ok = c.offsetsMap[sequence]
		if !ok {
			panic("cannot find offsets in map")
		}
		delete(c.offsetsMap, sequence)
		c.lastReleasedSequence = sequence
	} else {
		heap.Push(&c.offsHeap, sequence)
		// We pop sequences as long as sequence is contiguous and ascending
		for len(c.offsHeap) > 0 {
			top := c.offsHeap.Peek()
			if top == c.lastReleasedSequence+1 {
				heap.Pop(&c.offsHeap)
				infs, ok := c.offsetsMap[top]
				if !ok {
					panic("cannot find offsets in map")
				}
				delete(c.offsetsMap, top)
				c.lastReleasedSequence = top
				if infos == nil {
					infos = infs
				} else {
					infos = mergeTopicInfos(infos, infs)
				}
			} else {
				break
			}
		}
	}
	if err := c.updateLastReadable(infos); err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *Cache) updateLastReadable(infos []OffsetTopicInfo) error {
	for _, topicInfo := range infos {
		offs, exists, err := c.getTopicOffsets(topicInfo.TopicID)
		if err != nil {
			return err
		}
		if !exists {
			log.Warnf("updateLastReadable - unknown topic id %d", topicInfo.TopicID)
		} else {
			for _, partInfo := range topicInfo.PartitionInfos {
				offs[partInfo.PartitionID].setLastReadableOffset(partInfo.Offset)
			}
		}
	}
	return nil
}

func mergeTopicInfos(offs1 []OffsetTopicInfo, offs2 []OffsetTopicInfo) []OffsetTopicInfo {
	// We always get offsets in [topic id, partition id] order therefore we know the infos are also ordered this way
	// this means we can do a merge which is more efficient than using maps
	infos3 := make([]OffsetTopicInfo, 0, len(offs1)+len(offs2))
	i1 := 0
	i2 := 0
	for i1 < len(offs1) || i2 < len(offs2) {
		var topicID1 int
		if i1 == len(offs1) {
			topicID1 = math.MaxInt
		} else {
			topicID1 = offs1[i1].TopicID
		}
		var topicID2 int
		if i2 == len(offs2) {
			topicID2 = math.MaxInt
		} else {
			topicID2 = offs2[i2].TopicID
		}
		if topicID1 < topicID2 {
			infos3 = append(infos3, offs1[i1])
			i1++
		} else if topicID2 < topicID1 {
			infos3 = append(infos3, offs2[i2])
			i2++
		} else {
			infos3 = append(infos3, OffsetTopicInfo{
				TopicID:        topicID1,
				PartitionInfos: mergePartitionInfos(offs1[i1].PartitionInfos, offs2[i2].PartitionInfos),
			})
			i1++
			i2++
		}
	}
	return infos3
}

func mergePartitionInfos(offs1 []OffsetPartitionInfo, offs2 []OffsetPartitionInfo) []OffsetPartitionInfo {
	infos3 := make([]OffsetPartitionInfo, 0, len(offs1)+len(offs2))
	i1 := 0
	i2 := 0
	for i1 < len(offs1) || i2 < len(offs2) {
		var partitionID1 int
		if i1 >= len(offs1) {
			partitionID1 = math.MaxInt
		} else {
			partitionID1 = offs1[i1].PartitionID
		}
		var partitionID2 int
		if i2 >= len(offs2) {
			partitionID2 = math.MaxInt
		} else {
			partitionID2 = offs2[i2].PartitionID
		}
		if partitionID1 < partitionID2 {
			infos3 = append(infos3, offs1[i1])
			i1++
		} else if partitionID2 < partitionID1 {
			infos3 = append(infos3, offs2[i2])
			i2++
		} else {
			if offs1[i1].Offset > offs2[i2].Offset {
				panic(fmt.Sprintf("later sequence should always have higher offset. offs1: %v offs2:%v", offs1, offs2))
			}
			infos3 = append(infos3, OffsetPartitionInfo{
				PartitionID: partitionID1,
				Offset:      offs2[i2].Offset,
			})
			i1++
			i2++
		}
	}
	return infos3
}

func (c *Cache) LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error) {
	prefix, err := c.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return 0, err
	}
	var key []byte
	key = append(key, prefix...)
	prefix = append(prefix, common.EntryTypeTopicData)
	tables, err := c.querier.GetTablesForHighestKeyWithPrefix(prefix)
	if err != nil {
		return 0, err
	}
	if len(tables) > 0 {
		tableID := tables[0] // first one is most recent
		// TODO instead of going directly to the object store, should we fetch from fetch cache?
		buff, err := c.getWithRetry(tableID)
		if err != nil {
			return 0, err
		}
		if len(buff) == 0 {
			return 0, errors.Errorf("ssttable %s not found", tableID)
		}
		table, err := sst.GetSSTableFromBytes(buff)
		if err != nil {
			return 0, err
		}
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
				// The offset on the key is the last offset in the batch
				offset, _ = encoding.KeyDecodeInt(kv.Key, 17)
			} else {
				break
			}
		}
		return offset, nil
	}
	return -1, nil
}

func (c *Cache) getWithRetry(tableID sst.SSTableID) ([]byte, error) {
	for {
		buff, err := objstore.GetWithTimeout(c.objStore, c.dataBucketName, string(tableID), objectStoreCallTimeout)
		if err == nil {
			return buff, nil
		}
		if c.stopping.Load() {
			return nil, errors.New("offset loader is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load offset from object storage due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
}

// SetLastReadableOffset used in tests only
func (c *Cache) SetLastReadableOffset(topicID int, partitionID int, offset int64) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	pos, ok, err := c.getTopicOffsets(topicID)
	if err != nil {
		panic(err)
	}
	if !ok {
		return
	}
	pos[partitionID].forceSetLastReadableOffset(offset)
}

type partitionOffsets struct {
	lock               sync.Mutex
	nextWriteOffset    int64
	lastReadableOffset int64
	loaded             bool
}

func (p *partitionOffsets) getNextOffset(numOffsets int, topicID int, partitionID int, o *Cache) (int64, error) {
	if !p.loaded {
		if err := p.load(topicID, partitionID, o); err != nil {
			return 0, err
		}
	}
	offset := p.nextWriteOffset
	p.nextWriteOffset += int64(numOffsets)
	return offset, nil
}

func (p *partitionOffsets) getLastReadableOffset(topicID int, partitionID int, o *Cache) (int64, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.loaded {
		if err := p.load(topicID, partitionID, o); err != nil {
			return 0, err
		}
	}
	return p.lastReadableOffset, nil
}

func (p *partitionOffsets) load(topicID int, partitionID int, o *Cache) error {
	off, err := o.LoadHighestOffsetForPartition(topicID, partitionID)
	if err != nil {
		return err
	}
	p.nextWriteOffset = off + 1
	p.lastReadableOffset = off
	p.loaded = true
	return nil
}

func (p *partitionOffsets) setLastReadableOffset(offset int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lastReadableOffset != 0 && offset <= p.lastReadableOffset {
		// sanity check
		panic(fmt.Sprintf("attempt to set lro to value %d current value is %d", offset, p.lastReadableOffset))
	}
	p.lastReadableOffset = offset
}

func (p *partitionOffsets) forceSetLastReadableOffset(offset int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.lastReadableOffset = offset
	p.loaded = true
}

type seqHeap []int64

func (h *seqHeap) Len() int {
	return len(*h)
}

func (h *seqHeap) Less(i, j int) bool {
	hh := *h
	return hh[i] < hh[j]
}

func (h *seqHeap) Swap(i, j int) {
	hh := *h
	hh[i], hh[j] = hh[j], hh[i]
}

func (h *seqHeap) Push(x interface{}) {
	*h = append(*h, x.(int64))
}

func (h *seqHeap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n-1]
	*h = (*h)[:n-1]
	return x
}

func (h *seqHeap) Peek() int64 {
	return (*h)[0]
}
