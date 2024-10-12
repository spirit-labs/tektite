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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

/*
Cache caches next available and least readable offset for a topic partition in memory. It also re-orders table
registrations by offset, so tables notifications are always released by the controller in offset order.
Before an agent can write topic data to object storage it must first obtain partition offsets for the data it's writing.
It does this by calliong GetOffsets to request offsets for the partitions in the ssTable it wants to write. The offsets
cache allocates offsets while locking each partition being requested. It then returns the offsets along with a sequence
number. Requests to get offsets are always ordered by [topic_id, partition_id] so deadlock cannot occur between
concurrent requests for overlapping sets of partitions.

The Cache also maintains lastReadOffset for each partition. Any offsets greater than this might exist in storage but
cannot be read. As offsets are obtained before the SSTable is written to object storage and the SSTable is registered
with the LSM it's possible that two different agents who are writing for the same partition can register a SSTable
containing later offsets before earlier offsets. If we made all offsets immediately readable then a consumer could
read the later offsets (Kafka consumers tolerate gaps in offsets), thus skipping the data for the earlier offsets.
To prevent this behaviour, when an SStable is registered after pushing to object storage the caller also provides the
sequence number that was returned in the call to GetOffsets, we know that sequence is in offset order. A min heap
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
	lsm                      lsmHolder
	partitionHashes          *parthash.PartitionHashes
	objStore                 objstore.Client
	dataBucketName           string
	stopping                 atomic.Bool
	offsetsSeq               int64
	reorderLock              sync.Mutex
	offsHeap                 seqHeap
	offsetsMap               map[int64][]OffsetTopicInfo
	lastReleasedSequence     int64
	lowestAcceptableSequence int64
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
		offsetsMap:        make(map[int64][]OffsetTopicInfo),
	}, nil
}

type GetOffsetTopicInfo struct {
	TopicID        int
	PartitionInfos []GetOffsetPartitionInfo
}

type GetOffsetPartitionInfo struct {
	PartitionID int
	NumOffsets  int
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

// GetOffsets returns offsets for the provided GetOffsetTopicInfo instances. infos must be provided in
// [topic id, partition id] order to avoid deadlock
// Note, that the offset number returned is the *last* allocated offset for the partition.
func (o *Cache) GetOffsets(infos []GetOffsetTopicInfo) ([]OffsetTopicInfo, int64, error) {
	if len(infos) == 0 {
		return nil, 0, errors.New("empty infos")
	}
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return nil, 0, errors.New("offsets cache not started")
	}
	res, seq, err := o.getOffsets0(infos)
	if err != nil {
		return nil, 0, err
	}
	// reorderLock must be taken after partition locks have been unlocked, to avoid deadlock
	o.reorderLock.Lock()
	defer o.reorderLock.Unlock()
	o.offsetsMap[seq] = res
	return res, seq, nil
}

func (o *Cache) getOffsets0(infos []GetOffsetTopicInfo) ([]OffsetTopicInfo, int64, error) {
	// First we gather all the partition offsets, and obtain all the locks before we get any offsets. This is
	// essential to ensure that all offsets got for a particular sequence are higher than offsets got for a lower
	// sequence. We need this guarantee so that when we re-order registrations in sequence order we only output
	// tables with offsets in ascending order.
	// Note, that infos will always be provided in [topic id, partition id] order - this ensures deadlock is impossible
	// with concurrent calls to GetOffsets for overlapping sets of partitions
	var partOffs []*partitionOffsets
	defer func() {
		for _, off := range partOffs {
			off.lock.Unlock()
		}
	}()
	for _, topicInfo := range infos {
		topicOffsets, err := o.getTopicOffsets(topicInfo.TopicID)
		if err != nil {
			return nil, 0, err
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
	// Get a sequence value
	seq := atomic.AddInt64(&o.offsetsSeq, 1)
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
			offset, err := partOff.getNextOffset(partitionInfo.NumOffsets, topicInfo.TopicID, partitionInfo.PartitionID, o)
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
	return offInfos, seq, nil
}

func (o *Cache) GetLastReadableOffset(topicID int, partitionID int) (int64, error) {
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
	return offs[partitionID].getLastReadableOffset(topicID, partitionID, o)
}

func (o *Cache) MembershipChanged() {
	o.lock.Lock()
	defer o.lock.Unlock()
	if !o.started {
		return
	}
	// membership has changed so it's possible an agent has failed and it might have gotten offsets which will never
	// have a table registered for. in this case we tell each offset that membership changed so they can update
	// their last readable, and we also set lowestAcceptable offset to be last offset sequence + 1so we can reject any
	// attempts to release offsets for sequences below this.
	seq := atomic.LoadInt64(&o.offsetsSeq)
	atomic.StoreInt64(&o.lowestAcceptableSequence, seq+1)
	for _, offsets := range o.topicOffsets {
		for i := 0; i < len(offsets); i++ {
			offsets[i].clusterVersionChanged()
		}
	}
	o.reorderLock.Lock()
	defer o.reorderLock.Unlock()
	// reset any unordered tables waiting to be released
	o.offsetsMap = map[int64][]OffsetTopicInfo{}
	o.offsHeap = nil
	o.lastReleasedSequence = seq
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

func checkPartitionOffsetInRange(partitionID int, numPartitions int) error {
	if partitionID >= numPartitions {
		return errors.Errorf("partition offset out of range: %d", partitionID)
	}
	return nil
}

func (o *Cache) getTopicOffsets(topicID int) ([]partitionOffsets, error) {
	offsets, ok := o.topicOffsets[topicID]
	if !ok {
		var err error
		offsets, err = o.loadTopicInfo(topicID)
		if err != nil {
			return nil, err
		}
	}
	return offsets, nil
}

func (o *Cache) MaybeReleaseOffsets(sequence int64, sstableID sst.SSTableID) ([]OffsetTopicInfo, []sst.SSTableID, error) {
	o.lock.RLock()
	defer o.lock.RUnlock()
	if !o.started {
		return nil, nil, errors.New("offsets cache not started")
	}
	lowestAcceptable := atomic.LoadInt64(&o.lowestAcceptableSequence)
	if sequence < lowestAcceptable {
		// attempt to release offsets came in for a sequence that was gotten before membership change
		return nil, nil, common.NewTektiteErrorf(common.Unavailable, "cannot release offsets - membership change has occurred")
	}
	o.reorderLock.Lock()
	defer o.reorderLock.Unlock()
	var infos []OffsetTopicInfo
	var tableIDs []sst.SSTableID
	if sequence == o.lastReleasedSequence+1 && len(o.offsHeap) == 0 {
		// happy path - avoid heap
		var ok bool
		infos, ok = o.offsetsMap[sequence]
		if !ok {
			panic("cannot find offsets in map")
		}
		delete(o.offsetsMap, sequence)
		o.lastReleasedSequence = sequence
		tableIDs = []sst.SSTableID{sstableID}
	} else {
		heap.Push(&o.offsHeap, seqHolder{
			seq:     sequence,
			tableID: sstableID,
		})
		// We pop sequences as long as sequence is contiguous and ascending
		for len(o.offsHeap) > 0 {
			top := o.offsHeap.Peek()
			if top.seq == o.lastReleasedSequence+1 {
				heap.Pop(&o.offsHeap)
				infs, ok := o.offsetsMap[top.seq]
				if !ok {
					panic("cannot find offsets in map")
				}
				delete(o.offsetsMap, top.seq)
				o.lastReleasedSequence = top.seq
				if infos == nil {
					infos = infs
				} else {
					infos = mergeTopicInfos(infos, infs)
				}
				tableIDs = append(tableIDs, top.tableID)
			} else {
				break
			}
		}
	}
	if err := o.updateLastReadable(infos); err != nil {
		return nil, nil, err
	}
	return infos, tableIDs, nil
}

func (o *Cache) updateLastReadable(infos []OffsetTopicInfo) error {
	for _, topicInfo := range infos {
		offs, err := o.getTopicOffsets(topicInfo.TopicID)
		if err != nil {
			return err
		}
		for _, partInfo := range topicInfo.PartitionInfos {
			offs[partInfo.PartitionID].setLastReadableOffset(partInfo.Offset)
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
		if i1 == len(offs1) {
			partitionID1 = math.MaxInt
		} else {
			partitionID1 = offs1[i1].PartitionID
		}
		var partitionID2 int
		if i2 == len(offs2) {
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
			if offs2[i1].Offset > offs2[i2].Offset {
				panic("later sequence should always have higher offset")
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
			return nil, errors.New("offset loader is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load offset from object storage due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
}

type partitionOffsets struct {
	lock               sync.Mutex
	nextWriteOffset    int64
	lastReadableOffset int64
	loaded             bool
}

func (p *partitionOffsets) clusterVersionChanged() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.lastReadableOffset = p.nextWriteOffset - 1
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
	p.lastReadableOffset = offset
}

type seqHolder struct {
	seq     int64
	tableID sst.SSTableID
}

type seqHeap []seqHolder

func (h *seqHeap) Len() int {
	return len(*h)
}

func (h *seqHeap) Less(i, j int) bool {
	hh := *h
	return hh[i].seq < hh[j].seq
}

func (h *seqHeap) Swap(i, j int) {
	hh := *h
	hh[i], hh[j] = hh[j], hh[i]
}

func (h *seqHeap) Push(x interface{}) {
	*h = append(*h, x.(seqHolder))
}

func (h *seqHeap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n-1]
	*h = (*h)[:n-1]
	return x
}

func (h *seqHeap) Peek() seqHolder {
	return (*h)[0]
}
