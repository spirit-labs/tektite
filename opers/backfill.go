package opers

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type BackfillOperator struct {
	BaseOperator
	id                   string
	cfg                  *conf.Config
	receiverID           int
	schema               *OperatorSchema
	fromSlabID           int
	offsetsSlabID        int
	lagging              bool
	partitionIterators   []iteratorInfo
	rowCols              []int
	maxBackfillBatchSize int
	stopLock             sync.Mutex
	stopped              bool
	storeCommittedAsync  bool
	timers               sync.Map
	hashCache            *partitionHashCache
}

// Always accessed from same GR apart from the closed atomic bool
type iteratorInfo struct {
	initialised   bool
	iter          iteration.Iterator
	lagging       bool
	loadedAllRows bool
	partID        int
	closed        atomic.Bool
}

func NewBackfillOperator(schema *OperatorSchema, cfg *conf.Config, fromSlabID int, offsetsSlabID,
	maxBackfillBatchSize int, receiverID int, storeCommittedAsync bool) *BackfillOperator {
	if !HasOffsetColumn(schema.EventSchema) {
		panic("input not a stored stream")
	}
	var rowCols []int
	for i := 1; i < len(schema.EventSchema.ColumnTypes()); i++ {
		rowCols = append(rowCols, i)
	}
	partitionCount := schema.PartitionScheme.Partitions
	offsetsToCommit := make([]int64, partitionCount)
	for i := 0; i < partitionCount; i++ {
		offsetsToCommit[i] = -1
	}
	return &BackfillOperator{
		id:                   uuid.New().String(),
		schema:               schema,
		cfg:                  cfg,
		fromSlabID:           fromSlabID,
		offsetsSlabID:        offsetsSlabID,
		rowCols:              rowCols,
		partitionIterators:   make([]iteratorInfo, partitionCount),
		maxBackfillBatchSize: maxBackfillBatchSize,
		receiverID:           receiverID,
		storeCommittedAsync:  storeCommittedAsync,
		hashCache:            newPartitionHashCache(schema.MappingID, schema.Partitions),
	}
}

func (b *BackfillOperator) loadCommittedOffsetForPartition(execCtx StreamExecContext) (int64, bool, error) {
	partitionHash := b.hashCache.getHash(execCtx.PartitionID())
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(b.offsetsSlabID), 24)
	value, err := execCtx.Get(key)
	if err != nil {
		return 0, false, err
	}
	var offset int64
	if value == nil {
		return 0, false, nil
	}
	u := binary.LittleEndian.Uint64(value)
	offset = int64(u)
	return offset, true, nil
}

func (b *BackfillOperator) storeCommittedOffSetForPartition(partitionID int, offset int64, execCtx StreamExecContext) {
	partitionHash := b.hashCache.getHash(partitionID)
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(b.offsetsSlabID), 32)
	key = encoding.EncodeVersion(key, uint64(execCtx.WriteVersion()))
	value := make([]byte, 0, 8)
	value = encoding.AppendUint64ToBufferLE(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
}

func (b *BackfillOperator) pauseBackfill(partitionID int) {
	info := &b.partitionIterators[partitionID]
	if info.lagging {
		// if we're in a load we set loadedAllRows to true to prevent load of next batch being triggered
		info.loadedAllRows = true
	}
}

func (b *BackfillOperator) restartBackfill(partitionID int, processor proc.Processor) error {
	processor.CheckInProcessorLoop()
	// reset the info so it gets reloaded
	b.partitionIterators[partitionID] = iteratorInfo{}
	// restart load from last committed
	log.Debugf("restarting backfill on partition %d", partitionID)
	b.fireEmptyBatch(partitionID, processor)
	return nil
}

func (b *BackfillOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return b.HandleStreamBatch(batch, execCtx)
}

func (b *BackfillOperator) ReceiveBarrier(_ StreamExecContext) error {
	panic("not used")
}

func (b *BackfillOperator) RequiresBarriersInjection() bool {
	return false
}

func (b *BackfillOperator) ForwardingProcessorCount() int {
	return len(b.schema.PartitionScheme.ProcessorIDs)
}

func (b *BackfillOperator) Schema() *evbatch.EventSchema {
	return b.schema.EventSchema
}

func (b *BackfillOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in queries")
}

// IsLive returns whether the partition is live and active (i.e. passing through batches)
func (b *BackfillOperator) IsLive(partitionID int) bool {
	info := &b.partitionIterators[partitionID]
	if !info.initialised || info.lagging {
		return false
	}
	return true
}

func (b *BackfillOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	info := &b.partitionIterators[execCtx.PartitionID()]
	if !info.initialised {
		if err := b.initialiseIterator(execCtx, info); err != nil {
			return nil, err
		}
	}
	if execCtx.BackFill() {
		if info.closed.Load() { // Set from a different GR hence it is atomic
			if info.initialised {
				if info.iter != nil {
					info.iter.Close()
				}
				info.initialised = false
				info.lagging = false
			}
			return nil, nil
		}
		if batch == nil {
			// This is an empty batch to trigger the fill process
			if !info.lagging {
				// The partition might have been loaded and found to not be lagging, so nothing to do
				return nil, nil
			}
			loaded, err := b.loadAndIngestBackfillBatch(execCtx.Processor(), execCtx.PartitionID())
			if err != nil {
				return nil, err
			}
			if !loaded {
				// Nothing loaded so not lagging
				info.lagging = false
			}
			return nil, nil
		} else {
			_, err := b.handleStreamBatch(batch, execCtx)
			if err != nil {
				return nil, err
			}
			if info.loadedAllRows {
				info.lagging = false
			}
			if info.lagging {
				// We're still lagging so load the next batch
				_, err := b.loadAndIngestBackfillBatch(execCtx.Processor(), execCtx.PartitionID())
				if err != nil {
					return nil, err
				}
				return nil, nil
			}
			return nil, nil
		}
	}
	if info.lagging {
		// While we're back-filling we don't let any batches through
		return nil, nil
	}
	return b.handleStreamBatch(batch, execCtx)
}

func (b *BackfillOperator) handleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if !b.storeCommittedAsync && batch != nil && batch.RowCount != 0 {
		lastOffset := int(batch.GetIntColumn(0).Get(batch.RowCount - 1))
		b.storeCommittedOffSetForPartition(execCtx.PartitionID(), int64(lastOffset), execCtx)
	}
	return batch, b.sendBatchDownStream(batch, execCtx)
}

func (b *BackfillOperator) loadAndIngestBackfillBatch(processor proc.Processor, partitionID int) (bool, error) {
	batch, err := b.loadBatchForPartition(partitionID)
	if err != nil {
		return false, err
	}
	if batch.RowCount == 0 {
		return false, nil
	}
	pb := proc.NewProcessBatch(processor.ID(), batch, b.receiverID, partitionID, -1)
	pb.BackFill = true
	processor.IngestBatch(pb, func(err error) {
		if err != nil {
			log.Warnf("%s failed to ingest back-fill batch %v", b.cfg.LogScope, err)
		}
	})
	return true, nil
}

func (b *BackfillOperator) InSchema() *OperatorSchema {
	return b.schema
}

func (b *BackfillOperator) OutSchema() *OperatorSchema {
	return b.schema
}

func (b *BackfillOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(b.receiverID, b)
	processors := mgr.ProcessorManager().RegisterListener(b.id, b.processorChange)
	for _, p := range processors {
		b.processorChange(p, true, false)
	}
	b.stopLock.Lock()
	defer b.stopLock.Unlock()
	return nil
}

func (b *BackfillOperator) Teardown(mgr StreamManagerCtx, cb func(error)) {
	b.stopLock.Lock()
	defer b.stopLock.Unlock()
	b.stopped = true
	mgr.ProcessorManager().UnregisterListener(b.id)
	mgr.UnregisterReceiver(b.receiverID)
	b.timers.Range(func(key, value any) bool {
		value.(*common.TimerHandle).Stop()
		return true
	})
	cb(nil)
}

func (b *BackfillOperator) processorChange(processor proc.Processor, started bool, _ bool) {
	if err := b.processorsChanged(processor, started); err != nil {
		log.Errorf("%s failed to create or stop iterators %v", b.cfg.LogScope, err)
	}
}

const processorUnavailabilityRetryInterval = 250 * time.Millisecond

func (b *BackfillOperator) fireEmptyBatch(partitionID int, processor proc.Processor) {
	pb := proc.NewProcessBatch(-1, nil, b.receiverID, partitionID, -1)
	pb.BackFill = true
	processor.IngestBatch(pb, func(err error) {
		if err != nil {
			// if backfill starts up very soon after startup processor might not be initialised yet so ingest fails.
			// therefore we retry
			if common.IsUnavailableError(err) {
				// retry after delay
				tz := common.ScheduleTimer(processorUnavailabilityRetryInterval, true, func() {
					b.stopLock.Lock()
					defer b.stopLock.Unlock()
					if b.stopped {
						return
					}
					b.timers.Delete(partitionID)
					b.fireEmptyBatch(partitionID, processor)
				})
				b.timers.Store(partitionID, tz)
			} else {
				log.Errorf("%s failed to ingest empty batch1 %v", b.cfg.LogScope, err)
			}
		}
	})
}

func (b *BackfillOperator) processorsChanged(processor proc.Processor, started bool) error {
	partitions, ok := b.schema.PartitionScheme.ProcessorPartitionMapping[processor.ID()]
	if !ok {
		return nil
	}
	if started {
		for _, partID := range partitions {
			// fire off an empty batch - this will be executed on the event loop and cause the fill process to start
			// if it needs to
			b.fireEmptyBatch(partID, processor)
		}
	} else {
		// NOTE - currently this never gets called - processors get started, but because we currently
		// do rebalance, they never get stopped while the node is not crashed
		// (this would only happen on a node crash)
		for _, partID := range partitions {
			uPartID := uint64(partID)
			info := &b.partitionIterators[uPartID]
			info.closed.Store(true)
			// fire off an empty batch to close the iterator etc
			pb := proc.NewProcessBatch(-1, nil, b.receiverID, partID, -1)
			pb.BackFill = true
			processor.IngestBatch(pb, func(err error) {
				if err != nil {
					log.Errorf("%s failed to ingest empty batch2 %v", b.cfg.LogScope, err)
				}
			})
		}
	}
	return nil
}

func (b *BackfillOperator) initialiseIterator(execCtx StreamExecContext, info *iteratorInfo) error {
	execCtx.CheckInProcessorLoop()
	offset, ok, err := b.loadCommittedOffsetForPartition(execCtx)
	if err != nil {
		return err
	}
	if !ok {
		// We start at the beginning
		offset = 0
	} else {
		// Start at one past the last committed
		offset++
	}
	return b.initialiseIteratorAtOffset(execCtx, offset, info)
}

func (b *BackfillOperator) initialiseIteratorAtOffset(execCtx StreamExecContext, offset int64, info *iteratorInfo) error {
	partitionHash := b.hashCache.getHash(execCtx.PartitionID())
	keyStart := encoding.EncodeEntryPrefix(partitionHash, uint64(b.fromSlabID), 33)
	keyStart = append(keyStart, 1) // not null
	keyStart = encoding.KeyEncodeInt(keyStart, offset)
	keyEnd := encoding.EncodeEntryPrefix(partitionHash, uint64(b.fromSlabID)+1, 24)
	fact := func(ks []byte, ke []byte) (iteration.Iterator, error) {
		return execCtx.Processor().NewIterator(ks, ke, math.MaxInt64, false)
	}
	iter, err := newUpdatableIterator(keyStart, keyEnd, fact)
	if err != nil {
		return err
	}
	info.lagging = true
	info.iter = iter
	info.partID = execCtx.PartitionID()
	info.initialised = true
	return nil
}

func (b *BackfillOperator) loadBatchForPartition(partID int) (*evbatch.Batch, error) {
	info := &b.partitionIterators[partID]
	if info.iter == nil {
		panic("cannot find partition iterator")
	}
	colBuilders := evbatch.CreateColBuilders(b.schema.EventSchema.ColumnTypes())
	rowCount := 0
	for rowCount < b.maxBackfillBatchSize {
		valid, err := info.iter.IsValid()
		if err != nil {
			return nil, err
		}
		if !valid {
			// No more rows
			info.loadedAllRows = true
			info.lagging = false
			break
		}
		curr := info.iter.Current()
		k, _ := encoding.KeyDecodeInt(curr.Key, 25) // 17 as 1 byte null marker before offset
		colBuilders[0].(*evbatch.IntColBuilder).Append(k)
		buff := curr.Value
		byteOff := 0
		for _, rowCol := range b.rowCols {
			isNull := buff[byteOff] == 0
			byteOff++
			if isNull {
				colBuilders[rowCol].AppendNull()
				continue
			}
			colType := b.schema.EventSchema.ColumnTypes()[rowCol]
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				var u uint64
				u, byteOff = encoding.ReadUint64FromBufferLE(buff, byteOff)
				colBuilders[rowCol].(*evbatch.IntColBuilder).Append(int64(u))
			case types.ColumnTypeIDFloat:
				var f float64
				f, byteOff = encoding.ReadFloat64FromBufferLE(buff, byteOff)
				colBuilders[rowCol].(*evbatch.FloatColBuilder).Append(f)
			case types.ColumnTypeIDBool:
				var b bool
				b, byteOff = encoding.DecodeBool(buff, byteOff)
				colBuilders[rowCol].(*evbatch.BoolColBuilder).Append(b)
			case types.ColumnTypeIDDecimal:
				decType := colType.(*types.DecimalType)
				var dec types.Decimal
				dec, byteOff = encoding.ReadDecimalFromBuffer(buff, byteOff)
				dec.Precision = decType.Precision
				dec.Scale = decType.Scale
				colBuilders[rowCol].(*evbatch.DecimalColBuilder).Append(dec)
			case types.ColumnTypeIDString:
				var s string
				s, byteOff = encoding.ReadStringFromBufferLE(buff, byteOff)
				colBuilders[rowCol].(*evbatch.StringColBuilder).Append(s)
			case types.ColumnTypeIDBytes:
				var b []byte
				b, byteOff = encoding.ReadBytesFromBufferLE(buff, byteOff)
				colBuilders[rowCol].(*evbatch.BytesColBuilder).Append(b)
			case types.ColumnTypeIDTimestamp:
				var u uint64
				u, byteOff = encoding.ReadUint64FromBufferLE(buff, byteOff)
				ts := types.NewTimestamp(int64(u))
				colBuilders[rowCol].(*evbatch.TimestampColBuilder).Append(ts)
			default:
				panic("unknown type")
			}
		}
		rowCount++
		err = info.iter.Next()
		if err != nil {
			return nil, err
		}
	}
	batch := evbatch.NewBatchFromBuilders(b.schema.EventSchema, colBuilders...)
	log.Infof("%s backfill loaded offsets %s for partition %d", b.cfg.LogScope, getOffsets(batch), partID)
	return batch, nil
}

// updatableIterator ensures data that was added since creation gets seen
func newUpdatableIterator(keyStart []byte, keyEnd []byte, iterFactory func(keyStart []byte, keyEnd []byte) (iteration.Iterator, error)) (iteration.Iterator, error) {
	iter, err := iterFactory(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	return &updatableIterator{
		keyStart:    keyStart,
		keyEnd:      keyEnd,
		iter:        iter,
		iterFactory: iterFactory,
	}, nil
}

type updatableIterator struct {
	lastKey     []byte
	keyStart    []byte
	keyEnd      []byte
	iter        iteration.Iterator
	iterFactory func(keyStart []byte, keyEnd []byte) (iteration.Iterator, error)
}

func (u *updatableIterator) Current() common.KV {
	curr := u.iter.Current()
	u.lastKey = curr.Key
	return curr
}

func (u *updatableIterator) Next() error {
	return u.iter.Next()
}

func (u *updatableIterator) IsValid() (bool, error) {
	valid, err := u.iter.IsValid()
	if err != nil {
		return false, err
	}
	if valid {
		return true, nil
	}
	// If not valid we recreate the iterator and call IsValid() again.
	// This means we will pick up newly added data
	u.iter.Close()
	start := u.keyStart
	if u.lastKey != nil {
		start = common.IncrementBytesBigEndian(u.lastKey)
	}
	u.iter, err = u.iterFactory(start, u.keyEnd)
	if err != nil {
		return false, err
	}
	return u.iter.IsValid()
}

func (u *updatableIterator) Close() {
	u.iter.Close()
}
