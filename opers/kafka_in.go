package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"time"
)

var RecordBatchSchema = evbatch.NewEventSchema([]string{"record_batch"}, []types.ColumnType{types.ColumnTypeBytes})

func NewKafkaInOperator(mappingID string, offsetsSlabID int, receiverID int, partitions int,
	useServerTimestamp bool, numProcessors int) *KafkaInOperator {
	partitionScheme := NewPartitionScheme(mappingID, partitions, true, numProcessors)
	inSchema := &OperatorSchema{
		EventSchema:     RecordBatchSchema,
		PartitionScheme: partitionScheme,
	}
	outSchema := &OperatorSchema{
		EventSchema:     KafkaSchema,
		PartitionScheme: partitionScheme,
	}
	nextOffsets := make([]int64, partitions)
	for i := range nextOffsets {
		nextOffsets[i] = -1
	}
	return &KafkaInOperator{
		offsetsSlabID:           offsetsSlabID,
		inSchema:                inSchema,
		outSchema:               outSchema,
		receiverID:              receiverID,
		useServerTimestamp:      useServerTimestamp,
		nextOffsets:             nextOffsets,
		lastAppendTimes:         make([]int64, partitions),
		hashCache:               newPartitionHashCache(inSchema.MappingID, inSchema.Partitions),
		producerSequenceNumbers: map[int]int{},
	}
}

type KafkaInOperator struct {
	BaseOperator
	inSchema                *OperatorSchema
	outSchema               *OperatorSchema
	offsetsSlabID           int
	receiverID              int
	useServerTimestamp      bool
	nextOffsets             []int64
	lastAppendTimes         []int64
	watermarkOperator       *WaterMarkOperator
	hashCache               *partitionHashCache
	producerSequenceNumbers map[int]int
}

func (k *KafkaInOperator) PartitionScheme() *PartitionScheme {
	return &k.inSchema.PartitionScheme
}

func (k *KafkaInOperator) GetPartitionProcessorMapping() map[int]int {
	return k.inSchema.PartitionScheme.PartitionProcessorMapping
}

func (k *KafkaInOperator) ReceiverID() int {
	return k.receiverID
}

func (k *KafkaInOperator) GetLastProducedInfo(partitionID int) (int64, int64) {
	// Doesn't need locking as always called on same processor loop (GR) that set last offset
	return k.nextOffsets[partitionID] - 1, k.lastAppendTimes[partitionID]
}

func (k *KafkaInOperator) IngestBatch(recordBatchBytes []byte, processor proc.Processor, partitionID int,
	complFunc func(err error)) {
	bytesColBuilder := evbatch.NewBytesColBuilder()
	bytesColBuilder.Append(recordBatchBytes)
	evBatch := evbatch.NewBatch(RecordBatchSchema, bytesColBuilder.Build())
	processBatch := proc.NewProcessBatch(processor.ID(), evBatch,
		k.receiverID, partitionID, -1)
	processor.GetReplicator().ReplicateBatch(processBatch, complFunc)
}

func (k *KafkaInOperator) GetIdempotentProducerMetadata(producerID int) (int, bool) {
	value, exists := k.producerSequenceNumbers[producerID]
	return value, exists
}

func (k *KafkaInOperator) SetIdempotentProducerMetadata(producerID int, value int) {
	if existingValue, exists := k.producerSequenceNumbers[producerID]; exists {
		k.producerSequenceNumbers[producerID] = max(existingValue, value)
	} else {
		k.producerSequenceNumbers[producerID] = value
	}
}

func (k *KafkaInOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	// Convert the recordset/messageset into the tektite kafka schema
	bytes := batch.GetBytesColumn(0).Get(0)
	outBatch, maxEventTime, err := k.convertRecordset(bytes, execCtx)
	if err != nil {
		return nil, err
	}
	k.watermarkOperator.updateMaxEventTime(int(maxEventTime), execCtx.Processor().ID())
	return nil, k.sendBatchDownStream(outBatch, execCtx)
}

func (k *KafkaInOperator) getNextOffset(partitionID int, processor proc.Processor) (int64, error) {
	nextOffset := k.nextOffsets[partitionID]
	if nextOffset != -1 {
		return nextOffset, nil
	}
	// Load from store
	partitionHash := k.hashCache.getHash(partitionID)
	return loadOffset(partitionHash, k.offsetsSlabID, processor)
}

func (k *KafkaInOperator) convertRecordset(bytes []byte, execCtx StreamExecContext) (*evbatch.Batch, int64, error) {
	var appendTime int64
	if k.useServerTimestamp {
		appendTime = time.Now().UTC().UnixMilli()
	}
	partitionID := execCtx.PartitionID()
	colBuilders := evbatch.CreateColBuilders(KafkaSchema.ColumnTypes())
	offsetCol := colBuilders[0].(*evbatch.IntColBuilder)
	timestampCol := colBuilders[1].(*evbatch.TimestampColBuilder)
	keyCol := colBuilders[2].(*evbatch.BytesColBuilder)
	headersCol := colBuilders[3].(*evbatch.BytesColBuilder)
	valueCol := colBuilders[4].(*evbatch.BytesColBuilder)
	baseTimeStamp := int64(binary.BigEndian.Uint64(bytes[27:]))
	off := 57
	numRecords := int(binary.BigEndian.Uint32(bytes[off:]))
	off += 4
	kOffset, err := k.getNextOffset(partitionID, execCtx.Processor())
	if err != nil {
		return nil, 0, err
	}
	var lastTimestamp int64
	var maxTimestamp int64
	for i := 0; i < numRecords; i++ {
		recordLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		recordStart := off
		off++ // skip past attributes
		timestampDelta, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		_, bytesRead = binary.Varint(bytes[off:])
		off += bytesRead
		keyLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		var key []byte
		if keyLength != -1 {
			ikl := int(keyLength)
			key = bytes[off : off+ikl]
			off += ikl
		}
		valueLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ivl := int(valueLength)
		value := bytes[off : off+ivl]
		off += ivl
		headersEnd := recordStart + int(recordLength)
		headers := bytes[off:headersEnd]
		off = headersEnd
		offsetCol.Append(kOffset)
		if k.useServerTimestamp {
			lastTimestamp = appendTime
		} else {
			lastTimestamp = baseTimeStamp + timestampDelta
		}
		timestampCol.Append(types.NewTimestamp(lastTimestamp))
		if lastTimestamp > maxTimestamp {
			maxTimestamp = lastTimestamp
		}
		if key == nil {
			keyCol.AppendNull()
		} else {
			keyCol.Append(key)
		}
		// Note, we append the raw headers, including the length (varint), so its never null
		headersCol.Append(headers)
		if len(value) == 0 {
			valueCol.AppendNull()
		} else {
			valueCol.Append(value)
		}
		kOffset++
	}
	k.nextOffsets[partitionID] = kOffset
	k.lastAppendTimes[partitionID] = lastTimestamp
	partitionHash := k.hashCache.getHash(partitionID)
	storeOffset(execCtx, kOffset, partitionHash, k.offsetsSlabID, execCtx.WriteVersion())
	return evbatch.NewBatchFromBuilders(KafkaSchema, colBuilders...), maxTimestamp, nil
}

func (k *KafkaInOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not suported for streams")
}

func (k *KafkaInOperator) InSchema() *OperatorSchema {
	return k.inSchema
}

func (k *KafkaInOperator) OutSchema() *OperatorSchema {
	return k.outSchema
}

func (k *KafkaInOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(k.receiverID, k)
	return nil
}

func (k *KafkaInOperator) Teardown(mgr StreamManagerCtx, completeCB func(error)) {
	mgr.UnregisterReceiver(k.receiverID)
	completeCB(nil)
}

func (k *KafkaInOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return k.HandleStreamBatch(batch, execCtx)
}

func (k *KafkaInOperator) ReceiveBarrier(execCtx StreamExecContext) error {
	k.watermarkOperator.setWatermark(execCtx)
	return k.HandleBarrier(execCtx)
}

func (k *KafkaInOperator) RequiresBarriersInjection() bool {
	return true
}

func (k *KafkaInOperator) ForwardingProcessorCount() int {
	return len(k.inSchema.PartitionScheme.ProcessorIDs)
}

func (k *KafkaInOperator) GetWatermarkOperator() *WaterMarkOperator {
	return k.watermarkOperator
}
