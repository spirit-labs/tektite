package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"sync"
)

const OffsetColName = "offset"
const EventTimeColName = "event_time"

const rowInitialBufferSize = 64

type Operator interface {
	HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error)
	HandleQueryBatch(batch *evbatch.Batch, execCtx QueryExecContext) (*evbatch.Batch, error)
	HandleBarrier(execCtx StreamExecContext) error
	InSchema() *OperatorSchema
	OutSchema() *OperatorSchema
	Setup(mgr StreamManagerCtx) error
	AddDownStreamOperator(downstream Operator)
	RemoveDownStreamOperator(downstream Operator)
	GetDownStreamOperators() []Operator
	GetParentOperator() Operator
	SetParentOperator(operator Operator)
	SetStreamInfo(info *StreamInfo)
	GetStreamInfo() *StreamInfo
	Teardown(mgr StreamManagerCtx, lock *sync.RWMutex)
}

type OperatorSchema struct {
	EventSchema *evbatch.EventSchema
	PartitionScheme
}

type PartitionScheme struct {
	MappingID                 string
	Partitions                int
	MaxPartitionID            int
	MaxProcessorID            int
	RawPartitionKey           bool
	ProcessorIDs              []int
	PartitionProcessorMapping map[int]int
	ProcessorPartitionMapping map[int][]int
}

func NewPartitionScheme(mappingID string, partitions int, rawPartitionKey bool, numProcessors int) PartitionScheme {
	processorPartitionMapping := CalcProcessorPartitionMapping(mappingID, partitions, numProcessors)
	var processorIDs []int
	maxPartitionID := 0
	maxProcessorID := 0
	for processorID, partIDs := range processorPartitionMapping {
		if processorID > maxProcessorID {
			maxProcessorID = processorID
		}
		processorIDs = append(processorIDs, processorID)
		for _, partID := range partIDs {
			if partID > maxPartitionID {
				maxPartitionID = partID
			}
		}
	}
	return PartitionScheme{
		MappingID:                 mappingID,
		Partitions:                partitions,
		MaxPartitionID:            maxPartitionID,
		MaxProcessorID:            maxProcessorID,
		RawPartitionKey:           rawPartitionKey,
		ProcessorIDs:              processorIDs,
		PartitionProcessorMapping: CalcPartitionProcessorMapping(mappingID, partitions, numProcessors),
		ProcessorPartitionMapping: processorPartitionMapping,
	}
}

func (o *OperatorSchema) Copy() *OperatorSchema {
	return &OperatorSchema{
		EventSchema:     o.EventSchema,
		PartitionScheme: o.PartitionScheme,
	}
}

func HasOffsetColumn(eventSchema *evbatch.EventSchema) bool {
	cn := eventSchema.ColumnNames()
	return len(cn) >= 1 && cn[0] == OffsetColName
}

func createInColIndexMap(schema *evbatch.EventSchema) map[string]int {
	indexMap := make(map[string]int, len(schema.ColumnNames()))
	for i, inName := range schema.ColumnNames() {
		indexMap[inName] = i
	}
	return indexMap
}

func storeBatchInTable(batch *evbatch.Batch, keyCols []int, rowCols []int,
	keyPrefix []byte, execCtx StreamExecContext, nodeID int, noCache bool) {
	for i := 0; i < batch.RowCount; i++ {
		keyBuff := evbatch.EncodeKeyCols(batch, i, keyCols, keyPrefix)
		rowBuff := make([]byte, 0, rowInitialBufferSize)
		rowBuff = evbatch.EncodeRowCols(batch, i, rowCols, rowBuff)
		keyBuff = encoding.EncodeVersion(keyBuff, uint64(execCtx.WriteVersion()))
		if execCtx.WriteVersion() < 0 {
			panic(fmt.Sprintf("invalid write version: %d", execCtx.WriteVersion()))
		}
		if log.DebugEnabled {
			log.Debugf("node %d storing key %v (%s) value %v (%s) with version %d", nodeID, keyBuff, string(keyBuff),
				rowBuff, string(rowBuff), execCtx.WriteVersion())
		}
		execCtx.StoreEntry(common.KV{
			Key:   keyBuff,
			Value: rowBuff,
		}, noCache)
	}
}

type BaseOperator struct {
	parentOperator          Operator
	downstreamOperators     []Operator
	downstreamOperatorsLock sync.RWMutex
	streamInfo              *StreamInfo
}

func (b *BaseOperator) AddDownStreamOperator(downstream Operator) {
	b.downstreamOperatorsLock.Lock()
	defer b.downstreamOperatorsLock.Unlock()
	b.downstreamOperators = append(b.downstreamOperators, downstream)
}

func (b *BaseOperator) RemoveDownStreamOperator(downstream Operator) {
	b.downstreamOperatorsLock.Lock()
	defer b.downstreamOperatorsLock.Unlock()
	var newDownStream []Operator
	found := false
	for _, ds := range b.downstreamOperators {
		if ds != downstream {
			newDownStream = append(newDownStream, ds)
		} else {
			found = true
		}
	}
	if !found {
		panic("cannot find downstream to remove")
	}
	b.downstreamOperators = newDownStream
}

func (b *BaseOperator) GetDownStreamOperators() []Operator {
	b.downstreamOperatorsLock.RLock()
	defer b.downstreamOperatorsLock.RUnlock()
	downstream := make([]Operator, len(b.downstreamOperators))
	copy(downstream, b.downstreamOperators)
	return downstream
}

func (b *BaseOperator) GetParentOperator() Operator {
	return b.parentOperator
}

func (b *BaseOperator) SetParentOperator(operator Operator) {
	b.parentOperator = operator
}

func (b *BaseOperator) SetStreamInfo(info *StreamInfo) {
	b.streamInfo = info
}

func (b *BaseOperator) GetStreamInfo() *StreamInfo {
	return b.streamInfo
}

func (b *BaseOperator) HandleBarrier(execCtx StreamExecContext) error {
	b.downstreamOperatorsLock.RLock()
	defer b.downstreamOperatorsLock.RUnlock()
	for _, downstream := range b.downstreamOperators {
		if err := downstream.HandleBarrier(execCtx); err != nil {
			return err
		}
	}
	return nil
}

func (b *BaseOperator) sendBatchDownStream(batch *evbatch.Batch, execCtx StreamExecContext) error {
	b.downstreamOperatorsLock.RLock()
	defer b.downstreamOperatorsLock.RUnlock()
	for _, downstream := range b.downstreamOperators {
		if _, err := downstream.HandleStreamBatch(batch, execCtx); err != nil {
			return err
		}
	}
	return nil
}

func (b *BaseOperator) SendQueryBatchDownStream(batch *evbatch.Batch, execCtx QueryExecContext) error {
	// Don't need to lock for queries as downstream operators are always setup before any query is handled and never changed
	ld := len(b.downstreamOperators)
	if ld > 0 {
		if ld != 1 {
			panic("unexpected number of downstream operators for query")
		}
		_, err := b.downstreamOperators[0].HandleQueryBatch(batch, execCtx)
		return err
	}
	return nil
}

func LoadColsFromKey(colBuilders []evbatch.ColumnBuilder, keyColumnTypes []types.ColumnType, keyColIndexes []int,
	keyBuff []byte) error {
	off := 16
	for i, colIndex := range keyColIndexes {
		isNull := keyBuff[off] == 0
		off++
		colBuilder := colBuilders[colIndex]
		if isNull {
			colBuilder.AppendNull()
			continue
		}
		colType := keyColumnTypes[i]
		var err error
		switch colType.ID() {
		case types.ColumnTypeIDInt:
			var val int64
			val, off = encoding.KeyDecodeInt(keyBuff, off)
			colBuilder.(*evbatch.IntColBuilder).Append(val)
		case types.ColumnTypeIDFloat:
			var val float64
			val, off = encoding.KeyDecodeFloat(keyBuff, off)
			colBuilder.(*evbatch.FloatColBuilder).Append(val)
		case types.ColumnTypeIDBool:
			var val bool
			val, off = encoding.DecodeBool(keyBuff, off)
			colBuilder.(*evbatch.BoolColBuilder).Append(val)
		case types.ColumnTypeIDDecimal:
			var val types.Decimal
			val, off = encoding.KeyDecodeDecimal(keyBuff, off)
			colBuilder.(*evbatch.DecimalColBuilder).Append(val)
		case types.ColumnTypeIDString:
			var val string
			val, off, err = encoding.KeyDecodeString(keyBuff, off)
			if err != nil {
				return err
			}
			colBuilder.(*evbatch.StringColBuilder).Append(val)
		case types.ColumnTypeIDBytes:
			var val []byte
			val, off, err = encoding.KeyDecodeBytes(keyBuff, off)
			if err != nil {
				return err
			}
			colBuilder.(*evbatch.BytesColBuilder).Append(val)
		case types.ColumnTypeIDTimestamp:
			var val types.Timestamp
			val, off = encoding.KeyDecodeTimestamp(keyBuff, off)
			colBuilder.(*evbatch.TimestampColBuilder).Append(val)
		default:
			panic("unknown type")
		}
	}
	return nil
}

func LoadColsFromValue(colBuilders []evbatch.ColumnBuilder, rowColumnTypes []types.ColumnType, rowColIndexes []int, valueBuff []byte) {
	off := 0
	for i, colIndex := range rowColIndexes {
		isNull := valueBuff[off] == 0
		off++
		colBuilder := colBuilders[colIndex]
		if isNull {
			colBuilder.AppendNull()
			continue
		}
		colType := rowColumnTypes[i]
		switch colType.ID() {
		case types.ColumnTypeIDInt:
			var val uint64
			val, off = encoding.ReadUint64FromBufferLE(valueBuff, off)
			colBuilder.(*evbatch.IntColBuilder).Append(int64(val))
		case types.ColumnTypeIDFloat:
			var val float64
			val, off = encoding.ReadFloat64FromBufferLE(valueBuff, off)
			colBuilder.(*evbatch.FloatColBuilder).Append(val)
		case types.ColumnTypeIDBool:
			var val bool
			val, off = encoding.ReadBoolFromBuffer(valueBuff, off)
			colBuilder.(*evbatch.BoolColBuilder).Append(val)
		case types.ColumnTypeIDDecimal:
			var val types.Decimal
			val, off = encoding.ReadDecimalFromBuffer(valueBuff, off)
			colBuilder.(*evbatch.DecimalColBuilder).Append(val)
		case types.ColumnTypeIDString:
			var val string
			val, off = encoding.ReadStringFromBufferLE(valueBuff, off)
			colBuilder.(*evbatch.StringColBuilder).Append(val)
		case types.ColumnTypeIDBytes:
			var val []byte
			val, off = encoding.ReadBytesFromBufferLE(valueBuff, off)
			colBuilder.(*evbatch.BytesColBuilder).Append(val)
		case types.ColumnTypeIDTimestamp:
			var u uint64
			u, off = encoding.ReadUint64FromBufferLE(valueBuff, off)
			ts := types.NewTimestamp(int64(u))
			colBuilder.(*evbatch.TimestampColBuilder).Append(ts)
		default:
			panic("unknown type")
		}
	}
}
