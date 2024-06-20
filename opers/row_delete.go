package opers

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"sync"
)

type RowDeleteOperator struct {
	schema  *OperatorSchema
	slabID  uint64
	keyCols []int
	rowCols []int
}

func NewRowDeleteOperator(schema *OperatorSchema, slabID int, keyCols []int) (*RowDeleteOperator, error) {
	// We create a schema with just the key cols
	var columnNames []string
	var columnTypes []types.ColumnType
	for _, kc := range keyCols {
		columnNames = append(columnNames, schema.EventSchema.ColumnNames()[kc])
		columnTypes = append(columnTypes, schema.EventSchema.ColumnTypes()[kc])
	}
	keySchema := evbatch.NewEventSchema(columnNames, columnTypes)
	return &RowDeleteOperator{
		schema: &OperatorSchema{
			EventSchema:     keySchema,
			PartitionScheme: schema.PartitionScheme,
		},
		slabID:  uint64(slabID),
		keyCols: keyCols,
	}, nil
}

func (rd *RowDeleteOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in queries")
}

func (rd *RowDeleteOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	partitionHash := proc.CalcPartitionHash(rd.schema.MappingID, uint64(execCtx.PartitionID()))
	prefix := encoding.EncodeEntryPrefix(partitionHash, rd.slabID, 24)
	for i := 0; i < batch.RowCount; i++ {
		// Note: The incoming batch will only have the key cols
		keyBuff := make([]byte, 24, 64)
		copy(keyBuff, prefix)
		keyBuff = evbatch.EncodeKeyCols(batch, i, rd.keyCols, keyBuff)
		keyBuff = encoding.EncodeVersion(keyBuff, uint64(execCtx.WriteVersion()))
		execCtx.StoreEntry(common.KV{
			Key: keyBuff,
		}, true)
	}
	return batch, nil
}

func (rd *RowDeleteOperator) HandleBarrier(StreamExecContext) error {
	panic("not used")
}

func (rd *RowDeleteOperator) InSchema() *OperatorSchema {
	return rd.schema
}

func (rd *RowDeleteOperator) OutSchema() *OperatorSchema {
	return nil
}

func (rd *RowDeleteOperator) AddDownStreamOperator(Operator) {
}

func (rd *RowDeleteOperator) GetDownStreamOperators() []Operator {
	return nil
}

func (rd *RowDeleteOperator) RemoveDownStreamOperator(Operator) {
}

func (rd *RowDeleteOperator) GetParentOperator() Operator {
	return nil
}

func (rd *RowDeleteOperator) SetParentOperator(Operator) {
}

func (rd *RowDeleteOperator) SetStreamInfo(*StreamInfo) {
}

func (rd *RowDeleteOperator) GetStreamInfo() *StreamInfo {
	return nil
}

func (rd *RowDeleteOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (rd *RowDeleteOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}
