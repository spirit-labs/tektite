package opers

import (
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"sync"
)

func NewFilterOperator(schema *OperatorSchema, exprDesc parser.ExprDesc, expressionFactory *expr.ExpressionFactory) (*FilterOperator, error) {
	ok, exprDesc, alias, aliasExprDesc := parser.ExtractAlias(exprDesc)
	if !ok || alias != "" {
		return nil, aliasExprDesc.ErrorAtPosition("filter expressions must not have an alias")
	}
	e, err := expressionFactory.CreateExpression(exprDesc, schema.EventSchema)
	if err != nil {
		return nil, err
	}
	fo := &FilterOperator{
		schema: schema,
		expr:   e,
	}
	return fo, nil
}

type FilterOperator struct {
	BaseOperator
	schema *OperatorSchema
	expr   expr.Expression
}

func (f *FilterOperator) HandleQueryBatch(batch *evbatch.Batch, execCtx QueryExecContext) (*evbatch.Batch, error) {
	outBatch, err := f.processBatch(batch)
	if err != nil {
		return nil, err
	}
	return outBatch, f.SendQueryBatchDownStream(outBatch, execCtx)
}

func (f *FilterOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	outBatch, err := f.processBatch(batch)
	if err != nil {
		return nil, err
	}
	if outBatch.RowCount > 0 {
		return outBatch, f.sendBatchDownStream(outBatch, execCtx)
	}
	return outBatch, nil
}

func (f *FilterOperator) processBatch(batch *evbatch.Batch) (*evbatch.Batch, error) {
	defer batch.Release()
	colBuilders := evbatch.CreateColBuilders(f.schema.EventSchema.ColumnTypes())
	for rowIndex := 0; rowIndex < batch.RowCount; rowIndex++ {
		accept, null, err := f.expr.EvalBool(rowIndex, batch)
		if err != nil {
			return nil, err
		}
		if !null && accept {
			for colIndex, ft := range f.schema.EventSchema.ColumnTypes() {
				evbatch.CopyColumnEntry(ft, colBuilders, colIndex, rowIndex, batch)
			}
		}
	}
	return evbatch.NewBatchFromBuilders(f.OutSchema().EventSchema, colBuilders...), nil
}

func (f *FilterOperator) InSchema() *OperatorSchema {
	return f.schema
}

func (f *FilterOperator) OutSchema() *OperatorSchema {
	return f.schema
}

func (f *FilterOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (f *FilterOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}
