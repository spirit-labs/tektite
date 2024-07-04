package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
)

type ProjectOperator struct {
	BaseOperator
	inSchema    *OperatorSchema
	outSchema   *OperatorSchema
	expressions []expr.Expression
}

func NewProjectOperator(inSchema *OperatorSchema, exprDescs []parser.ExprDesc, includeSystemColumns bool,
	expressionFactory *expr.ExpressionFactory) (*ProjectOperator, error) {
	numExprs := len(exprDescs)
	inputHasOffset := HasOffsetColumn(inSchema.EventSchema)
	if includeSystemColumns {
		numExprs++
		if inputHasOffset {
			numExprs++
		}
	}
	outTypes := make([]types.ColumnType, numExprs)
	outNames := make([]string, numExprs)
	expressions := make([]expr.Expression, numExprs)
	index := 0
	if includeSystemColumns {
		if inputHasOffset {
			// Include offset column in output
			outTypes[0] = types.ColumnTypeInt
			outNames[0] = OffsetColName
			expressions[0] = expr.NewColumnExpression(0, types.ColumnTypeInt)
			index = 1
		}
		// Include event_time
		outTypes[index] = types.ColumnTypeTimestamp
		outNames[index] = EventTimeColName
		expressions[index] = expr.NewColumnExpression(index, types.ColumnTypeTimestamp)
		index++
	}
	for i, desc := range exprDescs {
		colExprDesc, isColExpr := desc.(*parser.IdentifierExprDesc)
		if isColExpr && includeSystemColumns && (colExprDesc.IdentifierName == OffsetColName || colExprDesc.IdentifierName == EventTimeColName) {
			// We disallow offset and event_time as they will be present anyway, and column names must be unique
			return nil, desc.ErrorAtPosition(
				"cannot specify column '%s' in projection, the columns '%s' and '%s' will always be included in the projection",
				colExprDesc.IdentifierName, OffsetColName, EventTimeColName)
		}
		ok, exprDesc, alias, aliasExprDesc := parser.ExtractAlias(desc)
		if !ok {
			return nil, desc.ErrorAtPosition("invalid alias")
		}
		var colName string
		if alias != "" {
			if isReservedIdentifierName(alias) {
				return nil, aliasExprDesc.ErrorAtPosition("cannot use alias '%s', it is a reserved name", alias)
			}
			colName = alias
		} else if isColExpr {
			colName = colExprDesc.IdentifierName
		} else {
			colName = fmt.Sprintf("col%d", i)
		}
		outNames[index] = colName
		e, err := expressionFactory.CreateExpression(exprDesc, inSchema.EventSchema)
		if err != nil {
			return nil, err
		}
		expressions[index] = e
		outTypes[index] = e.ResultType()
		index++
	}
	outEventSchema := evbatch.NewEventSchema(outNames, outTypes)
	outSchema := inSchema.Copy()
	outSchema.EventSchema = outEventSchema
	return &ProjectOperator{
		inSchema:    inSchema,
		outSchema:   outSchema,
		expressions: expressions,
	}, nil
}

func (f *ProjectOperator) HandleQueryBatch(batch *evbatch.Batch, execCtx QueryExecContext) (*evbatch.Batch, error) {
	outBatch, err := f.processBatch(batch)
	if err != nil {
		return nil, err
	}
	return outBatch, f.SendQueryBatchDownStream(outBatch, execCtx)
}

func (f *ProjectOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	outBatch, err := f.processBatch(batch)
	if err != nil {
		return nil, err
	}
	return outBatch, f.sendBatchDownStream(outBatch, execCtx)
}

func (f *ProjectOperator) processBatch(batch *evbatch.Batch) (*evbatch.Batch, error) {
	defer batch.Release()

	fTypes := f.outSchema.EventSchema.ColumnTypes()
	cols := make([]evbatch.Column, len(fTypes))
	for i, e := range f.expressions {
		col, err := expr.EvalColumn(e, batch)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	return evbatch.NewBatch(f.outSchema.EventSchema, cols...), nil
}

func (f *ProjectOperator) InSchema() *OperatorSchema {
	return f.inSchema
}

func (f *ProjectOperator) OutSchema() *OperatorSchema {
	return f.outSchema
}

func (f *ProjectOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (f *ProjectOperator) Teardown(mgr StreamManagerCtx, completeCB func(error)) {
	completeCB(nil)
}
