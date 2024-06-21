// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
