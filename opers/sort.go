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
	"bytes"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"sort"
	"strings"
	"sync"
)

type SortOperator struct {
	BaseOperator
	schema              *OperatorSchema
	expectedLastBatches int64
	sortExprs           []expr.Expression
	sortDesc            []bool
	useStableSort       bool
}

type SortState struct {
	lock           sync.Mutex
	numLastBatches int64
	batches        []*evbatch.Batch
}

func NewSortOperator(schema *OperatorSchema, expectedLastBatches int, sortByExprDescs []parser.ExprDesc,
	useStableSort bool, expressionFactory *expr.ExpressionFactory) (*SortOperator, error) {
	sortAscDesc := make([]bool, len(sortByExprDescs))
	sortExprs := make([]expr.Expression, len(sortByExprDescs))
	for i, exprDesc := range sortByExprDescs {
		exprDesc, ascDsc := parser.ExtractAscDesc(exprDesc)
		var desc bool
		if ascDsc == "desc" || ascDsc == "descending" {
			desc = true
		} else if ascDsc == "asc" || ascDsc == "ascending" {
			desc = false
		} else if ascDsc != "" {
			panic("invalid asc/desc")
		}
		sortAscDesc[i] = desc
		e, err := expressionFactory.CreateExpression(exprDesc, schema.EventSchema)
		if err != nil {
			return nil, err
		}
		sortExprs[i] = e
	}
	return &SortOperator{
		schema:              schema,
		expectedLastBatches: int64(expectedLastBatches),
		sortExprs:           sortExprs,
		sortDesc:            sortAscDesc,
		useStableSort:       useStableSort,
	}, nil
}

func (s *SortOperator) HandleQueryBatch(batch *evbatch.Batch, execCtx QueryExecContext) (*evbatch.Batch, error) {
	sortState := execCtx.ExecState().(*SortState)
	sortState.lock.Lock()
	defer sortState.lock.Unlock()
	sortState.batches = append(sortState.batches, batch)
	if execCtx.Last() {
		sortState.numLastBatches++
		if sortState.numLastBatches == s.expectedLastBatches {
			return s.sortBatches(sortState)
		}
	}
	return nil, nil
}

func (s *SortOperator) sortBatches(sortState *SortState) (*evbatch.Batch, error) {
	columnTypes := s.schema.EventSchema.ColumnTypes()
	// Combine the batches if necessary
	var unsortedBatch *evbatch.Batch
	if len(sortState.batches) > 1 {
		unsortedBatchBuilders := evbatch.CreateColBuilders(columnTypes)
		for _, batch := range sortState.batches {
			for colIndex, colType := range columnTypes {
				for rowIndex := 0; rowIndex < batch.RowCount; rowIndex++ {
					evbatch.CopyColumnEntry(colType, unsortedBatchBuilders, colIndex, rowIndex, batch)
				}
			}
			batch.Release()
		}
		unsortedBatch = evbatch.NewBatchFromBuilders(s.schema.EventSchema, unsortedBatchBuilders...)
	} else {
		unsortedBatch = sortState.batches[0]
	}
	defer unsortedBatch.Release()
	index := make([]int, unsortedBatch.RowCount)
	for i := range index {
		index[i] = i
	}

	// Sort the index
	var err error
	index, err = s.sortIndex(index, unsortedBatch)
	if err != nil {
		return nil, err
	}

	// Then assemble the output batch in sorted index order
	sortedBatchBuilders := evbatch.CreateColBuilders(columnTypes)
	for colIndex, colType := range columnTypes {
		for _, rowIndex := range index {
			evbatch.CopyColumnEntry(colType, sortedBatchBuilders, colIndex, rowIndex, unsortedBatch)
		}
	}
	return evbatch.NewBatchFromBuilders(s.schema.EventSchema, sortedBatchBuilders...), nil
}

func (s *SortOperator) sortIndex(index []int, unsorted *evbatch.Batch) ([]int, error) {
	// We first evaluate the expressions for all the sort columns to give us a new set of columns
	numSortExprs := len(s.sortExprs)
	sortCols := make([]evbatch.Column, numSortExprs)
	sortColTypes := make([]types.ColumnType, numSortExprs)
	for i, sortExpr := range s.sortExprs {
		colType := sortExpr.ResultType()
		sortCol, err := expr.EvalColumn(sortExpr, unsorted)
		if err != nil {
			return nil, err
		}
		sortCols[i] = sortCol
		sortColTypes[i] = colType
	}
	if s.useStableSort {
		sort.SliceStable(index, s.sortComparator(index, sortCols))
	} else {
		sort.Slice(index, s.sortComparator(index, sortCols))
	}
	return index, nil
}

func (s *SortOperator) sortComparator(index []int, sortCols []evbatch.Column) func(i, j int) bool {
	return func(i, j int) bool {
		for sortColIndex, sortExpr := range s.sortExprs {
			descending := s.sortDesc[sortColIndex]
			sortCol := sortCols[sortColIndex]
			i := index[i]
			j := index[j]
			null1 := sortCol.IsNull(i)
			null2 := sortCol.IsNull(j)
			if null1 && null2 {
				continue
			}
			if null1 && !null2 {
				return !descending
			}
			if null2 && !null1 {
				return descending
			}
			colType := sortExpr.ResultType()
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				intCol := sortCol.(*evbatch.IntColumn)
				val1 := intCol.Get(i)
				val2 := intCol.Get(j)
				if val1 < val2 {
					return !descending
				}
				if val2 < val1 {
					return descending
				}
			case types.ColumnTypeIDFloat:
				floatCol := sortCol.(*evbatch.FloatColumn)
				val1 := floatCol.Get(i)
				val2 := floatCol.Get(j)
				if val1 < val2 {
					return !descending
				}
				if val2 < val1 {
					return descending
				}
			case types.ColumnTypeIDBool:
				boolCol := sortCol.(*evbatch.BoolColumn)
				val1 := boolCol.Get(i)
				val2 := boolCol.Get(j)
				if !val1 && val2 {
					return !descending
				}
				if !val2 && val1 {
					return descending
				}
			case types.ColumnTypeIDDecimal:
				decCol := sortCol.(*evbatch.DecimalColumn)
				val1 := decCol.Get(i)
				val2 := decCol.Get(j)
				if val1.Num.Less(val2.Num) {
					return !descending
				}
				if val2.Num.Less(val1.Num) {
					return descending
				}
			case types.ColumnTypeIDString:
				stringCol := sortCol.(*evbatch.StringColumn)
				val1 := stringCol.Get(i)
				val2 := stringCol.Get(j)
				diff := strings.Compare(val1, val2)
				if diff < 0 {
					return !descending
				}
				if diff > 0 {
					return descending
				}
			case types.ColumnTypeIDBytes:
				bytesCol := sortCol.(*evbatch.BytesColumn)
				val1 := bytesCol.Get(i)
				val2 := bytesCol.Get(j)
				diff := bytes.Compare(val1, val2)
				if diff < 0 {
					return !descending
				}
				if diff > 0 {
					return descending
				}
			case types.ColumnTypeIDTimestamp:
				intCol := sortCol.(*evbatch.TimestampColumn)
				val1 := intCol.Get(i)
				val2 := intCol.Get(j)
				if val1.Val < val2.Val {
					return !descending
				}
				if val2.Val < val1.Val {
					return descending
				}
			default:
				panic("unknown type")
			}
		}
		return false
	}
}

func (s *SortOperator) HandleStreamBatch(*evbatch.Batch, StreamExecContext) (*evbatch.Batch, error) {
	panic("not supported in streams")
}

func (s *SortOperator) HandleBarrier(StreamExecContext) error {
	panic("not supported in streams")
}

func (s *SortOperator) InSchema() *OperatorSchema {
	return s.schema
}

func (s *SortOperator) OutSchema() *OperatorSchema {
	return s.schema
}

func (s *SortOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (s *SortOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}
