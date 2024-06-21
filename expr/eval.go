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

package expr

import (
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/types"
)

func EvalColumn(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	switch expr.ResultType().ID() {
	case types.ColumnTypeIDInt:
		return evalIntOnBatch(expr, batch)
	case types.ColumnTypeIDFloat:
		return evalFloatOnBatch(expr, batch)
	case types.ColumnTypeIDBool:
		return evalBoolOnBatch(expr, batch)
	case types.ColumnTypeIDDecimal:
		return evalDecimalOnBatch(expr, batch)
	case types.ColumnTypeIDString:
		return evalStringOnBatch(expr, batch)
	case types.ColumnTypeIDBytes:
		return evalBytesOnBatch(expr, batch)
	case types.ColumnTypeIDTimestamp:
		return evalTimestampOnBatch(expr, batch)
	default:
		panic("unexpected column type")
	}
}

func evalIntOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewIntColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalInt(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildIntColumn(), nil
}

func evalFloatOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewFloatColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalFloat(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildFloatColumn(), nil
}

func evalBoolOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewBoolColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalBool(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildBoolColumn(), nil
}

func evalDecimalOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewDecimalColBuilder(expr.ResultType().(*types.DecimalType))
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalDecimal(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildDecimalColumn(), nil
}

func evalStringOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewStringColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalString(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildStringColumn(), nil
}

func evalBytesOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewBytesColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalBytes(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildBytesColumn(), nil
}

func evalTimestampOnBatch(expr Expression, batch *evbatch.Batch) (evbatch.Column, error) {
	builder := evbatch.NewTimestampColBuilder()
	rc := batch.RowCount
	for i := 0; i < rc; i++ {
		val, null, err := expr.EvalTimestamp(i, batch)
		if err != nil {
			return nil, err
		}
		if null {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildTimestampColumn(), nil
}
