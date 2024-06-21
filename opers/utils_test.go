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
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"testing"
)

func testOperator(t *testing.T, fo Operator, inColumnNames []string, inColumnTypes []types.ColumnType, inData [][]any,
	expectedOutData [][]any) {

	batch := createEventBatch(inColumnNames, inColumnTypes, inData)
	defer batch.Release()

	out, err := fo.HandleStreamBatch(batch, nil)
	require.NoError(t, err)
	outData := convertBatchToAnyArray(out)
	require.Equal(t, expectedOutData, outData)
}

func convertBatchToAnyArray(batch *evbatch.Batch) [][]any {
	var data [][]any
	for i := 0; i < batch.RowCount; i++ {
		var row []any
		for j, colType := range batch.Schema.ColumnTypes() {
			if batch.Columns[j].IsNull(i) {
				row = append(row, nil)
				continue
			}
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				row = append(row, batch.GetIntColumn(j).Get(i))
			case types.ColumnTypeIDFloat:
				row = append(row, batch.GetFloatColumn(j).Get(i))
			case types.ColumnTypeIDBool:
				row = append(row, batch.GetBoolColumn(j).Get(i))
			case types.ColumnTypeIDDecimal:
				row = append(row, batch.GetDecimalColumn(j).Get(i))
			case types.ColumnTypeIDString:
				row = append(row, batch.GetStringColumn(j).Get(i))
			case types.ColumnTypeIDBytes:
				row = append(row, batch.GetBytesColumn(j).Get(i))
			case types.ColumnTypeIDTimestamp:
				row = append(row, batch.GetTimestampColumn(j).Get(i))
			default:
				panic("unknown type")
			}
		}
		data = append(data, row)
	}
	return data
}

func createEventBatch(columnNames []string, columnTypes []types.ColumnType, data [][]any) *evbatch.Batch {
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	colBuilders := evbatch.CreateColBuilders(columnTypes)
	for _, row := range data {
		for j, colType := range columnTypes {
			colBuilder := colBuilders[j]
			if row[j] == nil {
				colBuilder.AppendNull()
				continue
			}
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				colBuilder.(*evbatch.IntColBuilder).Append(row[j].(int64))
			case types.ColumnTypeIDFloat:
				colBuilder.(*evbatch.FloatColBuilder).Append(row[j].(float64))
			case types.ColumnTypeIDBool:
				colBuilder.(*evbatch.BoolColBuilder).Append(row[j].(bool))
			case types.ColumnTypeIDDecimal:
				colBuilder.(*evbatch.DecimalColBuilder).Append(row[j].(types.Decimal))
			case types.ColumnTypeIDString:
				colBuilder.(*evbatch.StringColBuilder).Append(row[j].(string))
			case types.ColumnTypeIDBytes:
				colBuilder.(*evbatch.BytesColBuilder).Append(row[j].([]byte))
			case types.ColumnTypeIDTimestamp:
				colBuilder.(*evbatch.TimestampColBuilder).Append(row[j].(types.Timestamp))
			default:
				panic("unknown type")
			}
		}
	}
	return evbatch.NewBatchFromBuilders(schema, colBuilders...)
}

func sortDataByKeyCols(data [][]any, keyColIndexes []int, keyColTypes []types.ColumnType) [][]any {
	sort.SliceStable(data, func(i, j int) bool {
		row1 := data[i]
		row2 := data[j]
		for k, keyCol := range keyColIndexes {
			kc1 := row1[keyCol]
			kc2 := row2[keyCol]
			if kc1 == nil && kc2 == nil {
				continue
			}
			if kc1 == nil {
				return true
			}
			if kc2 == nil {
				return false
			}
			kt := keyColTypes[k]
			switch kt.ID() {
			case types.ColumnTypeIDInt:
				if kc1.(int64) != kc2.(int64) {
					return kc1.(int64) < kc2.(int64)
				}
			case types.ColumnTypeIDFloat:
				if kc1.(float64) != kc2.(float64) {
					return kc1.(float64) < kc2.(float64)
				}
			case types.ColumnTypeIDBool:
				if kc1.(bool) != kc2.(bool) {
					return kc2.(bool)
				}
			case types.ColumnTypeIDDecimal:
				d1 := kc1.(types.Decimal)
				d2 := kc2.(types.Decimal)
				if d1.Num.Less(d2.Num) {
					return true
				}
				if d1.Num.Greater(d2.Num) {
					return false
				}
			case types.ColumnTypeIDString:
				s1 := kc1.(string)
				s2 := kc2.(string)
				d := strings.Compare(s1, s2)
				if d < 0 {
					return true
				}
				if d > 0 {
					return false
				}
			case types.ColumnTypeIDBytes:
				s1 := kc1.([]byte)
				s2 := kc2.([]byte)
				d := bytes.Compare(s1, s2)
				if d < 0 {
					return true
				}
				if d > 0 {
					return false
				}
			case types.ColumnTypeIDTimestamp:
				s1 := kc1.(types.Timestamp)
				s2 := kc2.(types.Timestamp)
				if s1.Val != s2.Val {
					return s1.Val < s2.Val
				}
			default:
				panic("unknown type")
			}
		}
		return false
	})
	return data
}
