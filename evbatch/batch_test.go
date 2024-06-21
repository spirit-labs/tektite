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

package evbatch

import (
	"fmt"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBatchTypes(t *testing.T) {
	decType := &types.DecimalType{
		Scale:     5,
		Precision: 11,
	}
	schema := NewEventSchema([]string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp})
	batch := createEventBatch(t, schema)
	defer batch.Release()

	require.Equal(t, 20, batch.RowCount)

	rowIndex := 0
	for i := 0; i < 10; i++ {
		intVal := batch.GetIntColumn(0).Get(rowIndex)
		require.Equal(t, int64(i), intVal)
		floatVal := batch.GetFloatColumn(1).Get(rowIndex)
		require.Equal(t, float64(1.23+float64(i)), floatVal)
		boolVal := batch.GetBoolColumn(2).Get(rowIndex)
		require.Equal(t, i%2 == 0, boolVal)
		decVal := batch.GetDecimalColumn(3).Get(rowIndex)
		decNum, err := decimal128.FromString(fmt.Sprintf("%d1234554321", i), 11, 0)
		require.NoError(t, err)
		decExpected := types.Decimal{
			Num:       decNum,
			Precision: 11,
			Scale:     5,
		}
		require.Equal(t, decExpected, decVal)
		strVal := batch.GetStringColumn(4).Get(rowIndex)
		require.Equal(t, fmt.Sprintf("ldg %d", i), strVal)
		bytesVal := batch.GetBytesColumn(5).Get(rowIndex)
		require.Equal(t, []byte(fmt.Sprintf("somebytes%d", i)), bytesVal)
		tsVal := batch.GetTimestampColumn(6).Get(rowIndex)
		require.Equal(t, types.NewTimestamp(int64(i*1000)), tsVal)
		rowIndex++
		require.True(t, batch.Columns[0].IsNull(rowIndex))
		require.True(t, batch.Columns[1].IsNull(rowIndex))
		require.True(t, batch.Columns[2].IsNull(rowIndex))
		require.True(t, batch.Columns[3].IsNull(rowIndex))
		require.True(t, batch.Columns[4].IsNull(rowIndex))
		require.True(t, batch.Columns[5].IsNull(rowIndex))
		require.True(t, batch.Columns[6].IsNull(rowIndex))
		rowIndex++
	}
}

func createEventBatch(t *testing.T, schema *EventSchema) *Batch {
	builders := CreateColBuilders(schema.ColumnTypes())
	for i := 0; i < 10; i++ {
		builders[0].(*IntColBuilder).Append(int64(i))
		builders[1].(*FloatColBuilder).Append(float64(1.23 + float64(i)))
		builders[2].(*BoolColBuilder).Append(i%2 == 0)
		decNum, err := decimal128.FromString(fmt.Sprintf("%d12345.54321", i), 20, 5)
		require.NoError(t, err)
		dec := types.Decimal{
			Num:       decNum,
			Precision: 20,
			Scale:     5,
		}
		builders[3].(*DecimalColBuilder).Append(dec)
		builders[4].(*StringColBuilder).Append(fmt.Sprintf("ldg %d", i))
		builders[5].(*BytesColBuilder).Append([]byte(fmt.Sprintf("somebytes%d", i)))
		builders[6].(*TimestampColBuilder).Append(types.NewTimestamp(int64(i * 1000)))
		builders[0].AppendNull()
		builders[1].AppendNull()
		builders[2].AppendNull()
		builders[3].AppendNull()
		builders[4].AppendNull()
		builders[5].AppendNull()
		builders[6].AppendNull()
	}
	return NewBatchFromBuilders(schema, builders...)
}

func TestBatchBytes(t *testing.T) {
	decType := &types.DecimalType{
		Scale:     5,
		Precision: 20,
	}
	schema := NewEventSchema([]string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp})
	batch := createEventBatch(t, schema)
	defer batch.Release()

	bytes := batch.ToBytes()

	batch2 := NewBatchFromBytes(schema, batch.RowCount, bytes)

	require.True(t, batch.Equal(batch2))
}
