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
	"fmt"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProjectOperatorReservedColNames(t *testing.T) {
	inSchema := evbatch.NewEventSchema([]string{"offset", "event_time", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt})
	exprs, err := toExprs("f1 as ws", "f2")
	require.NoError(t, err)
	_, err = NewProjectOperator(&OperatorSchema{EventSchema: inSchema}, exprs, true, &expr.ExpressionFactory{})
	require.Error(t, err)
	require.Equal(t, `cannot use alias 'ws', it is a reserved name (line 1 column 7):
f1 as ws
      ^`, err.Error())
}

func TestProjectOperatorSimple(t *testing.T) {
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt},
		[]string{"f0", "f1", "f2"},
		[][]any{{int64(0), types.Timestamp{Val: 0}, int64(1), int64(2), int64(3)}},
		[][]any{{int64(1), int64(2), int64(3)}},
	)
}

func TestProjectOperatorDirectColsOneRow(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[][]any{{int64(0), types.Timestamp{Val: 0}, int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 100), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		[][]any{{int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 100), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
	)
}

func TestProjectOperatorMultipleRowsPassThrough(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	numRows := 10
	var data [][]any
	var dataOut [][]any
	for i := 0; i < numRows; i++ {
		row := []any{
			int64(i), types.NewTimestamp(int64(i)), int64(i), 1.1 + float64(i), i%2 == 0, types.Decimal{Num: decimal128.New(0, uint64(i)), Precision: 12, Scale: 4}, fmt.Sprintf("foo%d", i),
			[]byte(fmt.Sprintf("bytes%d", i)), types.NewTimestamp(int64(i + 1000)),
		}
		data = append(data, row)
		dataOut = append(dataOut, row[2:])
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		data,
		dataOut,
	)
}

func TestProjectOperatorFunctionsOneRowOneOutCol(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{`sprintf("%d %f %t %v %s %v %v",f0,f1,f2,f3,f4,f5,f6)`},
		[][]any{{int64(0), types.NewTimestamp(10), int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 100), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		[][]any{{"1 1.230000 true 0.0100 foo [98 97 114] 1234"}},
	)
}

func TestProjectOperatorFunctionsMultipleRows(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     0,
	}
	numRows := 10
	var dataIn [][]any
	var dataOut [][]any
	for i := 0; i < numRows; i++ {
		rowIn := []any{
			int64(i), types.NewTimestamp(int64(i)), int64(i), 1.1 + float64(i), i%2 == 0,
			types.Decimal{Num: decimal128.New(0, uint64(i)), Precision: 12, Scale: 0}, fmt.Sprintf("foo%d", i),
			[]byte(fmt.Sprintf("bytes%d", i)), types.NewTimestamp(int64(i + 1000)),
		}
		dataIn = append(dataIn, rowIn)
		var rowOut []any
		if rowIn[4].(bool) {
			rowOut = append(rowOut, int64(100))
		} else {
			rowOut = append(rowOut, int64(200))
		}
		rowOut = append(rowOut, float64(float64(10*rowIn[2].(int64))+rowIn[3].(float64)))
		rowOut = append(rowOut, rowIn[2].(int64) < 5)
		lf4 := len(rowIn[6].(string))
		if rowIn[2].(int64) > 5 {
			dec := types.Decimal{
				Num:       decimal128.New(0, uint64(lf4)),
				Precision: 12,
				Scale:     0,
			}
			rowOut = append(rowOut, dec)
		} else {
			dec := types.Decimal{
				Num:       decimal128.New(0, uint64(lf4-2)),
				Precision: 12,
				Scale:     0,
			}
			rowOut = append(rowOut, dec)
		}
		ts := rowIn[8].(types.Timestamp)
		bytes := []byte(fmt.Sprintf("%d", ts.Val))
		rowOut = append(rowOut, bytes)

		dec := rowIn[5].(types.Decimal)
		rowOut = append(rowOut, types.NewTimestamp(int64(dec.Num.LowBits())))

		dataOut = append(dataOut, rowOut)
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{"if(f2,100,200)", "to_float(10 * f0) + f1", "f0 < 5", "if(f0 > 5,to_decimal(len(f4),12,0),to_decimal(len(f4)-2,12,0))",
			`to_bytes(sprintf("%d", f6))`, `to_timestamp(to_int(to_string(f3)))`},
		dataIn,
		dataOut,
	)
}

func TestProjectOperatorDecimalResultTypes(t *testing.T) {
	decType1 := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	decType2 := &types.DecimalType{
		Precision: 15,
		Scale:     3,
	}
	decType3 := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, decType1, decType2, decType3},
		[]string{"f0", "f1", "f2", "to_decimal(f0,13,1)", "to_decimal(f1,38,6)"},
		[][]any{{int64(0), types.NewTimestamp(0), types.Decimal{
			Num:       decimal128.New(0, 10000),
			Precision: 12,
			Scale:     4,
		}, types.Decimal{
			Num:       decimal128.New(0, 1000),
			Precision: 15,
			Scale:     3,
		}, types.Decimal{
			Num:       decimal128.New(0, 1000000),
			Precision: types.DefaultDecimalPrecision,
			Scale:     types.DefaultDecimalScale,
		}}},
		[][]any{{
			types.Decimal{
				Num:       decimal128.New(0, 10000),
				Precision: 12,
				Scale:     4,
			}, types.Decimal{
				Num:       decimal128.New(0, 1000),
				Precision: 15,
				Scale:     3,
			}, types.Decimal{
				Num:       decimal128.New(0, 1000000),
				Precision: types.DefaultDecimalPrecision,
				Scale:     types.DefaultDecimalScale,
			}, types.Decimal{
				Num:       decimal128.New(0, 10),
				Precision: 13,
				Scale:     1,
			}, types.Decimal{
				Num:       decimal128.New(0, 1000000),
				Precision: 38,
				Scale:     6,
			},
		}},
	)
}

func TestProjectOperatorMultipleRowsChangeColOrder(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	numRows := 10
	var dataIn [][]any
	var dataOut [][]any
	for i := 0; i < numRows; i++ {
		rowIn := []any{
			int64(i), types.NewTimestamp(int64(i)),
			int64(i), 1.1 + float64(i), i%2 == 0, types.Decimal{Num: decimal128.New(0, uint64(i)), Precision: 12, Scale: 4}, fmt.Sprintf("foo%d", i),
			[]byte(fmt.Sprintf("bytes%d", i)), types.NewTimestamp(int64(i + 1000)),
		}
		dataIn = append(dataIn, rowIn)

		rowOut := []any{
			rowIn[5], rowIn[8], rowIn[7], rowIn[4], rowIn[2], rowIn[3], rowIn[6],
		}
		dataOut = append(dataOut, rowOut)
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{"f3", "f6", "f5", "f2", "f0", "f1", "f4"},
		dataIn,
		dataOut,
	)
}

func TestProjectOperatorNoInColsUsed(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{`to_decimal("1000.1234",38,4)`, "23", `to_bytes("quux")`, "1.23f", "false", `"wibble"`, "to_timestamp(1000)"},
		[][]any{{int64(0), types.NewTimestamp(1), int64(1), float64(1.77), true, types.Decimal{Num: decimal128.New(0, 100), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		[][]any{{types.Decimal{Num: decimal128.New(0, 10001234), Precision: 38, Scale: 4}, int64(23), []byte("quux"),
			float64(1.23), false, "wibble", types.NewTimestamp(1000)}},
	)
}

func TestProjectOperatorNotAllColsUsed(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testProjectOper(t, []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		[]string{`f5`, "23", `to_bytes("quux")`, "1.23f", "false", "f4", "to_timestamp(1000)"},
		[][]any{{int64(0), types.NewTimestamp(1), int64(1), float64(1.77), true, types.Decimal{Num: decimal128.New(0, 100), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		[][]any{{[]byte("bar"), int64(23), []byte("quux"),
			float64(1.23), false, "foo", types.NewTimestamp(1000)}},
	)
}

func testProjectOper(t *testing.T, inColumnNames []string, inColumnTypes []types.ColumnType,
	colExprs []string, inData [][]any,
	expectedOutData [][]any) {
	testProjectOperIncludeSystemCols(t, inColumnNames, inColumnTypes, colExprs, inData, expectedOutData, false)
}

func testProjectOperIncludeSystemCols(t *testing.T, inColumnNames []string, inColumnTypes []types.ColumnType,
	colExprs []string, inData [][]any,
	expectedOutData [][]any, includeSystemCols bool) {
	inSchema := evbatch.NewEventSchema(inColumnNames, inColumnTypes)
	exprs, err := toExprs(colExprs...)
	require.NoError(t, err)
	fo, err := NewProjectOperator(&OperatorSchema{EventSchema: inSchema}, exprs, includeSystemCols, &expr.ExpressionFactory{})
	require.NoError(t, err)
	testOperator(t, fo, inColumnNames, inColumnTypes, inData, expectedOutData)
}

func TestOutSchemaDirectCols(t *testing.T) {
	testOutSchemaType(t, "f0", types.ColumnTypeInt, types.ColumnTypeInt)
	testOutSchemaType(t, "f0", types.ColumnTypeFloat, types.ColumnTypeFloat)
	testOutSchemaType(t, "f0", types.ColumnTypeBool, types.ColumnTypeBool)
	testOutSchemaType(t, "f0", types.ColumnTypeString, types.ColumnTypeString)
	testOutSchemaType(t, "f0", types.ColumnTypeTimestamp, types.ColumnTypeTimestamp)
	testOutSchemaType(t, "f0", types.ColumnTypeBytes, types.ColumnTypeBytes)
	decType := &types.DecimalType{
		Precision: 10,
		Scale:     2,
	}
	testOutSchemaType(t, "f0", decType, decType)
}

func TestOutSchemaExplicitConversions(t *testing.T) {
	testOutSchemaType(t, "to_int(f0)", types.ColumnTypeInt, types.ColumnTypeInt)
	testOutSchemaType(t, "to_int(f0)", types.ColumnTypeString, types.ColumnTypeInt)
	testOutSchemaType(t, "to_int(f0)", types.ColumnTypeFloat, types.ColumnTypeInt)
	testOutSchemaType(t, "to_int(f0)", types.ColumnTypeTimestamp, types.ColumnTypeInt)

	testOutSchemaType(t, "to_float(f0)", types.ColumnTypeInt, types.ColumnTypeFloat)
	testOutSchemaType(t, "to_float(f0)", types.ColumnTypeString, types.ColumnTypeFloat)
	testOutSchemaType(t, "to_float(f0)", types.ColumnTypeFloat, types.ColumnTypeFloat)

	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeInt, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeString, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeFloat, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeBool, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeBytes, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", &types.DecimalType{
		Precision: 10,
		Scale:     2,
	}, types.ColumnTypeString)
	testOutSchemaType(t, "to_string(f0)", types.ColumnTypeTimestamp, types.ColumnTypeString)

	testOutSchemaType(t, "to_bytes(f0)", types.ColumnTypeString, types.ColumnTypeBytes)
	testOutSchemaType(t, "to_bytes(f0)", types.ColumnTypeBytes, types.ColumnTypeBytes)

	testOutSchemaType(t, "to_timestamp(f0)", types.ColumnTypeTimestamp, types.ColumnTypeTimestamp)

	testOutSchemaType(t, "to_decimal(f0, 14, 4)", types.ColumnTypeString, &types.DecimalType{
		Precision: 14,
		Scale:     4,
	})
	testOutSchemaType(t, "to_decimal(f0, 32, 6)", types.ColumnTypeString, &types.DecimalType{
		Precision: 32,
		Scale:     6,
	})
	testOutSchemaType(t, "to_decimal(f0, 14, 4)+to_decimal(f0, 14, 4)", types.ColumnTypeString, &types.DecimalType{
		Precision: 14,
		Scale:     4,
	})
}

func TestOutSchemaColumnNames(t *testing.T) {
	testOutSchemaColName(t, []string{"f0", "f1", "f2"}, []string{"f0", "f1", "f2"}, []string{"f0", "f1", "f2"})

	testOutSchemaColName(t, []string{"f0 as g0", "f1 as g1", "f2 as g2"}, []string{"f0", "f1", "f2"}, []string{"g0", "g1", "g2"})

	testOutSchemaColName(t, []string{"to_string(f0)", "to_string(f0)", "to_string(f0)"}, []string{"f0", "f1", "f2"},
		[]string{"col0", "col1", "col2"})

	testOutSchemaColName(t, []string{"to_string(f0) as foo", "to_string(f0) as bar", "to_string(f0) as quux"},
		[]string{"f0", "f1", "f2"}, []string{"foo", "bar", "quux"})
}

func TestOutSchemaOperators(t *testing.T) {
	testOutSchemaType(t, "1 + 1", types.ColumnTypeInt, types.ColumnTypeInt)
}

func testOutSchemaType(t *testing.T, e string, inType types.ColumnType, outType types.ColumnType) {
	inSchema := evbatch.NewEventSchema([]string{"offset", "event_time", "f0"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, inType})
	exprStrs := []string{e}
	exprs, err := toExprs(exprStrs...)
	require.NoError(t, err)
	fo, err := NewProjectOperator(&OperatorSchema{EventSchema: inSchema}, exprs, false, &expr.ExpressionFactory{})
	require.NoError(t, err)
	outSchema := fo.OutSchema()
	require.Equal(t, 1, len(fo.OutSchema().EventSchema.ColumnNames()))
	ftOut := outSchema.EventSchema.ColumnTypes()[0]
	require.Equal(t, outType, ftOut)
}

func testOutSchemaColName(t *testing.T, exprStrs []string, inNames []string, outNames []string) {
	fNames := []string{"offset", "event_time"}
	fNames = append(fNames, inNames...)

	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp}
	for i := 0; i < len(inNames); i++ {
		fTypes = append(fTypes, types.ColumnTypeInt)
	}
	inSchema := evbatch.NewEventSchema(fNames, fTypes)
	exprs, err := toExprs(exprStrs...)
	require.NoError(t, err)
	fo, err := NewProjectOperator(&OperatorSchema{EventSchema: inSchema}, exprs, false, &expr.ExpressionFactory{})
	require.NoError(t, err)
	outSchema := fo.OutSchema()
	fnsOut := outSchema.EventSchema.ColumnNames()
	require.Equal(t, outNames, fnsOut)
}

func TestIncludeSystemCols(t *testing.T) {
	testProjectOperIncludeSystemCols(t, []string{"offset", "event_time", "f0", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt},
		[]string{"f0", "f1", "f2"},
		[][]any{{int64(3), types.Timestamp{Val: 100}, int64(1), int64(2), int64(3)}},
		[][]any{{int64(3), types.Timestamp{Val: 100}, int64(1), int64(2), int64(3)}},
		true,
	)
}

func TestSelectSystemColsWhenNotIncluded(t *testing.T) {
	testProjectOperIncludeSystemCols(t, []string{"offset", "event_time", "f0", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt},
		[]string{"f0", "f1", "f2", "offset", "event_time"},
		[][]any{{int64(3), types.Timestamp{Val: 100}, int64(1), int64(2), int64(3)}},
		[][]any{{int64(1), int64(2), int64(3), int64(3), types.Timestamp{Val: 100}}},
		false,
	)
}
