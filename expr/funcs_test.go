package expr

import (
	"encoding/binary"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestIfFunctionInt(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false}, []bool{false, false, true, false, true, true, false, true})

	colTrue := createIntCol([]bool{false, true, false, false, false, false, false, false}, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	colFalse := createIntCol([]bool{false, true, false, false, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8, 9})

	expected := createIntCol([]bool{false, true, true, false, true, false, false, false}, []int64{2, 0, 0, 5, 0, 6, 8, 8})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionFloat(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false}, []bool{false, false, true, false, true, true, false, true})

	colTrue := createFloatCol([]bool{false, true, false, false, false, false, false, false}, []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1})
	colFalse := createFloatCol([]bool{false, true, false, false, false, true, false, false}, []float64{2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1})

	expected := createFloatCol([]bool{false, true, true, false, true, false, false, false}, []float64{2.1, 0, 0, 5.1, 0, 6.1, 8.1, 8.1})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionBool(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false}, []bool{false, false, true, false, true, true, false, true})

	colTrue := createBoolCol([]bool{false, true, false, false, false, false, false, false}, []bool{true, true, false, true, false, true, true, true})
	colFalse := createBoolCol([]bool{false, true, false, false, false, true, false, false}, []bool{false, true, true, false, false, true, false, false})

	expected := createBoolCol([]bool{false, true, true, false, true, false, false, false}, []bool{false, false, false, false, false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBool},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionDecimal(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false}, []bool{false, false, true, false, true, true, false, true})

	colTrue := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{false, true, false, false, false, false, false, false},
		[]string{"1.1", "2.1", "3.1", "4.1", "5.1", "6.1", "7.1", "8.1"})
	colFalse := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{false, true, false, false, false, true, false, false},
		[]string{"2.1", "3.1", "4.1", "5.1", "6.1", "7.1", "8.1", "9.1"})

	expected := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{false, true, true, false, true, false, false, false},
		[]string{"2.1", "0", "0", "5.1", "0", "6.1", "8.1", "8.1"})

	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: decType},
		&ColumnExpr{colIndex: 2, exprType: decType},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, decType, decType})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionString(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false},
		[]bool{false, false, true, false, true, true, false, true})

	colTrue := createStringCol([]bool{false, true, false, false, false, false, false, false},
		[]string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"})
	colFalse := createStringCol([]bool{false, true, false, false, false, true, false, false},
		[]string{"b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8"})

	expected := createStringCol([]bool{false, true, true, false, true, false, false, false},
		[]string{"b1", "0", "0", "b4", "0", "a6", "b7", "a8"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionBytes(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false},
		[]bool{false, false, true, false, true, true, false, true})

	colTrue := createBytesCol([]bool{false, true, false, false, false, false, false, false},
		[]string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"})
	colFalse := createBytesCol([]bool{false, true, false, false, false, true, false, false},
		[]string{"b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8"})

	expected := createBytesCol([]bool{false, true, true, false, true, false, false, false},
		[]string{"b1", "0", "0", "b4", "0", "a6", "b7", "a8"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionTimestamp(t *testing.T) {
	colTest := createBoolCol([]bool{false, false, true, false, true, false, false, false}, []bool{false, false, true, false, true, true, false, true})

	colTrue := createTimestampCol([]bool{false, true, false, false, false, false, false, false}, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	colFalse := createTimestampCol([]bool{false, true, false, false, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8, 9})

	expected := createTimestampCol([]bool{false, true, true, false, true, false, false, false}, []int64{2, 0, 0, 5, 0, 6, 8, 8})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeTimestamp},
	}

	fun, err := NewIfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, colTest, colTrue, colFalse)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIfFunctionArgs(t *testing.T) {
	_, err := NewIfFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'if' function requires 3 arguments"))

	_, err = NewIfFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'if' function requires 3 arguments"))

	_, err = NewIfFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'if' function first argument must be of type bool"))

	_, err = NewIfFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'if' function second and third arguments must be of same type"))
}

func TestIsNullFunctionInt(t *testing.T) {

	col1 := createIntCol([]bool{false, true, false, false, true}, []int64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, true, false, false, true}, []float64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, true, false, false, true}, []bool{true, true, true, true, true})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionDecimal(t *testing.T) {

	col1 := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{false, true, false, false, true},
		[]string{"123.123", "123.123", "123.123", "123.123", "123.123"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{decType})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionString(t *testing.T) {

	col1 := createStringCol([]bool{false, true, false, false, true}, []string{"a", "a", "a", "a", "a"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionBytes(t *testing.T) {

	col1 := createBytesCol([]bool{false, true, false, false, true}, []string{"a", "a", "a", "a", "a"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, true, false, false, true}, []int64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{false, true, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNullFunctionArgs(t *testing.T) {
	_, err := NewIsNullFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'is_null' function requires 1 argument"))
}

func TestIsNotNullFunctionInt(t *testing.T) {

	col1 := createIntCol([]bool{false, true, false, false, true}, []int64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, true, false, false, true}, []float64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, true, false, false, true}, []bool{true, true, true, true, true})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionDecimal(t *testing.T) {

	col1 := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{false, true, false, false, true},
		[]string{"123.123", "123.123", "123.123", "123.123", "123.123"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{decType})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionString(t *testing.T) {

	col1 := createStringCol([]bool{false, true, false, false, true}, []string{"a", "a", "a", "a", "a"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionBytes(t *testing.T) {

	col1 := createBytesCol([]bool{false, true, false, false, true}, []string{"a", "a", "a", "a", "a"})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, true, false, false, true}, []int64{1, 2, 3, 4, 5})
	expected := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewIsNotNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestIsNotNullFunctionArgs(t *testing.T) {
	_, err := NewIsNotNullFunction([]Expression{
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'is_not_null' function requires 1 argument"))
}

func TestInFunctionInt(t *testing.T) {

	colTest := createIntCol([]bool{true, false, false, false}, []int64{1, 2, 3, 4})

	col1 := createIntCol([]bool{false, false, false, false}, []int64{6, 7, 4, 1})
	col2 := createIntCol([]bool{true, false, false, false}, []int64{1, 2, 6, 3})
	col3 := createIntCol([]bool{false, false, false, false}, []int64{5, 8, 9, 4})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionFloat(t *testing.T) {

	colTest := createFloatCol([]bool{true, false, false, false}, []float64{1.1, 2.1, 3.1, 4.1})

	col1 := createFloatCol([]bool{false, false, false, false}, []float64{6.1, 7.1, 4.1, 1.1})
	col2 := createFloatCol([]bool{true, false, false, false}, []float64{1.1, 2.1, 6.1, 3.1})
	col3 := createFloatCol([]bool{false, false, false, false}, []float64{5.1, 8.1, 9.1, 4.1})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeFloat}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionBool(t *testing.T) {

	colTest := createBoolCol([]bool{true, false, false, false, false}, []bool{true, true, false, true, false})

	col1 := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, false, true})
	col2 := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, true, true})
	col3 := createBoolCol([]bool{false, false, false, false, false}, []bool{true, false, true, false, false})

	expected := createBoolCol([]bool{true, false, false, false, false}, []bool{false, false, false, true, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBool}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionDecimal(t *testing.T) {

	prec := types.DefaultDecimalPrecision
	scale := types.DefaultDecimalScale
	colTest := createDecimalCol(prec, scale, []bool{true, false, false, false}, []string{"1.1", "2.1", "3.1", "4.1"})

	col1 := createDecimalCol(prec, scale, []bool{false, false, false, false}, []string{"6.1", "7.1", "4.1", "1.1"})
	col2 := createDecimalCol(prec, scale, []bool{true, false, false, false}, []string{"1.1", "2.1", "6.1", "3.1"})
	col3 := createDecimalCol(prec, scale, []bool{false, false, false, false}, []string{"5.1", "8.1", "9.1", "4.1"})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	decType := &types.DecimalType{Precision: prec, Scale: scale}

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType},
		&ColumnExpr{colIndex: 1, exprType: decType},
		&ColumnExpr{colIndex: 2, exprType: decType},
		&ColumnExpr{colIndex: 3, exprType: decType}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{decType, decType, decType, decType})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionString(t *testing.T) {

	colTest := createStringCol([]bool{true, false, false, false}, []string{"1.1", "2.1", "3.1", "4.1"})

	col1 := createStringCol([]bool{false, false, false, false}, []string{"6.1", "7.1", "4.1", "1.1"})
	col2 := createStringCol([]bool{true, false, false, false}, []string{"1.1", "2.1", "6.1", "3.1"})
	col3 := createStringCol([]bool{false, false, false, false}, []string{"5.1", "8.1", "9.1", "4.1"})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionBytes(t *testing.T) {

	colTest := createBytesCol([]bool{true, false, false, false}, []string{"1.1", "2.1", "3.1", "4.1"})

	col1 := createBytesCol([]bool{false, false, false, false}, []string{"6.1", "7.1", "4.1", "1.1"})
	col2 := createBytesCol([]bool{true, false, false, false}, []string{"1.1", "2.1", "6.1", "3.1"})
	col3 := createBytesCol([]bool{false, false, false, false}, []string{"5.1", "8.1", "9.1", "4.1"})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBytes}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionTimestamp(t *testing.T) {

	colTest := createTimestampCol([]bool{true, false, false, false}, []int64{1, 2, 3, 4})

	col1 := createTimestampCol([]bool{false, false, false, false}, []int64{6, 7, 4, 1})
	col2 := createTimestampCol([]bool{true, false, false, false}, []int64{1, 2, 6, 3})
	col3 := createTimestampCol([]bool{false, false, false, false}, []int64{5, 8, 9, 4})

	expected := createBoolCol([]bool{true, false, false, false}, []bool{false, true, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewInFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col0", "col1", "col2", "col3"},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, colTest, col1, col2, col3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestInFunctionArgs(t *testing.T) {

	_, err := NewInFunction([]Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'in' function requires at least 3 arguments"))

	_, err = NewInFunction([]Expression{
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
	}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'in' function arguments must have same type - first arg has type int - arg at position 2 has type bool"))
}

func TestCaseFunctionInt(t *testing.T) {
	colDefault := createIntCol([]bool{false, false, false, false, true, false}, []int64{21, 23, 26, 24, 27, 29})

	colTest := createIntCol([]bool{true, false, false, false, false, false}, []int64{1, 3, 6, 4, 7, 9})

	case1 := createIntCol([]bool{false, false, false, false, false, false}, []int64{7, 5, 10, 5, 10, 8})
	case2 := createIntCol([]bool{false, false, false, false, false, false}, []int64{1, 3, 2, 6, 2, 19})
	case3 := createIntCol([]bool{false, false, false, false, false, false}, []int64{9, 8, 6, 10, 5, 9})

	ret1 := createIntCol([]bool{false, false, false, false, false, false}, []int64{10, 27, 6, 4, 7, 10})
	ret2 := createIntCol([]bool{false, false, false, false, false, false}, []int64{11, 13, 6, 4, 7, 4})
	ret3 := createIntCol([]bool{false, false, false, false, false, true}, []int64{12, 11, 23, 4, 7, 9})

	expected := createIntCol([]bool{true, false, false, false, true, true},
		[]int64{0, 13, 23, 24, 0, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeInt},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
			types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionDifferentCaseAndReturnTypes(t *testing.T) {
	colDefault := createFloatCol([]bool{false, false, false, false, true, false}, []float64{21, 23, 26, 24, 27, 29})

	colTest := createIntCol([]bool{true, false, false, false, false, false}, []int64{1, 3, 6, 4, 7, 9})

	case1 := createIntCol([]bool{false, false, false, false, false, false}, []int64{7, 5, 10, 5, 10, 8})
	case2 := createIntCol([]bool{false, false, false, false, false, false}, []int64{1, 3, 2, 6, 2, 19})
	case3 := createIntCol([]bool{false, false, false, false, false, false}, []int64{9, 8, 6, 10, 5, 9})

	ret1 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{10, 27, 6, 4, 7, 10})
	ret2 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{11, 13, 6, 4, 7, 4})
	ret3 := createFloatCol([]bool{false, false, false, false, false, true}, []float64{12, 11, 23, 4, 7, 9})

	expected := createFloatCol([]bool{true, false, false, false, true, true},
		[]float64{0, 13, 23, 24, 0, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeFloat},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeInt,
			types.ColumnTypeFloat, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionFloat(t *testing.T) {
	colDefault := createFloatCol([]bool{false, false, false, false, true, false}, []float64{21, 23, 26, 24, 27, 29})

	colTest := createFloatCol([]bool{true, false, false, false, false, false}, []float64{1, 3, 6, 4, 7, 9})

	case1 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{7, 5, 10, 5, 10, 8})
	case2 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{1, 3, 2, 6, 2, 19})
	case3 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{9, 8, 6, 10, 5, 9})

	ret1 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{10, 27, 6, 4, 7, 10})
	ret2 := createFloatCol([]bool{false, false, false, false, false, false}, []float64{11, 13, 6, 4, 7, 4})
	ret3 := createFloatCol([]bool{false, false, false, false, false, true}, []float64{12, 11, 23, 4, 7, 9})

	expected := createFloatCol([]bool{true, false, false, false, true, true},
		[]float64{0, 13, 23, 24, 0, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeFloat},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat,
			types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionBool(t *testing.T) {
	colDefault := createBoolCol([]bool{false, false, false, false, true, false}, []bool{false, true, false, true, true, false})

	colTest := createBoolCol([]bool{true, false, false, false, false, false}, []bool{true, false, true, false, true, false})

	case1 := createBoolCol([]bool{false, false, false, false, false, false}, []bool{false, true, false, true, false, true})
	case2 := createBoolCol([]bool{false, false, false, false, false, false}, []bool{true, false, false, true, false, true})
	case3 := createBoolCol([]bool{false, false, false, false, false, false}, []bool{false, true, true, true, false, false})

	ret1 := createBoolCol([]bool{false, false, false, false, false, false}, []bool{false, false, true, false, true, false})
	ret2 := createBoolCol([]bool{false, false, false, false, false, false}, []bool{true, true, true, false, true, false})
	ret3 := createBoolCol([]bool{false, false, false, false, false, true}, []bool{false, false, false, false, true, true})

	expected := createBoolCol([]bool{true, false, false, false, true, true},
		[]bool{false, true, false, true, false, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeBool},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool,
			types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionDecimal(t *testing.T) {
	prec := types.DefaultDecimalPrecision
	scale := types.DefaultDecimalScale
	colDefault := createDecimalCol(prec, scale, []bool{false, false, false, false, true, false}, []string{"21", "23", "26", "24", "27", "29"})

	colTest := createDecimalCol(prec, scale, []bool{true, false, false, false, false, false}, []string{"1", "3", "6", "4", "7", "9"})

	case1 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, false}, []string{"7", "5", "10", "52", "10", "8"})
	case2 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, false}, []string{"1", "3", "2", "62", "2", "19"})
	case3 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, false}, []string{"9", "8", "6", "10", "5", "9"})

	ret1 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, false}, []string{"10", "27", "6", "4", "7", "10"})
	ret2 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, false}, []string{"11", "13", "6", "4", "7", "4"})
	ret3 := createDecimalCol(prec, scale, []bool{false, false, false, false, false, true}, []string{"12", "11", "23", "4", "7", "9"})

	expected := createDecimalCol(prec, scale, []bool{true, false, false, false, true, true},
		[]string{"0", "13", "23", "24", "0", "0"})

	decType := &types.DecimalType{Precision: types.DefaultDecimalPrecision, Scale: types.DefaultDecimalScale}

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType},
		&ColumnExpr{colIndex: 1, exprType: decType},
		&ColumnExpr{colIndex: 2, exprType: decType},
		&ColumnExpr{colIndex: 3, exprType: decType},
		&ColumnExpr{colIndex: 4, exprType: decType},
		&ColumnExpr{colIndex: 5, exprType: decType},
		&ColumnExpr{colIndex: 6, exprType: decType},
		&ColumnExpr{colIndex: 7, exprType: decType},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{decType, decType, decType, decType,
			decType, decType, decType, decType})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionString(t *testing.T) {
	colDefault := createStringCol([]bool{false, false, false, false, true, false}, []string{"21", "23", "26", "24", "27", "29"})

	colTest := createStringCol([]bool{true, false, false, false, false, false}, []string{"1", "3", "6", "4", "7", "9"})

	case1 := createStringCol([]bool{false, false, false, false, false, false}, []string{"7", "5", "10", "52", "10", "8"})
	case2 := createStringCol([]bool{false, false, false, false, false, false}, []string{"1", "3", "2", "62", "2", "19"})
	case3 := createStringCol([]bool{false, false, false, false, false, false}, []string{"9", "8", "6", "10", "5", "9"})

	ret1 := createStringCol([]bool{false, false, false, false, false, false}, []string{"10", "27", "6", "4", "7", "10"})
	ret2 := createStringCol([]bool{false, false, false, false, false, false}, []string{"11", "13", "6", "4", "7", "4"})
	ret3 := createStringCol([]bool{false, false, false, false, false, true}, []string{"12", "11", "23", "4", "7", "9"})

	expected := createStringCol([]bool{true, false, false, false, true, true},
		[]string{"0", "13", "23", "24", "0", "0"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeString},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString,
			types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionBytes(t *testing.T) {
	colDefault := createBytesCol([]bool{false, false, false, false, true, false}, []string{"21", "23", "26", "24", "27", "29"})

	colTest := createBytesCol([]bool{true, false, false, false, false, false}, []string{"1", "3", "6", "4", "7", "9"})

	case1 := createBytesCol([]bool{false, false, false, false, false, false}, []string{"7", "5", "10", "52", "10", "8"})
	case2 := createBytesCol([]bool{false, false, false, false, false, false}, []string{"1", "3", "2", "62", "2", "19"})
	case3 := createBytesCol([]bool{false, false, false, false, false, false}, []string{"9", "8", "6", "10", "5", "9"})

	ret1 := createBytesCol([]bool{false, false, false, false, false, false}, []string{"10", "27", "6", "4", "7", "10"})
	ret2 := createBytesCol([]bool{false, false, false, false, false, false}, []string{"11", "13", "6", "4", "7", "4"})
	ret3 := createBytesCol([]bool{false, false, false, false, false, true}, []string{"12", "11", "23", "4", "7", "9"})

	expected := createBytesCol([]bool{true, false, false, false, true, true},
		[]string{"0", "13", "23", "24", "0", "0"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeBytes},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes,
			types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionTimestamp(t *testing.T) {
	colDefault := createTimestampCol([]bool{false, false, false, false, true, false}, []int64{21, 23, 26, 24, 27, 29})

	colTest := createTimestampCol([]bool{true, false, false, false, false, false}, []int64{1, 3, 6, 4, 7, 9})

	case1 := createTimestampCol([]bool{false, false, false, false, false, false}, []int64{7, 5, 10, 5, 10, 8})
	case2 := createTimestampCol([]bool{false, false, false, false, false, false}, []int64{1, 3, 2, 6, 2, 19})
	case3 := createTimestampCol([]bool{false, false, false, false, false, false}, []int64{9, 8, 6, 10, 5, 9})

	ret1 := createTimestampCol([]bool{false, false, false, false, false, false}, []int64{10, 27, 6, 4, 7, 10})
	ret2 := createTimestampCol([]bool{false, false, false, false, false, false}, []int64{11, 13, 6, 4, 7, 4})
	ret3 := createTimestampCol([]bool{false, false, false, false, false, true}, []int64{12, 11, 23, 4, 7, 9})

	expected := createTimestampCol([]bool{true, false, false, false, true, true},
		[]int64{0, 13, 23, 24, 0, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeTimestamp},
	}
	fun, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp,
			types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, colTest, case1, ret1, case2, ret2, case3, ret3, colDefault)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestCaseFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
	}
	_, err := NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'case' function requires at least 4 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeInt},
	}
	_, err = NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'case' function requires an even number of arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeString},
	}
	_, err = NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'case' function cases must have same type as test expression - test expression has type int - arg at position 1 has type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeString},
	}
	_, err = NewCaseFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'case' function return expressions must have same type as default expression - default expression has type string - arg at position 0 has type float"))
}

func TestDecimalShiftFunction(t *testing.T) {
	vals := []string{"1234", "1000.0000", "1.0001", "0.0023", "1234.5678"}
	expectedVals := []string{"0", "100000", "100.01", "0.23", "123456.78"}
	round := true
	testDecimalShiftFunction(t, 2, &round, vals, expectedVals)

	vals = []string{"1234", "1000.0000", "1.0001", "0.0023", "1234.5678"}
	expectedVals = []string{"0", "10", "0.01", "0", "12.3457"}
	round = true
	testDecimalShiftFunction(t, -2, &round, vals, expectedVals)

	vals = []string{"1234", "1000.0000", "1.0001", "0.0023", "1234.5678"}
	expectedVals = []string{"0", "10", "0.01", "0", "12.3457"}
	testDecimalShiftFunction(t, -2, nil, vals, expectedVals)

	vals = []string{"1234", "1000.0000", "1.0001", "0.0023", "1234.5678"}
	expectedVals = []string{"0", "10", "0.01", "0", "12.3456"}
	round = false
	testDecimalShiftFunction(t, -2, &round, vals, expectedVals)
}

func TestDecimalShiftArgs(t *testing.T) {
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType}}
	_, err := NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'decimal_shift' requires 2 or 3 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: decType}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err = NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'decimal_shift' requires 2 or 3 arguments - 4 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}}
	_, err = NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "decimal_shift' first argument must be of type decimal - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: decType}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}}
	_, err = NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "decimal_shift' second argument must be of type int - it is of type float"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: decType}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err = NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "decimal_shift' third argument must be of type bool - it is of type int"))
}

func testDecimalShiftFunction(t *testing.T, places int, round *bool, vals []string, expectedVals []string) {
	argCol := createDecimalCol(30, 4, []bool{true, false, false, false, false, false, false}, vals)

	expected := createDecimalCol(30, 4, []bool{true, false, false, false, false, false}, expectedVals)

	decType := &types.DecimalType{
		Precision: 30,
		Scale:     4,
	}

	var args []Expression
	if round != nil {
		args = []Expression{&ColumnExpr{colIndex: 0, exprType: decType},
			&IntegerConstantExpr{val: int64(places)},
			&BoolConstantExpr{val: *round},
		}
	} else {
		args = []Expression{&ColumnExpr{colIndex: 0, exprType: decType},
			&IntegerConstantExpr{val: int64(places)},
		}
	}

	fun, err := NewDecimalShiftFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{decType})
	batch := evbatch.NewBatch(schema, argCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLenFunctionString(t *testing.T) {
	argCol := createStringCol([]bool{true, false, false, false},
		[]string{"foo", "apples", "z", ""})

	expected := createIntCol([]bool{true, false, false, false}, []int64{0, 6, 1, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}

	fun, err := NewLenFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLenFunctionBytes(t *testing.T) {
	argCol := createBytesCol([]bool{true, false, false, false},
		[]string{"foo", "apples", "z", ""})

	expected := createIntCol([]bool{true, false, false, false}, []int64{0, 6, 1, 0})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}

	fun, err := NewLenFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, argCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLenFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err := NewLenFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'len' requires 1 argument - 2 found"))

	_, err = NewLenFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'len' requires 1 argument - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err = NewLenFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'len' argument must be of type string or bytes - it is of type int"))
}

func TestConcatFunctionString(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false},
		[]string{"", "", "", "apples", "", "bananas"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false},
		[]string{"", "", "", "pears", "peaches", ""})

	expected := createStringCol([]bool{true, true, true, false, false, false}, []string{"", "", "", "applespears", "peaches", "bananas"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewConcatFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestConcatFunctionBytes(t *testing.T) {
	argCol1 := createBytesCol([]bool{true, true, false, false, false, false},
		[]string{"", "", "", "apples", "", "bananas"})
	argCol2 := createBytesCol([]bool{true, false, true, false, false, false},
		[]string{"", "", "", "pears", "peaches", ""})

	expected := createBytesCol([]bool{true, true, true, false, false, false}, []string{"", "", "", "applespears", "peaches", "bananas"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewConcatFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestConcatFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewConcatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'concat' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewConcatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'concat' first argument must be of type string or bytes - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}
	_, err = NewConcatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'concat' arguments must be of same type - first is string and second is bytes"))
}

func TestStartsWithFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false, false},
		[]string{"", "", "", "apples", "", "bananas", "cherries"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false, false},
		[]string{"", "", "", "app", "", "", "foo"})

	expected := createBoolCol([]bool{true, true, true, false, false, false, false}, []bool{false, false, false, true, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewStartsWithFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestStartsWithFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewStartsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'starts_with' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewStartsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'starts_with' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewStartsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'starts_with' second argument must be of type string - it is of type bool"))
}

func TestEndsWithFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false, false},
		[]string{"", "", "", "apples", "", "bananas", "cherries"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false, false},
		[]string{"", "", "", "les", "", "", "foo"})

	expected := createBoolCol([]bool{true, true, true, false, false, false, false}, []bool{false, false, false, true, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewEndsWithFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEndsWithFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewEndsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ends_with' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewEndsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ends_with' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewEndsWithFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ends_with' second argument must be of type string - it is of type bool"))
}

func TestMatchesFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "", "news.uk", "news.usa", "weather.uk"})
	expected := createBoolCol([]bool{true, false, false, false, false}, []bool{false, false, true, true, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewStringConstantExpr("s")}

	fun, err := NewMatchesFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestMatchesFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewMatchesFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'matches' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewMatchesFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'matches' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewMatchesFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'matches' second argument must be a string literal"))
}

func TestTrimFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false, false, false, false},
		[]string{"", "", "", "aaaapplessss", "", "bbbbbananassss", "ccccherriessss", "wwwibblllesss", "  \t\t \n\r\n oranges\t\n \r\n \r"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false, false, false, false},
		[]string{"", "", "", "as", "", "b", "zq", "", " \t\r\n"})

	expected := createStringCol([]bool{true, true, true, false, false, false, false, false, false},
		[]string{"", "", "", "pple", "", "ananassss", "ccccherriessss", "wwwibblllesss", "oranges"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewTrimFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestTrimFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'trim' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'trim' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'trim' arguments must be of same type - first is string and second is bool"))
}

func TestLTrimFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false, false, false},
		[]string{"", "", "", "aaaapplessss", "", "bbbbbananassss", "ccccherriessss", "wwwibblllesss"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false, false, false},
		[]string{"", "", "", "as", "", "b", "zq", ""})

	expected := createStringCol([]bool{true, true, true, false, false, false, false, false},
		[]string{"", "", "", "pplessss", "", "ananassss", "ccccherriessss", "wwwibblllesss"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewLTrimFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLTrimFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewLTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ltrim' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewLTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ltrim' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewLTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'ltrim' arguments must be of same type - first is string and second is bool"))
}

func TestRTrimFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, true, false, false, false, false, false, false},
		[]string{"", "", "", "aaaapplessss", "", "bbbbbananassss", "ccccherriessss", "wwwibblllesss"})
	argCol2 := createStringCol([]bool{true, false, true, false, false, false, false, false},
		[]string{"", "", "", "as", "", "b", "zq", ""})

	expected := createStringCol([]bool{true, true, true, false, false, false, false, false},
		[]string{"", "", "", "aaaapple", "", "bbbbbananassss", "ccccherriessss", "wwwibblllesss"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewRTrimFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestRTrimFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewRTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'rtrim' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewRTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'rtrim' first argument must be of type string - it is of type bool"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}
	_, err = NewRTrimFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'rtrim' arguments must be of same type - first is string and second is bool"))
}

func TestToLowerFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "APPLES", "", "apples", "ApPlLeS"})
	expected := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "apples", "", "apples", "applles"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}

	fun, err := NewToLowerFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestToLowerFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToLowerFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_lower' requires 1 argument - 2 found"))

	_, err = NewToLowerFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_lower' requires 1 argument - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err = NewToLowerFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_lower' argument must be of type string - it is of type int"))
}

func TestToUpperFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "apples", "", "APPLES", "ApPlLeS"})
	expected := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "APPLES", "", "APPLES", "APPLLES"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}

	fun, err := NewToUpperFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestToUpperFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToUpperFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_upper' requires 1 argument - 2 found"))

	_, err = NewToUpperFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_upper' requires 1 argument - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err = NewToUpperFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_upper' argument must be of type string - it is of type int"))
}

func TestSubStrFunction(t *testing.T) {
	argCol1 := createStringCol([]bool{true, false, false, false, false, false, false, false, false, false},
		[]string{"", "", "", "apples and pears", "", "apples and pears", "apples and pears", "apples and pears", "apples and pears", "apples and pears"})

	argCol2 := createIntCol([]bool{false, true, false, false, false, false, false, false, false, false},
		[]int64{0, 0, 0, 3, 0, -1, -3, 5, 100, 10})

	argCol3 := createIntCol([]bool{false, false, true, false, false, false, false, false, false, false},
		[]int64{0, 0, 0, 12, 0, 5, -1, 100, 200, 3})

	expected := createStringCol([]bool{true, true, true, false, false, false, false, false, false, false},
		[]string{"", "", "", "les and p", "", "apple", "", "s and pears", "", ""})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}

	fun, err := NewSubStrFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, argCol1, argCol2, argCol3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSubStrFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewSubStrFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sub_str' requires 3 arguments - 2 found"))

	_, err = NewSubStrFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sub_str' requires 3 arguments - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}
	_, err = NewSubStrFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sub_str' first argument must be of type string - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}
	_, err = NewSubStrFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sub_str' second argument must be of type int - it is of type bytes"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes}}
	_, err = NewSubStrFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sub_str' third argument must be of type int - it is of type bytes"))
}

func TestReplaceFunction(t *testing.T) {
	strCol := createStringCol([]bool{true, true, false, false, false, false}, []string{"", "", "", "applapples", "", "apple"})
	oldCol := createStringCol([]bool{true, false, true, false, false, false}, []string{"", "", "", "ap", "a", ""})
	newCol := createStringCol([]bool{true, false, true, false, false, false}, []string{"", "", "", "zo", "b", "zoo"})

	expected := createStringCol([]bool{true, true, true, false, false, false}, []string{"", "", "", "zoplzoples", "", "zooazoopzoopzoolzooezoo"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}

	fun, err := NewReplaceFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, strCol, oldCol, newCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestReplaceFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewReplaceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'replace' requires 3 arguments - 2 found"))

	_, err = NewReplaceFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'replace' requires 3 arguments - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err = NewReplaceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'replace' first argument must be of type string - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err = NewReplaceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'replace' second argument must be of type string - it is of type bytes"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes}}
	_, err = NewReplaceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'replace' third argument must be of type string - it is of type bytes"))
}

func TestSprintfFunction(t *testing.T) {
	formatCol := createStringCol([]bool{true, false}, []string{"", "int:%d float:%f bool:%t decimal:%s string:%s bytes:%v timestamp:%d"})
	arg1Col := createIntCol([]bool{true, false}, []int64{0, 23})
	arg2Col := createFloatCol([]bool{true, false}, []float64{0, 23.23})
	arg3Col := createBoolCol([]bool{true, false}, []bool{false, true})
	arg4Col := createDecimalCol(38, 6, []bool{true, false}, []string{"", "1234.4321"})
	arg5Col := createStringCol([]bool{true, false}, []string{"", "apples"})
	arg6Col := createBytesCol([]bool{true, false}, []string{"", "apples"})
	arg7Col := createTimestampCol([]bool{true, false}, []int64{0, 1234})

	expected := createStringCol([]bool{true, false},
		[]string{"", "int:23 float:23.230000 bool:true decimal:1234.432100 string:apples bytes:[97 112 112 108 101 115] timestamp:1234"})

	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 4, exprType: decType},
		&ColumnExpr{colIndex: 5, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 6, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 7, exprType: types.ColumnTypeTimestamp},
	}

	fun, err := NewSprintfFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
		[]types.ColumnType{types.ColumnTypeString,
			types.ColumnTypeInt,
			types.ColumnTypeFloat,
			types.ColumnTypeBool,
			decType,
			types.ColumnTypeString,
			types.ColumnTypeBytes,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, formatCol, arg1Col, arg2Col, arg3Col, arg4Col, arg5Col, arg6Col, arg7Col)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSprintfFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewSprintfFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sprintf' requires at least 2 arguments - 1 found"))

	_, err = NewSprintfFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sprintf' requires at least 2 arguments - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err = NewSprintfFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'sprintf' first argument must be of type string - it is of type int"))
}

func TestToIntFunction(t *testing.T) {

	intCol := createIntCol([]bool{true, false, false, false, false}, []int64{3, 9, 7, -10, -12})
	floatCol := createFloatCol([]bool{true, false, false, false, false}, []float64{1.23, 13.89, 344.23, -456.89, -56.11})
	decCol := createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"12.34", "5858.23", "464.12", "-4746.4784", "-7667363.87"})
	stringCol := createStringCol([]bool{true, false, false, false, false}, []string{"4234", "1234.23", "476464.87", "-276236365", "68687678.23"})
	timestampCol := createTimestampCol([]bool{true, false, false, false, false}, []int64{4, 568, 585, -2339, 4746})

	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4"},
		[]types.ColumnType{
			types.ColumnTypeInt,
			types.ColumnTypeFloat,
			decType,
			types.ColumnTypeString,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, intCol, floatCol, decCol, stringCol, timestampCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 9, 7, -10, -12})
	testToIntType(t, args, expected, batch)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	// 13.89, 344.23,-456.89, -56.11})
	expected = createIntCol([]bool{true, false, false, false, false}, []int64{0, 13, 344, -456, -56})
	testToIntType(t, args, expected, batch)

	//  "5858.23", "464.12", "-4746.4784", "-7667363.87"
	args = []Expression{&ColumnExpr{colIndex: 2, exprType: decType}}
	expected = createIntCol([]bool{true, false, false, false, false}, []int64{0, 5858, 464, -4747, -7667364})
	testToIntType(t, args, expected, batch)

	// "1234.23", "476464.87", "-276236365", "68687678.23"
	args = []Expression{&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}
	expected = createIntCol([]bool{true, false, false, false, false}, []int64{0, 1234, 476464, -276236365, 68687678})
	testToIntType(t, args, expected, batch)

	// 568, 585, -2339, 4746
	args = []Expression{&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeTimestamp}}
	expected = createIntCol([]bool{true, false, false, false, false}, []int64{0, 568, 585, -2339, 4746})
	testToIntType(t, args, expected, batch)
}

func testToIntType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch) {
	fun, err := NewToIntFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToIntFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToIntFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_int' requires 1 argument"))

	_, err = NewToIntFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_int' requires 1 argument"))

	_, err = NewToIntFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "to_int' has operand with unsupported type bool"))

	_, err = NewToIntFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "to_int' has operand with unsupported type bytes"))
}

func TestToFloatFunction(t *testing.T) {

	intCol := createIntCol([]bool{true, false, false, false, false}, []int64{3, 9, 7, -10, -12})
	floatCol := createFloatCol([]bool{true, false, false, false, false}, []float64{1.23, 13.89, 344.23, -456.89, -56.11})
	decCol := createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"12.34", "5858.23", "464.12", "-4746.4784", "-7667363.87"})
	stringCol := createStringCol([]bool{true, false, false, false, false}, []string{"4234", "1234.23", "476464.87", "-276236365", "68687678.23"})
	timestampCol := createTimestampCol([]bool{true, false, false, false, false}, []int64{4, 568, 585, -2339, 4746})

	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4"},
		[]types.ColumnType{
			types.ColumnTypeInt,
			types.ColumnTypeFloat,
			decType,
			types.ColumnTypeString,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, intCol, floatCol, decCol, stringCol, timestampCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	expected := createFloatCol([]bool{true, false, false, false, false}, []float64{0, 9, 7, -10, -12})
	testToFloatType(t, args, expected, batch)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	// 13.89, 344.23,-456.89, -56.11})
	expected = createFloatCol([]bool{true, false, false, false, false}, []float64{0, 13.89, 344.23, -456.89, -56.11})
	testToFloatType(t, args, expected, batch)

	//  "5858.23", "464.12", "-4746.4784", "-7667363.87"
	args = []Expression{&ColumnExpr{colIndex: 2, exprType: decType}}
	expected = createFloatCol([]bool{true, false, false, false, false}, []float64{0, 5858.23, 464.12, -4746.4784, -7667363.87})
	testToFloatType(t, args, expected, batch)

	// "1234.23", "476464.87", "-276236365", "68687678.23"
	args = []Expression{&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}
	expected = createFloatCol([]bool{true, false, false, false, false}, []float64{0, 1234.23, 476464.87, -276236365, 68687678.23})
	testToFloatType(t, args, expected, batch)

	// 568, 585, -2339, 4746
	args = []Expression{&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeTimestamp}}
	expected = createFloatCol([]bool{true, false, false, false, false}, []float64{0, 568, 585, -2339, 4746})
	testToFloatType(t, args, expected, batch)
}

func testToFloatType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch) {
	fun, err := NewToFloatFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToFloatFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToFloatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_float' requires 1 argument"))

	_, err = NewToFloatFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_float' requires 1 argument"))

	_, err = NewToFloatFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "to_float' has operand with unsupported type bool"))

	_, err = NewToFloatFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "to_float' has operand with unsupported type bytes"))
}

func TestToStringFunction(t *testing.T) {

	intCol := createIntCol([]bool{true, false, false, false, false}, []int64{3, 9, 7, -10, -12})
	floatCol := createFloatCol([]bool{true, false, false, false, false}, []float64{1.23, 13.89, 344.23, -456.89, -56.11})
	decCol := createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"12.34", "5858.23", "464.12", "-4746.4784", "-7667363.87"})
	stringCol := createStringCol([]bool{true, false, false, false, false}, []string{"4234", "1234.23", "476464.87", "-276236365", "68687678.23"})
	timestampCol := createTimestampCol([]bool{true, false, false, false, false}, []int64{4, 568, 585, -2339, 4746})

	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4"},
		[]types.ColumnType{
			types.ColumnTypeInt,
			types.ColumnTypeFloat,
			decType,
			types.ColumnTypeString,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, intCol, floatCol, decCol, stringCol, timestampCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	expected := createStringCol([]bool{true, false, false, false, false}, []string{"0", "9", "7", "-10", "-12"})
	testToStringType(t, args, expected, batch)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	// 13.89, 344.23,-456.89, -56.11})
	expected = createStringCol([]bool{true, false, false, false, false}, []string{"0", "13.89", "344.23", "-456.89", "-56.11"})
	testToStringType(t, args, expected, batch)

	//  "5858.23", "464.12", "-4746.4784", "-7667363.87"
	args = []Expression{&ColumnExpr{colIndex: 2, exprType: decType}}
	expected = createStringCol([]bool{true, false, false, false, false}, []string{"0", "5858.230000", "464.120000", "-4746.478400", "-7667363.870000"})
	testToStringType(t, args, expected, batch)

	// "1234.23", "476464.87", "-276236365", "68687678.23"
	args = []Expression{&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}
	expected = createStringCol([]bool{true, false, false, false, false}, []string{"0", "1234.23", "476464.87", "-276236365", "68687678.23"})
	testToStringType(t, args, expected, batch)

	// 568, 585, -2339, 4746
	args = []Expression{&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeTimestamp}}
	expected = createStringCol([]bool{true, false, false, false, false}, []string{"0", "568", "585", "-2339", "4746"})
	testToStringType(t, args, expected, batch)
}

func testToStringType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch) {
	fun, err := NewToStringFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToStringFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToStringFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_string' requires 1 argument"))

	_, err = NewToStringFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_string' requires 1 argument"))
}

func TestToDecimalFunction(t *testing.T) {

	intCol := createIntCol([]bool{true, false, false, false, false}, []int64{3, 9, 7, -10, -12})
	floatCol := createFloatCol([]bool{true, false, false, false, false}, []float64{1.23, 13.89, 344.23, -456.89, -56.11})
	decCol := createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"12.34", "5858.23", "464.12", "-4746.4784", "-7667363.87"})
	stringCol := createStringCol([]bool{true, false, false, false, false}, []string{"4234", "1234.23", "476464.87", "-276236365", "68687678.23"})
	timestampCol := createTimestampCol([]bool{true, false, false, false, false}, []int64{4, 568, 585, -2339, 4746})

	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3", "c4"},
		[]types.ColumnType{
			types.ColumnTypeInt,
			types.ColumnTypeFloat,
			decType,
			types.ColumnTypeString,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, intCol, floatCol, decCol, stringCol, timestampCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	expected := createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"0", "9", "7", "-10", "-12"})
	testToDecimalType(t, args, expected, batch, 38, 6)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	// 13.89, 344.23,-456.89, -56.11})
	expected = createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"0", "13.89", "344.23", "-456.89", "-56.11"})
	testToDecimalType(t, args, expected, batch, 38, 6)

	expected = createDecimalCol(38, 2, []bool{true, false, false, false, false}, []string{"0", "13.89", "344.23", "-456.89", "-56.11"})
	testToDecimalType(t, args, expected, batch, 38, 2)

	//  "5858.23", "464.12", "-4746.4784", "-7667363.87"
	args = []Expression{&ColumnExpr{colIndex: 2, exprType: decType}}
	expected = createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"0", "5858.230000", "464.120000", "-4746.478400", "-7667363.870000"})
	testToDecimalType(t, args, expected, batch, 38, 6)

	// Test converting scale - including round down
	expected = createDecimalCol(38, 2, []bool{true, false, false, false, false}, []string{"0", "5858.23", "464.12", "-4746.48", "-7667363.87"})
	testToDecimalType(t, args, expected, batch, 38, 2)

	// "1234.23", "476464.87", "-276236365", "68687678.23"
	args = []Expression{&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}
	expected = createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"0", "1234.23", "476464.87", "-276236365", "68687678.23"})
	testToDecimalType(t, args, expected, batch, 38, 6)

	expected = createDecimalCol(38, 2, []bool{true, false, false, false, false}, []string{"0", "1234.23", "476464.87", "-276236365", "68687678.23"})
	testToDecimalType(t, args, expected, batch, 38, 2)

	// 568, 585, -2339, 4746
	args = []Expression{&ColumnExpr{colIndex: 4, exprType: types.ColumnTypeTimestamp}}
	expected = createDecimalCol(38, 6, []bool{true, false, false, false, false}, []string{"0", "568", "585", "-2339", "4746"})
	testToDecimalType(t, args, expected, batch, 38, 6)

	expected = createDecimalCol(38, 2, []bool{true, false, false, false, false}, []string{"0", "568", "585", "-2339", "4746"})
	testToDecimalType(t, args, expected, batch, 38, 2)
}

func testToDecimalType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch, prec int, scale int) {
	args = append(args, NewIntegerConstantExpr(int64(prec)))
	args = append(args, NewIntegerConstantExpr(int64(scale)))
	fun, err := NewToDecimalFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToDecimalFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToDecimalFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' requires 3 arguments"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "to_decimal' has operand with unsupported type bool"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' has operand with unsupported type bytes"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}, NewIntegerConstantExpr(10)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' second argument must be an int constant"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(38), &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' third argument must be an int constant"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(39), NewIntegerConstantExpr(6)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' precision must be between 1 and 38 inclusive"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(0), NewIntegerConstantExpr(6)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' precision must be between 1 and 38 inclusive"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(38), NewIntegerConstantExpr(39)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' scale must be between 0 and 38 inclusive"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(38), NewIntegerConstantExpr(-1)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' scale must be between 0 and 38 inclusive"))

	_, err = NewToDecimalFunction([]Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		NewIntegerConstantExpr(10), NewIntegerConstantExpr(11)}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_decimal' scale cannot be greater than precision"))
}

func TestToBytesFunction(t *testing.T) {

	stringCol := createStringCol([]bool{true, false, false, false}, []string{"apples", "pears", "oranges", "grapes"})
	bytesCol := createBytesCol([]bool{true, false, false, false}, []string{"apples", "pears", "oranges", "grapes"})

	schema := evbatch.NewEventSchema([]string{"c0", "c1"},
		[]types.ColumnType{
			types.ColumnTypeString,
			types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, stringCol, bytesCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	expected := createBytesCol([]bool{true, false, false, false}, []string{"", "pears", "oranges", "grapes"})
	testToBytesType(t, args, expected, batch)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	expected = createBytesCol([]bool{true, false, false, false}, []string{"", "pears", "oranges", "grapes"})
	testToBytesType(t, args, expected, batch)
}

func testToBytesType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch) {
	fun, err := NewToBytesFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToBytesFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewToBytesFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_bytes' requires 1 argument"))

	_, err = NewToBytesFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_bytes' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}
	_, err = NewToBytesFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_bytes' has operand with unsupported type timestamp"))
}

func TestToTimestampFunction(t *testing.T) {

	intCol := createIntCol([]bool{true, false, false, false}, []int64{23, 45, 234, 543})
	timestampCol := createTimestampCol([]bool{true, false, false, false}, []int64{23452, 1234, 324, 45})

	schema := evbatch.NewEventSchema([]string{"c0", "c1"},
		[]types.ColumnType{
			types.ColumnTypeInt,
			types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, intCol, timestampCol)

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	expected := createTimestampCol([]bool{true, false, false, false}, []int64{0, 45, 234, 543})
	testToTimestampType(t, args, expected, batch)

	args = []Expression{&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}}
	expected = createTimestampCol([]bool{true, false, false, false}, []int64{0, 1234, 324, 45})
	testToTimestampType(t, args, expected, batch)
}

func testToTimestampType(t *testing.T, args []Expression, expected evbatch.Column, batch *evbatch.Batch) {
	fun, err := NewToTimestampFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	colsEqual(t, expected, res)
}

func TestToTimestampFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err := NewToTimestampFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_timestamp' requires 1 argument"))

	_, err = NewToTimestampFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_timestamp' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}}
	_, err = NewToTimestampFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'to_timestamp' has operand with unsupported type float"))
}

func TestFormatDateFunction(t *testing.T) {

	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false},
		[]int64{0, u1, u2, u3, u4})
	argCol2 := createStringCol([]bool{false, true, false, false, false},
		[]string{"", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000"})

	expected := createStringCol([]bool{true, true, false, false, false},
		[]string{"", "", "2024-05-24 11:56:22.201", "2025-06-25 12:57:23.202", "2026-07-26 09:58:05.203"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewFormatDateFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestFormatDateFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewFormatDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'format_date' requires 2 arguments"))

	_, err = NewFormatDateFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'format_date' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewFormatDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'format_date' first argument must be of type timestamp - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err = NewFormatDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'format_date' second argument must be of type string - it is int"))
}

func TestParseDateFunction(t *testing.T) {
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)

	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "2023-04-23 10:55:21.200", "2024-05-24 11:56:22.201", "2025-06-25 12:57:23.202", "2026-07-26 09:58:05.203"})
	argCol2 := createStringCol([]bool{false, true, false, false, false},
		[]string{"", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05.000"})

	expected := createTimestampCol([]bool{true, true, false, false, false},
		[]int64{0, 0, u2, u3, u4})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewParseDateFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseDateFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewParseDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'parse_date' requires 2 arguments"))

	_, err = NewParseDateFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'parse_date' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewParseDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'parse_date' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}
	_, err = NewParseDateFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'parse_date' second argument must be of type string - it is int"))
}

func TestYearFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 2023, 2024, 2025, 2026})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewYearFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseYearFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewYearFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'year' requires 1 argument"))

	_, err = NewYearFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'year' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewYearFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'year' argument must be of type timestamp - it is string"))
}

func TestMonthFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 4, 5, 6, 7})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewMonthFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseMonthFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewMonthFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'month' requires 1 argument"))

	_, err = NewMonthFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'month' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewMonthFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'month' argument must be of type timestamp - it is string"))
}

func TestDayFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 23, 24, 25, 26})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewDayFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseDayFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewDayFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'day' requires 1 argument"))

	_, err = NewDayFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'day' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewDayFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'day' argument must be of type timestamp - it is string"))
}

func TestHourFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 10, 11, 12, 9})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewHourFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseHourFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewHourFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'hour' requires 1 argument"))

	_, err = NewHourFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'hour' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewHourFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'hour' argument must be of type timestamp - it is string"))
}

func TestMinuteFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 55, 56, 57, 58})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewMinuteFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseMinuteFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewMinuteFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'minute' requires 1 argument"))

	_, err = NewMinuteFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'minute' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewMinuteFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'minute' argument must be of type timestamp - it is string"))
}

func TestSecondFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 21, 22, 23, 5})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewSecondFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseSecondFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewSecondFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'second' requires 1 argument"))

	_, err = NewSecondFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'second' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewSecondFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'second' argument must be of type timestamp - it is string"))
}

func TestMillisFunction(t *testing.T) {
	d1 := time.Date(2023, 4, 23, 10, 55, 21, 200000000, time.UTC)
	d2 := time.Date(2024, 5, 24, 11, 56, 22, 201000000, time.UTC)
	d3 := time.Date(2025, 6, 25, 12, 57, 23, 202000000, time.UTC)
	d4 := time.Date(2026, 7, 26, 9, 58, 5, 203000000, time.UTC)
	u1 := d1.UnixMilli()
	u2 := d2.UnixMilli()
	u3 := d3.UnixMilli()
	u4 := d4.UnixMilli()

	argCol1 := createTimestampCol([]bool{true, false, false, false, false}, []int64{0, u1, u2, u3, u4})

	expected := createIntCol([]bool{true, false, false, false, false}, []int64{0, 200, 201, 202, 203})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}

	fun, err := NewMillisFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestParseMillisFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewMillisFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'millis' requires 1 argument"))

	_, err = NewMillisFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'millis' requires 1 argument"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewMillisFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'millis' argument must be of type timestamp - it is string"))
}

func TestNowFunction(t *testing.T) {
	fun, err := NewNowFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	col1 := createIntCol([]bool{false, false, false, false}, []int64{1, 2, 3, 4})
	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1)

	start := time.Now().UTC().UnixMilli()
	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)
	end := time.Now().UTC().UnixMilli()

	tsCol, ok := res.(*evbatch.TimestampColumn)
	require.True(t, ok)
	require.Equal(t, 4, tsCol.Len())
	for i := 0; i < 4; i++ {
		tsVal := tsCol.Get(i)
		require.GreaterOrEqual(t, tsVal.Val, start)
		require.LessOrEqual(t, tsVal.Val, end)
	}
}

func TestParseNowFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}}
	_, err := NewNowFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'now' does not take any arguments"))
}

func TestJsonIntFunctionBytes(t *testing.T) {
	json := `{"x":{"y":23,"z":null,"s":"2300"},"y":2323}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.s"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createIntCol([]bool{true, true, false, true, false, true, false},
		[]int64{0, 0, 23, 0, 2323, 0, 2300})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonIntFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonIntFunctionString(t *testing.T) {
	json := `{"x":{"y":23,"z":null},"y":2323}`
	pathCol := createStringCol([]bool{true, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y"})
	stringCol := createStringCol([]bool{false, true, false, false, false},
		[]string{json, json, json, json, json})

	expectedCol := createIntCol([]bool{true, true, false, true, false},
		[]int64{0, 0, 23, 0, 2323})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonIntFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)

}

func TestJsonIntFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonIntFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_int' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonIntFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_int' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonIntFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_int' second argument must be of type string or bytes - it is float"))
}

func TestJsonFloatFunctionBytes(t *testing.T) {
	json := `{"x":{"y":23.23,"z":null,"s":"2300.23"},"y":2323.23}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.s"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createFloatCol([]bool{true, true, false, true, false, true, false},
		[]float64{0, 0, 23.23, 0, 2323.23, 0, 2300.23})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonFloatFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonFloatFunctionString(t *testing.T) {
	json := `{"x":{"y":23.23,"z":null,"s":"2300.23"},"y":2323.23}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.s"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createFloatCol([]bool{true, true, false, true, false, true, false},
		[]float64{0, 0, 23.23, 0, 2323.23, 0, 2300.23})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonFloatFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)

}

func TestJsonFloatFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonFloatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_float' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonFloatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_float' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonFloatFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_float' second argument must be of type string or bytes - it is float"))
}

func TestJsonBoolFunctionBytes(t *testing.T) {
	json := `{"x":{"y":true,"z":null,"s":"true"},"y":false}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.s"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createBoolCol([]bool{true, true, false, true, false, true, false},
		[]bool{false, false, true, false, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonBoolFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonBoolFunctionString(t *testing.T) {
	json := `{"x":{"y":true,"z":null,"s":"true"},"y":false}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.s"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createBoolCol([]bool{true, true, false, true, false, true, false},
		[]bool{false, false, true, false, false, false, true})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonBoolFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonBoolFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonBoolFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_bool' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonBoolFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_bool' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonBoolFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_bool' second argument must be of type string or bytes - it is float"))
}

func TestJsonStringFunctionBytes(t *testing.T) {
	json := `{"x":{"y":"foo","z":null,"i":23},"y":"wibble"}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.i", "x"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, true, false, true, false, false},
		[]string{"", "", "foo", "", "wibble", "", "23", `{"y":"foo","z":null,"i":23}`})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonStringFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonStringFunctionString(t *testing.T) {
	json := `{"x":{"y":"foo","z":null,"i":23},"y":"wibble"}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.i", "x"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, true, false, true, false, false},
		[]string{"", "", "foo", "", "wibble", "", "23", `{"y":"foo","z":null,"i":23}`})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonStringFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonStringFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonStringFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_string' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonStringFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_string' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonStringFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_string' second argument must be of type string or bytes - it is float"))
}

func TestJsonRawFunctionBytes(t *testing.T) {
	json := `{"x":{"y":"foo","z":null,"i":23},"y":"wibble"}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.i", "x"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, true, false, true, false, false},
		[]string{"", "", `"foo"`, "", `"wibble"`, "", "23", `{"y":"foo","z":null,"i":23}`})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonRawFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonRawFunctionString(t *testing.T) {
	json := `{"x":{"y":"foo","z":null,"i":23},"y":"wibble"}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x.i", "x"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, true, false, true, false, false},
		[]string{"", "", `"foo"`, "", `"wibble"`, "", "23", `{"y":"foo","z":null,"i":23}`})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonRawFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonRawFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonRawFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_raw' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonRawFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_raw' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonRawFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_raw' second argument must be of type string or bytes - it is float"))
}

func TestJsonIsNullFunctionBytes(t *testing.T) {
	json := `{"x":{"y":null,"z":"bar",},"y":null}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createBoolCol([]bool{true, true, false, false, false, true, false},
		[]bool{false, false, true, false, true, false, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonIsNullFunctionString(t *testing.T) {
	json := `{"x":{"y":null,"z":"bar",},"y":null}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "y", "notfound", "x"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json})

	expectedCol := createBoolCol([]bool{true, true, false, false, false, true, false},
		[]bool{false, false, true, false, true, false, false})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonIsNullFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonIsNullFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonIsNullFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_is_null' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonIsNullFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_is_null' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonIsNullFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_is_null' second argument must be of type string or bytes - it is float"))
}

func TestJsonTypeFunctionBytes(t *testing.T) {
	json := `{"x":{"y":"foo","z":23},"q": true, "y":null,"z":[23]}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "q", "y", "z", "x", "notfound"})
	bytesCol := createBytesCol([]bool{false, true, false, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, false, false, false, false, false, true},
		[]string{"", "", "string", "number", "bool", "null", "json", "json", ""})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}

	fun, err := NewJsonTypeFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, pathCol, bytesCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonTypeFunctionString(t *testing.T) {
	json := `{"x":{"y":"foo","z":23},"q": true, "y":null,"z":[23]}`
	pathCol := createStringCol([]bool{true, false, false, false, false, false, false, false, false},
		[]string{"", "", "x.y", "x.z", "q", "y", "z", "x", "notfound"})
	stringCol := createStringCol([]bool{false, true, false, false, false, false, false, false, false},
		[]string{json, json, json, json, json, json, json, json, json})

	expectedCol := createStringCol([]bool{true, true, false, false, false, false, false, false, true},
		[]string{"", "", "string", "number", "bool", "null", "json", "json", ""})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}}

	fun, err := NewJsonTypeFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, pathCol, stringCol)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expectedCol, res)
}

func TestJsonTypeFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err := NewJsonTypeFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_type' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewJsonTypeFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_type' first argument must be of type string - it is int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewJsonTypeFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'json_type' second argument must be of type string or bytes - it is float"))
}

func TestKafkaHeaders(t *testing.T) {

	// first build headers

	hNameCol1 := createStringCol([]bool{false, false}, []string{"h1", "h1"})
	hValCol1 := createStringCol([]bool{false, false}, []string{"h1.v1", "h1.v2"})

	hNameCol2 := createStringCol([]bool{false, false}, []string{"h2", "h2"})
	hValCol2 := createStringCol([]bool{false, false}, []string{"h2.v1", "h2.v2"})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 3, exprType: types.ColumnTypeString}}

	buildHeadersFun, err := NewKafkaBuildHeadersFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2", "c3"},
		[]types.ColumnType{types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, hNameCol1, hValCol1, hNameCol2, hValCol2)

	headersRes, err := EvalColumn(buildHeadersFun, batch)
	require.NoError(t, err)

	// now get header h1

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString},
		&ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	getHeaderFun, err := NewKafkaHeaderFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema = evbatch.NewEventSchema([]string{"c0", "c1"},
		[]types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes})
	batch = evbatch.NewBatch(schema, hNameCol1, headersRes)

	res, err := EvalColumn(getHeaderFun, batch)
	require.NoError(t, err)

	expected := createBytesCol([]bool{false, false}, []string{"h1.v1", "h1.v2"})

	// now get header h2

	batch = evbatch.NewBatch(schema, hNameCol2, headersRes)

	res, err = EvalColumn(getHeaderFun, batch)
	require.NoError(t, err)

	expected = createBytesCol([]bool{false, false}, []string{"h2.v1", "h2.v2"})

	colsEqual(t, expected, res)

	// get non existent header

	nonExistCol := createStringCol([]bool{false, false}, []string{"who", "who"})

	batch = evbatch.NewBatch(schema, nonExistCol, headersRes)

	res, err = EvalColumn(getHeaderFun, batch)
	require.NoError(t, err)

	expected = createBytesCol([]bool{true, true}, []string{"", ""})

	colsEqual(t, expected, res)
}

func TestKafkaBuildHeadersArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewKafkaBuildHeadersFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_build_headers' requires at least 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}}
	_, err = NewKafkaBuildHeadersFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_build_headers' requires an even number of arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeFloat}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewKafkaBuildHeadersFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_build_headers' header name expression must be of type string - expression at position 2 is of type float"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}}
	_, err = NewKafkaBuildHeadersFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_build_headers' header value expression must be of type string or bytes - expression at position 2 is of type timestamp"))
}

func TestKafkaHeaderArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewKafkaHeaderFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_header' requires 2 arguments"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewKafkaHeaderFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_header' first argument must be of type string"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}}
	_, err = NewKafkaHeaderFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'kafka_header' second argument must be of type bytes"))
}

func TestBytesSliceFunction(t *testing.T) {
	argCol1 := createBytesCol([]bool{true, false, false, false, false, false, false, false, false, false},
		[]string{"", "", "", "apples and pears", "", "apples and pears", "apples and pears", "apples and pears", "apples and pears", "apples and pears"})

	argCol2 := createIntCol([]bool{false, true, false, false, false, false, false, false, false, false},
		[]int64{0, 0, 0, 3, 0, -1, -3, 5, 100, 10})

	argCol3 := createIntCol([]bool{false, false, true, false, false, false, false, false, false, false},
		[]int64{0, 0, 0, 12, 0, 5, -1, 100, 200, 3})

	expected := createBytesCol([]bool{true, true, true, false, false, false, false, false, false, false},
		[]string{"", "", "", "les and p", "", "apple", "", "s and pears", "", ""})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}

	fun, err := NewBytesSliceFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1", "c2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, argCol1, argCol2, argCol3)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestBytesSliceFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err := NewBytesSliceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'bytes_slice' requires 3 arguments - 2 found"))

	_, err = NewBytesSliceFunction([]Expression{}, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'bytes_slice' requires 3 arguments - 0 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}
	_, err = NewBytesSliceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'bytes_slice' first argument must be of type bytes - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeInt}}
	_, err = NewBytesSliceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'bytes_slice' second argument must be of type int - it is of type bytes"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt},
		&ColumnExpr{colIndex: 2, exprType: types.ColumnTypeBytes}}
	_, err = NewBytesSliceFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'bytes_slice' third argument must be of type int - it is of type bytes"))
}

func TestUint64BEFunction(t *testing.T) {
	b1 := make([]byte, 8)
	binary.BigEndian.PutUint64(b1, 12345)
	b2 := make([]byte, 12)
	binary.BigEndian.PutUint64(b2[4:], 645454)
	b3 := make([]byte, 20)
	binary.BigEndian.PutUint64(b3[7:], 457464)

	argCol1 := createBytesCol([]bool{true, false, false, false}, []string{"", string(b1), string(b2), string(b3)})
	argCol2 := createIntCol([]bool{false, false, false, false}, []int64{0, 0, 4, 7})
	expected := createIntCol([]bool{true, false, false, false}, []int64{0, 12345, 645454, 457464})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}

	fun, err := NewUint64BEFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestUint64LEFunction(t *testing.T) {
	b1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b1, 12345)
	b2 := make([]byte, 12)
	binary.LittleEndian.PutUint64(b2[4:], 645454)
	b3 := make([]byte, 20)
	binary.LittleEndian.PutUint64(b3[7:], 457464)

	argCol1 := createBytesCol([]bool{true, false, false, false}, []string{"", string(b1), string(b2), string(b3)})
	argCol2 := createIntCol([]bool{false, false, false, false}, []int64{0, 0, 4, 7})
	expected := createIntCol([]bool{true, false, false, false}, []int64{0, 12345, 645454, 457464})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}

	fun, err := NewUint64LEFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0", "c1"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, argCol1, argCol2)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestUint64BEFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}
	_, err := NewUint64BEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_be' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err = NewUint64BEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_be' first argument must be of type bytes - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewUint64BEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_be' second argument must be of type int - it is of type bytes"))
}

func TestUint64LEFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}}
	_, err := NewUint64LEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_le' requires 2 arguments - 1 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err = NewUint64LEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_le' first argument must be of type bytes - it is of type int"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}}
	_, err = NewUint64LEFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'uint64_le' second argument must be of type int - it is of type bytes"))
}

func TestAbsFunctionInt(t *testing.T) {
	argCol1 := createIntCol([]bool{true, false, false, false}, []int64{23, 0, -1234, 1234})
	expected := createIntCol([]bool{true, false, false, false}, []int64{0, 0, 1234, 1234})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}}

	fun, err := NewAbsFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAbsFunctionFloat(t *testing.T) {
	argCol1 := createFloatCol([]bool{true, false, false, false}, []float64{23.25, 0, -1234.25, 1234.25})
	expected := createFloatCol([]bool{true, false, false, false}, []float64{0, 0, 1234.25, 1234.25})

	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}}

	fun, err := NewAbsFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAbsFunctionDecimal(t *testing.T) {
	argCol1 := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{true, false, false, false}, []string{"23.25", "0", "-1234.25", "1234.25"})
	expected := createDecimalCol(types.DefaultDecimalPrecision, types.DefaultDecimalScale, []bool{true, false, false, false}, []string{"0", "0", "1234.25", "1234.25"})

	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: decType}}

	fun, err := NewAbsFunction(args, &parser.FunctionExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"c0"}, []types.ColumnType{decType})
	batch := evbatch.NewBatch(schema, argCol1)

	res, err := EvalColumn(fun, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAbsFunctionArgs(t *testing.T) {
	args := []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}, &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}}
	_, err := NewAbsFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'abs' requires 1 argument - 2 found"))

	args = []Expression{&ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}}
	_, err = NewAbsFunction(args, &parser.FunctionExprDesc{})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.StatementError))
	require.True(t, strings.Contains(err.Error(), "'abs' argument must be of type int, float or decimal - it is of type string"))
}

func createIntCol(nulls []bool, values []int64) *evbatch.IntColumn {
	builder := evbatch.NewIntColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildIntColumn()
}

func createFloatCol(nulls []bool, values []float64) *evbatch.FloatColumn {
	builder := evbatch.NewFloatColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildFloatColumn()
}

func createBoolCol(nulls []bool, values []bool) *evbatch.BoolColumn {
	builder := evbatch.NewBoolColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildBoolColumn()
}

func createDecimalCol(prec int, scale int, nulls []bool, values []string) *evbatch.DecimalColumn {
	decType := &types.DecimalType{
		Precision: prec,
		Scale:     scale,
	}
	builder := evbatch.NewDecimalColBuilder(decType)
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			dec := createDecimal(val, prec, scale)
			builder.Append(dec)
		}
	}
	return builder.BuildDecimalColumn()
}

func createStringCol(nulls []bool, values []string) *evbatch.StringColumn {
	builder := evbatch.NewStringColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.BuildStringColumn()
}

func createBytesCol(nulls []bool, values []string) *evbatch.BytesColumn {
	builder := evbatch.NewBytesColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append([]byte(val))
		}
	}
	return builder.BuildBytesColumn()
}

func createTimestampCol(nulls []bool, values []int64) *evbatch.TimestampColumn {
	builder := evbatch.NewTimestampColBuilder()
	for i, val := range values {
		if nulls[i] {
			builder.AppendNull()
		} else {
			builder.Append(types.NewTimestamp(val))
		}
	}
	return builder.BuildTimestampColumn()
}

func createDecimal(str string, prec int, scale int) types.Decimal {
	num, err := decimal128.FromString(str, int32(prec), int32(scale))
	if err != nil {
		panic(err)
	}
	return types.Decimal{
		Num:       num,
		Precision: prec,
		Scale:     scale,
	}
}

func colsEqual(t *testing.T, col1 evbatch.Column, col2 evbatch.Column) {

	switch col1.(type) {
	case *evbatch.IntColumn:
		_, ok := col2.(*evbatch.IntColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.IntColumn)
		icol2 := col2.(*evbatch.IntColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				log.Debugf("got val: %d", icol2.Get(i))
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.FloatColumn:
		_, ok := col2.(*evbatch.FloatColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.FloatColumn)
		icol2 := col2.(*evbatch.FloatColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.DecimalColumn:
		_, ok := col2.(*evbatch.DecimalColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.DecimalColumn)
		icol2 := col2.(*evbatch.DecimalColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.TimestampColumn:
		_, ok := col2.(*evbatch.TimestampColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.TimestampColumn)
		icol2 := col2.(*evbatch.TimestampColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.BoolColumn:
		_, ok := col2.(*evbatch.BoolColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.BoolColumn)
		icol2 := col2.(*evbatch.BoolColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.StringColumn:
		_, ok := col2.(*evbatch.StringColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.StringColumn)
		icol2 := col2.(*evbatch.StringColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				log.Debugf("got val: %s", icol2.Get(i))
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	case *evbatch.BytesColumn:
		_, ok := col2.(*evbatch.BytesColumn)
		require.True(t, ok)
		icol1 := col1.(*evbatch.BytesColumn)
		icol2 := col2.(*evbatch.BytesColumn)
		require.Equal(t, icol1.Len(), icol2.Len())
		for i := 0; i < icol1.Len(); i++ {
			if icol1.IsNull(i) {
				require.True(t, icol2.IsNull(i))
			} else {
				require.False(t, icol2.IsNull(i))
				require.Equal(t, icol1.Get(i), icol2.Get(i))
			}
		}
	default:
		panic("unexpected type")
	}
}
