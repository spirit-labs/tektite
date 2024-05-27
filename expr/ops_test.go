package expr

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestAddOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})
	expected := createIntCol([]bool{false, false, true, false, true, true, false}, []int64{3, 5, 0, 9, 0, 0, 15})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAddOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.2, 2.2, 3.25, 4.25, 5.25, 6.25, 7.25})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{6.2, 7.2, 8.25, 9.25, 10.25, 11.25, 12.25})

	expected := createFloatCol([]bool{false, false, true, false, true, true, false}, []float64{7.4, 9.4, 0, 13.5, 0, 0, 19.5})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAddOperatorDecimal(t *testing.T) {
	testAddOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testAddOperatorDecimal(t, 20, 3, 20, 3)
	testAddOperatorDecimal(t, 30, 6, 20, 3)
}

func testAddOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1000.123", "1001.123", "1002.123", "1003.123", "1004.123", "1005.123", "1006.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"2000.123", "2001.123", "2002.123", "2003.123", "2004.123", "2005.123", "2006.123"})

	prec, scale := types.AddResultPrecScale(prec1, scale1, prec2, scale2)

	expected := createDecimalCol(prec, scale, []bool{false, false, true, false, true, true, false},
		[]string{"3000.246", "3002.246", "0", "3006.246", "0", "0", "3012.246"})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	require.Equal(t, prec, oper.ResultType().(*types.DecimalType).Precision)
	require.Equal(t, scale, oper.ResultType().(*types.DecimalType).Scale)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAddOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createTimestampCol([]bool{false, false, true, false, true, true, false}, []int64{3, 5, 0, 9, 0, 0, 15})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestAddOperatorUnsupportedTypes(t *testing.T) {
	testAddOperatorUnsupportedTypes(t, types.ColumnTypeBool)
	testAddOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testAddOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
}

func testAddOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestAddOperatorDifferentTypes(t *testing.T) {
	testAddOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testAddOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testAddOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testAddOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewAddOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestSubtractOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createIntCol([]bool{false, false, true, false, true, true, false}, []int64{1, 1, 0, 1, 0, 0, 1})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}

	oper, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSubtractOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.2, 2.2, 3.25, 4.25, 5.25, 6.25, 7.25})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{6.2, 7.2, 8.25, 9.25, 10.25, 11.25, 12.25})

	expected := createFloatCol([]bool{false, false, true, false, true, true, false}, []float64{5, 5, 0, 5, 0, 0, 5})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}

	oper, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSubtractOperatorDecimal(t *testing.T) {
	testSubtractOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testSubtractOperatorDecimal(t, 20, 3, 20, 3)
	testSubtractOperatorDecimal(t, 30, 6, 20, 3)
}

func testSubtractOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1000.123", "1001.123", "1002.123", "1003.123", "1004.123", "1005.123", "1006.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"2000.123", "2001.123", "2002.123", "2003.123", "2004.123", "2005.123", "2006.123"})

	prec, scale := types.AddResultPrecScale(prec1, scale1, prec2, scale2)

	expected := createDecimalCol(prec, scale, []bool{false, false, true, false, true, true, false},
		[]string{"1000.000", "1000.0", "0", "1000.000", "0", "0", "1000.000"})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 1, exprType: decType1}
	right := &ColumnExpr{colIndex: 0, exprType: decType2}

	oper, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	require.Equal(t, prec, oper.ResultType().(*types.DecimalType).Precision)
	require.Equal(t, scale, oper.ResultType().(*types.DecimalType).Scale)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSubtractOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createTimestampCol([]bool{false, false, true, false, true, true, false}, []int64{1, 1, 0, 1, 0, 0, 1})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}

	oper, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestSubtractOperatorUnsupportedTypes(t *testing.T) {
	testSubtractOperatorUnsupportedTypes(t, types.ColumnTypeBool)
	testSubtractOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testSubtractOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
}

func testSubtractOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestSubtractOperatorDifferentTypes(t *testing.T) {
	testSubtractOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testSubtractOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testSubtractOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testSubtractOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestMultiplyOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createIntCol([]bool{false, false, true, false, true, true, false}, []int64{2, 6, 0, 20, 0, 0, 56})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}

	oper, err := NewMultiplyOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestMultiplyOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.2, 2.2, 3.25, 4.25, 5.25, 6.25, 7.25})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{6.2, 7.2, 8.25, 9.25, 10.25, 11.25, 12.25})

	expected := createFloatCol([]bool{false, false, true, false, true, true, false}, []float64{7.4399999999999995, 15.840000000000002, 0, 39.3125, 0, 0, 88.8125})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}

	oper, err := NewMultiplyOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestMultiplyOperatorDecimal(t *testing.T) {
	testMultiplyOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testMultiplyOperatorDecimal(t, 20, 3, 20, 3)
	testMultiplyOperatorDecimal(t, 30, 6, 20, 3)
}

func testMultiplyOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1000.123", "1001.123", "1002.123", "1003.123", "1004.123", "1005.123", "1006.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"2000.123", "2001.123", "2002.123", "2003.123", "2004.123", "2005.123", "2006.123"})
	expectedPrec, expectedScale := types.MultiplyResultPrecScale(prec1, scale1, prec2, scale2)
	expected := createDecimalCol(expectedPrec, expectedScale, []bool{false, false, true, false, true, true, false},
		[]string{"2000369.015129", "2003370.261129", "0", "2009378.753129", "0", "0", "2018406.491129"})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 1, exprType: decType1}
	right := &ColumnExpr{colIndex: 0, exprType: decType2}

	oper, err := NewMultiplyOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	require.Equal(t, expectedPrec, oper.ResultType().(*types.DecimalType).Precision)
	require.Equal(t, expectedScale, oper.ResultType().(*types.DecimalType).Scale)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestMultiplyOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createTimestampCol([]bool{false, false, true, false, true, true, false}, []int64{2, 6, 0, 20, 0, 0, 56})

	left := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}

	oper, err := NewMultiplyOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestMultiplyOperatorUnsupportedTypes(t *testing.T) {
	testMultiplyOperatorUnsupportedTypes(t, types.ColumnTypeBool)
	testMultiplyOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testMultiplyOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
}

func testMultiplyOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewMultiplyOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestMultiplyOperatorDifferentTypes(t *testing.T) {
	testMultiplyOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testMultiplyOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testMultiplyOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testMultiplyOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestDivideOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{4, 9, 12, 15, 30, 42, 64})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createIntCol([]bool{false, false, true, false, true, true, false}, []int64{2, 3, 0, 3, 0, 0, 8})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewDivideOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestDivideOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{4.4, 9.9, 12.12, 15.5, 30.3, 42.4, 64.4})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{2.2, 3.3, 4.4, 5.5, 6.3, 7.4, 8.2})

	expected := createFloatCol([]bool{false, false, true, false, true, true, false}, []float64{2, 3.0000000000000004, 0, 2.8181818181818183, 0, 0, 7.8536585365853675})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewDivideOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestDivideOperatorTimestamp(t *testing.T) {
	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{4, 9, 12, 15, 30, 42, 64})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createTimestampCol([]bool{false, false, true, false, true, true, false}, []int64{2, 3, 0, 3, 0, 0, 8})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewDivideOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestDivideOperatorDividebyZero(t *testing.T) {
	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{4})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{0})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewDivideOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	_, _, err = oper.EvalInt(0, batch)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "divide by zero"))
}

func TestDivideOperatorUnsupportedTypes(t *testing.T) {
	testDivideOperatorUnsupportedTypes(t, types.ColumnTypeBool)
	testDivideOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testDivideOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
}

func testDivideOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewDivideOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestDivideOperatorDifferentTypes(t *testing.T) {
	testDivideOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testDivideOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
}

func testDivideOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewSubtractOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestModulusOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{5, 11, 12, 16, 30, 42, 67})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{2, 3, 4, 5, 6, 7, 8})

	expected := createIntCol([]bool{false, false, true, false, true, true, false}, []int64{1, 2, 0, 1, 0, 0, 3})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewModulusOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestModulusOperatorDividebyZero(t *testing.T) {
	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{4})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{0})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewModulusOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	_, _, err = oper.EvalInt(0, batch)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "modulus by zero"))
}

func TestModulusOperatorUnsupportedTypes(t *testing.T) {
	testModulusOperatorUnsupportedTypes(t, types.ColumnTypeFloat)
	testModulusOperatorUnsupportedTypes(t, types.ColumnTypeBool)
	testModulusOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testModulusOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
	testModulusOperatorUnsupportedTypes(t, &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	})
	testModulusOperatorUnsupportedTypes(t, types.ColumnTypeTimestamp)
}

func testModulusOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewModulusOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLogicalNotOperator(t *testing.T) {

	col1 := createBoolCol([]bool{false, false, true, false, false, true, false},
		[]bool{true, false, true, true, false, false, true})

	expected := createBoolCol([]bool{false, false, true, false, false, true, false},
		[]bool{false, true, false, false, true, false, false})

	colExpr := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}
	oper, err := NewLogicalNotOperator(colExpr, &parser.UnaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1"}, []types.ColumnType{types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLogicalNotOperatorUnsupportedTypes(t *testing.T) {
	testLogicalNotOperatorUnsupportedTypes(t, types.ColumnTypeInt)
	testLogicalNotOperatorUnsupportedTypes(t, types.ColumnTypeFloat)
	testLogicalNotOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testLogicalNotOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
	testLogicalNotOperatorUnsupportedTypes(t, &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	})
	testLogicalNotOperatorUnsupportedTypes(t, types.ColumnTypeTimestamp)
}

func testLogicalNotOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	operand := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewLogicalNotOperator(operand, &parser.UnaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestEqualsOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, false, true, false, false, true, false}, []bool{true, true, false, true, false, true, false})
	col2 := createBoolCol([]bool{false, false, true, false, true, false, false}, []bool{true, false, true, false, true, false, false})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorDecimal(t *testing.T) {
	testEqualsOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testEqualsOperatorDecimal(t, 20, 3, 20, 3)
	testEqualsOperatorDecimal(t, 30, 6, 20, 3)
}

func testEqualsOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1000.123", "1001.123", "1002.123", "1003.123", "1004.123", "1005.123", "1006.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1000.123", "2001.123", "2002.123", "1003.123", "2004.123", "2005.123", "2006.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{true, false, false, true, false, false, false})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false}, []string{"a", "a", "b", "a", "b", "a", "b"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false}, []string{"a", "b", "a", "b", "a", "b", "b"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorBytes(t *testing.T) {

	col1 := createBytesCol([]bool{false, false, true, false, false, true, false}, []string{"a", "a", "b", "a", "b", "a", "b"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false}, []string{"a", "b", "a", "b", "a", "b", "b"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestEqualsOperatorDifferentTypes(t *testing.T) {
	testEqualsOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testEqualsOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testEqualsOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testEqualsOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestNotEqualsOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, false, true, false, false, true, false}, []bool{true, true, false, true, false, true, false})
	col2 := createBoolCol([]bool{false, false, true, false, true, false, false}, []bool{true, false, true, false, true, false, false})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorDecimal(t *testing.T) {
	testNotEqualsOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testNotEqualsOperatorDecimal(t, 20, 3, 20, 3)
	testNotEqualsOperatorDecimal(t, 30, 6, 20, 3)
}

func testNotEqualsOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1000.123", "1001.123", "1002.123", "1003.123", "1004.123", "1005.123", "1006.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1000.123", "2001.123", "2002.123", "1003.123", "2004.123", "2005.123", "2006.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{false, true, false, false, false, false, true})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false}, []string{"a", "a", "b", "a", "b", "a", "b"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false}, []string{"a", "b", "a", "b", "a", "b", "b"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorBytes(t *testing.T) {

	col1 := createBytesCol([]bool{false, false, true, false, false, true, false}, []string{"a", "a", "b", "a", "b", "a", "b"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false}, []string{"a", "b", "a", "b", "a", "b", "b"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 2, 3, 4, 5, 6, 7})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewNotEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestNotEqualsOperatorDifferentTypes(t *testing.T) {
	testNotEqualsOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testNotEqualsOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testNotEqualsOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testNotEqualsOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestGreaterThanOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 5.1, 3.1, 4.1, 5.1, 6.1, 8.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorDecimal(t *testing.T) {
	testGreaterThanOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testGreaterThanOperatorDecimal(t, 20, 3, 20, 3)
	testGreaterThanOperatorDecimal(t, 30, 6, 20, 3)
}

func testGreaterThanOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1001.123", "1005.123", "1003.123", "1004.123", "1005.123", "1006.123", "1008.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1001.123", "1003.123", "1004.123", "1005.123", "1006.123", "1007.123", "1007.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{false, true, false, false, false, false, true})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{true, false, false, false, false, false, true, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorBytes(t *testing.T) {
	col1 := createBytesCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{true, false, false, false, false, false, true, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterThanOperatorUnsupportedTypes(t *testing.T) {
	testGreaterThanOperatorUnsupportedTypes(t, types.ColumnTypeBool)
}

func testGreaterThanOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestGreaterThanOperatorDifferentTypes(t *testing.T) {
	testGreaterThanOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testGreaterThanOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testGreaterThanOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testGreaterThanOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewGreaterThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestGreaterOrEqualsOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 5.1, 3.1, 4.1, 5.1, 6.1, 8.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorDecimal(t *testing.T) {
	testGreaterOrEqualsOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testGreaterOrEqualsOperatorDecimal(t, 20, 3, 20, 3)
	testGreaterOrEqualsOperatorDecimal(t, 30, 6, 20, 3)
}

func testGreaterOrEqualsOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1001.123", "1005.123", "1003.123", "1004.123", "1005.123", "1006.123", "1008.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1001.123", "1003.123", "1004.123", "1005.123", "1006.123", "1007.123", "1007.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{true, true, false, false, false, false, true})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{true, false, false, false, false, false, true, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorBytes(t *testing.T) {
	col1 := createBytesCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{true, false, false, false, false, false, true, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, true, false, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestGreaterOrEqualsOperatorUnsupportedTypes(t *testing.T) {
	testGreaterOrEqualsOperatorUnsupportedTypes(t, types.ColumnTypeBool)
}

func testGreaterOrEqualsOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestGreaterOrEqualsOperatorDifferentTypes(t *testing.T) {
	testGreaterOrEqualsOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testGreaterOrEqualsOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testGreaterOrEqualsOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testGreaterOrEqualsOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewGreaterOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLessThanOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 5.1, 3.1, 4.1, 5.1, 6.1, 8.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorDecimal(t *testing.T) {
	testLessThanOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testLessThanOperatorDecimal(t, 20, 3, 20, 3)
	testLessThanOperatorDecimal(t, 30, 6, 20, 3)
}

func testLessThanOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1001.123", "1005.123", "1003.123", "1004.123", "1005.123", "1006.123", "1008.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1001.123", "1003.123", "1004.123", "1005.123", "1006.123", "1007.123", "1007.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{false, false, false, true, false, false, false})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{false, true, false, true, false, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorBytes(t *testing.T) {
	col1 := createBytesCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{false, true, false, true, false, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{false, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessThanOperatorUnsupportedTypes(t *testing.T) {
	testLessThanOperatorUnsupportedTypes(t, types.ColumnTypeBool)
}

func testLessThanOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLessThanOperatorDifferentTypes(t *testing.T) {
	testLessThanOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testLessThanOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testLessThanOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testLessThanOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewLessThanOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLessOrEqualsOperatorInt(t *testing.T) {

	col1 := createIntCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createIntCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeInt}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeInt}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorFloat(t *testing.T) {

	col1 := createFloatCol([]bool{false, false, true, false, false, true, false}, []float64{1.1, 5.1, 3.1, 4.1, 5.1, 6.1, 8.1})
	col2 := createFloatCol([]bool{false, false, true, false, true, false, false}, []float64{1.1, 3.1, 4.1, 5.1, 6.1, 7.1, 7.1})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeFloat}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeFloat}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeFloat})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorDecimal(t *testing.T) {
	testLessOrEqualsOperatorDecimal(t, types.DefaultDecimalPrecision, types.DefaultDecimalScale, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	testLessOrEqualsOperatorDecimal(t, 20, 3, 20, 3)
	testLessOrEqualsOperatorDecimal(t, 30, 6, 20, 3)
}

func testLessOrEqualsOperatorDecimal(t *testing.T, prec1 int, scale1 int, prec2 int, scale2 int) {

	col1 := createDecimalCol(prec1, scale1, []bool{false, false, true, false, false, true, false},
		[]string{"1001.123", "1005.123", "1003.123", "1004.123", "1005.123", "1006.123", "1008.123"})
	col2 := createDecimalCol(prec2, scale2, []bool{false, false, true, false, true, false, false},
		[]string{"1001.123", "1003.123", "1004.123", "1005.123", "1006.123", "1007.123", "1007.123"})

	expected := createBoolCol([]bool{false, false, true, false, true, true, false},
		[]bool{true, false, false, true, false, false, false})

	decType1 := &types.DecimalType{
		Precision: prec1,
		Scale:     scale1,
	}
	decType2 := &types.DecimalType{
		Precision: prec2,
		Scale:     scale2,
	}

	left := &ColumnExpr{colIndex: 0, exprType: decType1}
	right := &ColumnExpr{colIndex: 1, exprType: decType2}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{decType1, decType2})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorString(t *testing.T) {

	col1 := createStringCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createStringCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{false, true, false, true, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeString}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeString}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorBytes(t *testing.T) {
	col1 := createBytesCol([]bool{false, false, true, false, false, true, false, false}, []string{"z", "a", "z", "a", "a", "a", "apples", "zz"})
	col2 := createBytesCol([]bool{false, false, true, false, true, false, false, false}, []string{"a", "z", "a", "apples", "z", "z", "a", "zz"})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false, false}, []bool{false, true, false, true, false, false, false, true})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBytes}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBytes}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorTimestamp(t *testing.T) {

	col1 := createTimestampCol([]bool{false, false, true, false, false, true, false}, []int64{1, 5, 3, 4, 5, 6, 8})
	col2 := createTimestampCol([]bool{false, false, true, false, true, false, false}, []int64{1, 3, 4, 5, 6, 7, 7})
	expected := createBoolCol([]bool{false, false, true, false, true, true, false}, []bool{true, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeTimestamp}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeTimestamp}

	oper, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLessOrEqualsOperatorUnsupportedTypes(t *testing.T) {
	testLessOrEqualsOperatorUnsupportedTypes(t, types.ColumnTypeBool)
}

func testLessOrEqualsOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLessOrEqualsOperatorDifferentTypes(t *testing.T) {
	testLessOrEqualsOperatorDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testLessOrEqualsOperatorDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testLessOrEqualsOperatorDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testLessOrEqualsOperatorDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewLessOrEqualsOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLogicalAndOperatorBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, false, false, false, true, false, true}, []bool{false, false, true, true, true, false, true})
	col2 := createBoolCol([]bool{false, false, false, false, false, true, true}, []bool{false, true, false, true, true, true, false})
	expected := createBoolCol([]bool{false, false, false, false, true, true, true}, []bool{false, false, false, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}

	oper, err := NewLogicalAndOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLogicalAndOperatorUnsupportedTypes(t *testing.T) {
	testLogicalAndOperatorUnsupportedTypes(t, types.ColumnTypeInt)
	testLogicalAndOperatorUnsupportedTypes(t, types.ColumnTypeFloat)
	testLogicalAndOperatorUnsupportedTypes(t, &types.DecimalType{Precision: types.DefaultDecimalPrecision, Scale: types.DefaultDecimalScale})
	testLogicalAndOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testLogicalAndOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
	testLogicalAndOperatorUnsupportedTypes(t, types.ColumnTypeTimestamp)
}

func testLogicalAndOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewLogicalAndOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLogicalAndOperatorUnsupportedTypesDifferentTypes(t *testing.T) {
	testLogicalAndOperatorUnsupportedTypesDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testLogicalAndOperatorUnsupportedTypesDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testLogicalAndOperatorUnsupportedTypesDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testLogicalAndOperatorUnsupportedTypesDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewLogicalAndOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLogicalOrOperatorBool(t *testing.T) {

	col1 := createBoolCol([]bool{false, false, false, false, true, false, true}, []bool{false, false, true, true, true, false, true})
	col2 := createBoolCol([]bool{false, false, false, false, false, true, true}, []bool{false, true, false, true, true, true, false})
	expected := createBoolCol([]bool{false, false, false, false, true, true, true}, []bool{false, true, true, true, false, false, false})

	left := &ColumnExpr{colIndex: 0, exprType: types.ColumnTypeBool}
	right := &ColumnExpr{colIndex: 1, exprType: types.ColumnTypeBool}

	oper, err := NewLogicalOrOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.NoError(t, err)

	schema := evbatch.NewEventSchema([]string{"col1", "col2"}, []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeBool})
	batch := evbatch.NewBatch(schema, col1, col2)

	res, err := EvalColumn(oper, batch)
	require.NoError(t, err)

	colsEqual(t, expected, res)
}

func TestLogicalOrOperatorUnsupportedTypes(t *testing.T) {
	testLogicalOrOperatorUnsupportedTypes(t, types.ColumnTypeInt)
	testLogicalOrOperatorUnsupportedTypes(t, types.ColumnTypeFloat)
	testLogicalOrOperatorUnsupportedTypes(t, &types.DecimalType{Precision: types.DefaultDecimalPrecision, Scale: types.DefaultDecimalScale})
	testLogicalOrOperatorUnsupportedTypes(t, types.ColumnTypeString)
	testLogicalOrOperatorUnsupportedTypes(t, types.ColumnTypeBytes)
	testLogicalOrOperatorUnsupportedTypes(t, types.ColumnTypeTimestamp)
}

func testLogicalOrOperatorUnsupportedTypes(t *testing.T, ct types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct}
	right := &ColumnExpr{colIndex: 0, exprType: ct}
	_, err := NewLogicalOrOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}

func TestLogicalOrOperatorUnsupportedTypesDifferentTypes(t *testing.T) {
	testLogicalOrOperatorUnsupportedTypesDifferentTypes(t, types.ColumnTypeInt, types.ColumnTypeFloat)
	testLogicalOrOperatorUnsupportedTypesDifferentTypes(t, types.ColumnTypeTimestamp, types.ColumnTypeFloat)
	testLogicalOrOperatorUnsupportedTypesDifferentTypes(t, &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}, types.ColumnTypeFloat)
}

func testLogicalOrOperatorUnsupportedTypesDifferentTypes(t *testing.T, ct1 types.ColumnType, ct2 types.ColumnType) {
	left := &ColumnExpr{colIndex: 1, exprType: ct1}
	right := &ColumnExpr{colIndex: 0, exprType: ct2}
	_, err := NewLogicalOrOperator(left, right, &parser.BinaryOperatorExprDesc{})
	require.True(t, common.IsTektiteErrorWithCode(err, errors.StatementError))
	require.Error(t, err)
}
