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

func TestFilterOperatorSingleRowAccepted(t *testing.T) {
	testFilterOperatorSingleRowAccepted(t, "true")
	testFilterOperatorSingleRowAccepted(t, "f0==1")
	testFilterOperatorSingleRowAccepted(t, "f1==1.23f")
	testFilterOperatorSingleRowAccepted(t, "f2")
	testFilterOperatorSingleRowAccepted(t, `f3==to_decimal("100.00",12,4)`)
	testFilterOperatorSingleRowAccepted(t, `starts_with(f4,"fo")`)
	testFilterOperatorSingleRowAccepted(t, "len(f5)==3")
	testFilterOperatorSingleRowAccepted(t, "to_int(f6)==1234")
	testFilterOperatorSingleRowAccepted(t, "f0==1 && f1>1.1f")
}

func TestFilterOperatorSingleRowRejected(t *testing.T) {
	testFilterOperatorSingleRowRejected(t, "false")
	testFilterOperatorSingleRowRejected(t, "f0==2")
	testFilterOperatorSingleRowRejected(t, "f1==2.23f")
	testFilterOperatorSingleRowRejected(t, "!f2")
	testFilterOperatorSingleRowRejected(t, `f3==to_decimal("101.00",12,4)`)
	testFilterOperatorSingleRowRejected(t, `starts_with(f4,"bo")`)
	testFilterOperatorSingleRowRejected(t, "len(f5)==4")
	testFilterOperatorSingleRowRejected(t, "to_int(f6)==2234")
	testFilterOperatorSingleRowRejected(t, "f0==1 && f1<1.1f")
}

func TestFilterOperatorMultipleRowsAllAccepted(t *testing.T) {
	var allRows []int
	for i := 0; i < 10; i++ {
		allRows = append(allRows, i)
	}
	testFilterOperatorMultipleRows(t, "true", allRows)
	testFilterOperatorMultipleRows(t, "f0 >= 0", allRows)
	testFilterOperatorMultipleRows(t, "f0 < 1000", allRows)
	testFilterOperatorMultipleRows(t, "f1 < 12f", allRows)
	testFilterOperatorMultipleRows(t, "f2 || !f2", allRows)
	testFilterOperatorMultipleRows(t, `to_string(f3) != "blah"`, allRows)
	testFilterOperatorMultipleRows(t, `starts_with(f4, "foo")`, allRows)
	testFilterOperatorMultipleRows(t, `len(f5) < 10`, allRows)
	testFilterOperatorMultipleRows(t, `to_int(f6) < 1010`, allRows)
}

func TestFilterOperatorMultipleRowsAllRejected(t *testing.T) {
	testFilterOperatorMultipleRows(t, "false", nil)
	testFilterOperatorMultipleRows(t, "f0 < 0", nil)
	testFilterOperatorMultipleRows(t, "f0 >= 1000", nil)
	testFilterOperatorMultipleRows(t, "f1 >= 12f", nil)
	testFilterOperatorMultipleRows(t, "f2 && !f2", nil)
	testFilterOperatorMultipleRows(t, `to_string(f3) == "blah"`, nil)
	testFilterOperatorMultipleRows(t, `starts_with(f4, "oop")`, nil)
	testFilterOperatorMultipleRows(t, `len(f5) > 10`, nil)
	testFilterOperatorMultipleRows(t, `to_int(f6) >= 1010`, nil)
}

func TestFilterOperatorSomeRowsAccepted(t *testing.T) {
	testFilterOperatorMultipleRows(t, "f0 > 2 && f0 < 8", []int{3, 4, 5, 6, 7})
	testFilterOperatorMultipleRows(t, "f0 <= 2 || f0 >= 8", []int{0, 1, 2, 8, 9})
	testFilterOperatorMultipleRows(t, "f2", []int{0, 2, 4, 6, 8})
}

func TestFilterOperatorMultipleRowsNoneAccepted(t *testing.T) {
	testFilterOperatorMultipleRows(t, "false", nil)
}

func testFilterOperatorSingleRowAccepted(t *testing.T, filterExpr string) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testFilterOperator(t, []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		filterExpr,
		[][]any{{int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 1000000), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		[][]any{{int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 1000000), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
	)
}

func testFilterOperatorSingleRowRejected(t *testing.T, filterExpr string) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	testFilterOperator(t, []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		filterExpr,
		[][]any{{int64(1), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 1000000), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}},
		nil,
	)
}

func testFilterOperatorMultipleRows(t *testing.T, filterExpr string, acceptedRows []int) {
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	numRows := 10
	var dataIn [][]any
	for i := 0; i < numRows; i++ {
		row := []any{
			int64(i), 1.1 + float64(i), i%2 == 0, types.Decimal{Num: decimal128.New(0, uint64(i*10000)), Precision: 12, Scale: 4}, fmt.Sprintf("foo%d", i),
			[]byte(fmt.Sprintf("bytes%d", i)), types.NewTimestamp(int64(i + 1000)),
		}
		dataIn = append(dataIn, row)
	}
	var dataOut [][]any
	for _, index := range acceptedRows {
		dataOut = append(dataOut, dataIn[index])
	}
	testFilterOperator(t, []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
			types.ColumnTypeBytes, types.ColumnTypeTimestamp},
		filterExpr,
		dataIn,
		dataOut,
	)
}

func testFilterOperator(t *testing.T, inColumnNames []string, inColumnTypes []types.ColumnType, filterExpr string, inData [][]any,
	expectedOutData [][]any) {
	inSchema := evbatch.NewEventSchema(inColumnNames, inColumnTypes)
	exprs, err := toExprs(filterExpr)
	require.NoError(t, err)
	fo, err := NewFilterOperator(&OperatorSchema{EventSchema: inSchema}, exprs[0], &expr.ExpressionFactory{})
	require.NoError(t, err)
	testOperator(t, fo, inColumnNames, inColumnTypes, inData, expectedOutData)
}
