package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestSortManyBatches(t *testing.T) {
	partitionBatches := map[int][]*evbatch.Batch{}
	numPartitions := 10
	numBatchesPerPartition := 5
	numRowsPerBatch := 50
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	columnNames := []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}

	totRows := numPartitions * numBatchesPerPartition * numRowsPerBatch
	keys := make([]int, totRows)
	var allRows [][]any
	// generate some rows and shuffle them
	for n := range keys {
		keys[n] = n
		row := []any{int64(n), float64(n) + 1.1, n%2 == 0, createDecimal(t, fmt.Sprintf("%d", n*1000)),
			fmt.Sprintf("foo%d", n), []byte(fmt.Sprintf("foo%d", n)), types.NewTimestamp(int64(n*1000 + 234))}
		allRows = append(allRows, row)
	}
	shuffledRows := make([][]any, totRows)
	copy(shuffledRows, allRows)
	rand.Shuffle(totRows, func(i, j int) {
		shuffledRows[i], shuffledRows[j] = shuffledRows[j], shuffledRows[i]
	})
	rc := 0
	// separate the shuffled rows into multiple batches per partition
	for pid := 0; pid < numPartitions; pid++ {
		partBatches := make([]*evbatch.Batch, numBatchesPerPartition)
		for i := 0; i < numBatchesPerPartition; i++ {
			var batchRows [][]any
			for j := 0; j < numRowsPerBatch; j++ {
				row := shuffledRows[rc]
				batchRows = append(batchRows, row)
				rc++
			}
			evBatch := createEventBatch(columnNames, columnTypes, batchRows)
			partBatches[i] = evBatch
		}
		partitionBatches[pid] = partBatches
	}

	evSchema := evbatch.NewEventSchema(columnNames, columnTypes)
	opSchema := &OperatorSchema{
		EventSchema:     evSchema,
		PartitionScheme: PartitionScheme{Partitions: numPartitions},
	}

	exprs, err := toExprs("f0")
	require.NoError(t, err)

	so, err := NewSortOperator(opSchema, numPartitions, exprs, true, &expr.ExpressionFactory{})
	require.NoError(t, err)

	sortState := &SortState{}
	for i := 0; i < numBatchesPerPartition; i++ {
		for j := 0; j < numPartitions; j++ {
			batch := partitionBatches[j][i]
			last := i == numBatchesPerPartition-1
			ctx := &testQueryExecCtx{
				execID:        "test_exec_id",
				resultAddress: "test_result_address",
				last:          last,
				execState:     sortState,
			}
			b, err := so.HandleQueryBatch(batch, ctx)
			require.NoError(t, err)
			if j == numPartitions-1 && i == numBatchesPerPartition-1 {
				require.NotNil(t, b)
				actualOut := convertBatchToAnyArray(b)
				require.Equal(t, allRows, actualOut)
			} else {
				require.Nil(t, b)
			}
		}
	}
}

func TestSortWithExpressionAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{int64(-2), "x1"},
			{int64(4), "x2"},
			{int64(-1), "x3"},
			{nil, "x4"},
			{int64(5), "x5"},
			{int64(-3), "x6"},
			{int64(4), "x7"},
			{int64(1), "x8"},
			{int64(2), "x9"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(-1), "x12"},
			{nil, "x13"},
			{int64(0), "x14"},
			{int64(3), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{int64(0), "x14"},
			{int64(-1), "x3"},
			{int64(1), "x8"},
			{int64(-1), "x12"},
			{int64(-2), "x1"},
			{int64(2), "x9"},
			{int64(-3), "x6"},
			{int64(3), "x15"},
			{int64(4), "x2"},
			{int64(4), "x7"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(5), "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}

	testSort(t, []string{"abs(k)"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"abs(k) asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"abs(k) ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortWithExpressionDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{int64(-2), "x1"},
			{int64(4), "x2"},
			{int64(-1), "x3"},
			{nil, "x4"},
			{int64(5), "x5"},
			{int64(-3), "x6"},
			{int64(4), "x7"},
			{int64(1), "x8"},
			{int64(2), "x9"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(-1), "x12"},
			{nil, "x13"},
			{int64(0), "x14"},
			{int64(3), "x15"},
		}
	expectedOut :=
		[][]any{
			{int64(5), "x5"},
			{int64(4), "x2"},
			{int64(4), "x7"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(-3), "x6"},
			{int64(3), "x15"},
			{int64(-2), "x1"},
			{int64(2), "x9"},
			{int64(-1), "x3"},
			{int64(1), "x8"},
			{int64(-1), "x12"},
			{int64(0), "x14"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}

	testSort(t, []string{"abs(k) desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"abs(k) descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleIntDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{int64(-2), "x1"},
			{int64(4), "x2"},
			{int64(-1), "x3"},
			{nil, "x4"},
			{int64(5), "x5"},
			{int64(-3), "x6"},
			{int64(4), "x7"},
			{int64(1), "x8"},
			{int64(2), "x9"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(-1), "x12"},
			{nil, "x13"},
			{int64(0), "x14"},
			{int64(3), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{int64(-4), "x10"},
			{int64(-3), "x6"},
			{int64(-2), "x1"},
			{int64(-1), "x3"},
			{int64(-1), "x12"},
			{int64(0), "x14"},
			{int64(1), "x8"},
			{int64(2), "x9"},
			{int64(3), "x15"},
			{int64(4), "x2"},
			{int64(4), "x7"},
			{int64(4), "x11"},
			{int64(5), "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}

	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleIntDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{int64(-2), "x1"},
			{int64(4), "x2"},
			{int64(-1), "x3"},
			{nil, "x4"},
			{int64(5), "x5"},
			{int64(-3), "x6"},
			{int64(4), "x7"},
			{int64(1), "x8"},
			{int64(2), "x9"},
			{int64(-4), "x10"},
			{int64(4), "x11"},
			{int64(-1), "x12"},
			{nil, "x13"},
			{int64(0), "x14"},
			{int64(3), "x15"},
		}
	expectedOut :=
		[][]any{
			{int64(5), "x5"},
			{int64(4), "x2"},
			{int64(4), "x7"},
			{int64(4), "x11"},
			{int64(3), "x15"},
			{int64(2), "x9"},
			{int64(1), "x8"},
			{int64(0), "x14"},
			{int64(-1), "x3"},
			{int64(-1), "x12"},
			{int64(-2), "x1"},
			{int64(-3), "x6"},
			{int64(-4), "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleFloatDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{float64(-2.1), "x1"},
			{float64(4.1), "x2"},
			{float64(-1.1), "x3"},
			{nil, "x4"},
			{float64(5.1), "x5"},
			{float64(-3.1), "x6"},
			{float64(4.1), "x7"},
			{float64(1.1), "x8"},
			{float64(2.1), "x9"},
			{float64(-4.1), "x10"},
			{float64(4.1), "x11"},
			{float64(-1.1), "x12"},
			{nil, "x13"},
			{float64(0), "x14"},
			{float64(3.1), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{float64(-4.1), "x10"},
			{float64(-3.1), "x6"},
			{float64(-2.1), "x1"},
			{float64(-1.1), "x3"},
			{float64(-1.1), "x12"},
			{float64(0), "x14"},
			{float64(1.1), "x8"},
			{float64(2.1), "x9"},
			{float64(3.1), "x15"},
			{float64(4.1), "x2"},
			{float64(4.1), "x7"},
			{float64(4.1), "x11"},
			{float64(5.1), "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleFloatDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{float64(-2.1), "x1"},
			{float64(4.1), "x2"},
			{float64(-1.1), "x3"},
			{nil, "x4"},
			{float64(5.1), "x5"},
			{float64(-3.1), "x6"},
			{float64(4.1), "x7"},
			{float64(1.1), "x8"},
			{float64(2.1), "x9"},
			{float64(-4.1), "x10"},
			{float64(4.1), "x11"},
			{float64(-1.1), "x12"},
			{nil, "x13"},
			{float64(0), "x14"},
			{float64(3.1), "x15"},
		}
	expectedOut :=
		[][]any{
			{float64(5.1), "x5"},
			{float64(4.1), "x2"},
			{float64(4.1), "x7"},
			{float64(4.1), "x11"},
			{float64(3.1), "x15"},
			{float64(2.1), "x9"},
			{float64(1.1), "x8"},
			{float64(0), "x14"},
			{float64(-1.1), "x3"},
			{float64(-1.1), "x12"},
			{float64(-2.1), "x1"},
			{float64(-3.1), "x6"},
			{float64(-4.1), "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}

	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleBoolDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{true, "x1"},
			{nil, "x2"},
			{false, "x3"},
			{false, "x4"},
			{true, "x5"},
			{nil, "x6"},
			{false, "x7"},
		}
	expectedOut :=
		[][]any{
			{nil, "x2"},
			{nil, "x6"},
			{false, "x3"},
			{false, "x4"},
			{false, "x7"},
			{true, "x1"},
			{true, "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleBoolDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{true, "x1"},
			{nil, "x2"},
			{false, "x3"},
			{false, "x4"},
			{true, "x5"},
			{nil, "x6"},
			{false, "x7"},
		}
	expectedOut :=
		[][]any{
			{true, "x1"},
			{true, "x5"},
			{false, "x3"},
			{false, "x4"},
			{false, "x7"},
			{nil, "x2"},
			{nil, "x6"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleDecimalDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{createDecimal(t, "-2000.1234"), "x1"},
			{createDecimal(t, "4000.1234"), "x2"},
			{createDecimal(t, "-1000.1234"), "x3"},
			{nil, "x4"},
			{createDecimal(t, "5000.1234"), "x5"},
			{createDecimal(t, "-3000.1234"), "x6"},
			{createDecimal(t, "4000.1234"), "x7"},
			{createDecimal(t, "1000.1234"), "x8"},
			{createDecimal(t, "2000.1234"), "x9"},
			{createDecimal(t, "-4000.1234"), "x10"},
			{createDecimal(t, "4000.1234"), "x11"},
			{createDecimal(t, "-1000.1234"), "x12"},
			{nil, "x13"},
			{createDecimal(t, "0"), "x14"},
			{createDecimal(t, "3000.1234"), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{createDecimal(t, "-4000.1234"), "x10"},
			{createDecimal(t, "-3000.1234"), "x6"},
			{createDecimal(t, "-2000.1234"), "x1"},
			{createDecimal(t, "-1000.1234"), "x3"},
			{createDecimal(t, "-1000.1234"), "x12"},
			{createDecimal(t, "0"), "x14"},
			{createDecimal(t, "1000.1234"), "x8"},
			{createDecimal(t, "2000.1234"), "x9"},
			{createDecimal(t, "3000.1234"), "x15"},
			{createDecimal(t, "4000.1234"), "x2"},
			{createDecimal(t, "4000.1234"), "x7"},
			{createDecimal(t, "4000.1234"), "x11"},
			{createDecimal(t, "5000.1234"), "x5"},
		}
	columnNames := []string{"k", "v"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	columnTypes := []types.ColumnType{decType, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleDecimalDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{createDecimal(t, "-2000.1234"), "x1"},
			{createDecimal(t, "4000.1234"), "x2"},
			{createDecimal(t, "-1000.1234"), "x3"},
			{nil, "x4"},
			{createDecimal(t, "5000.1234"), "x5"},
			{createDecimal(t, "-3000.1234"), "x6"},
			{createDecimal(t, "4000.1234"), "x7"},
			{createDecimal(t, "1000.1234"), "x8"},
			{createDecimal(t, "2000.1234"), "x9"},
			{createDecimal(t, "-4000.1234"), "x10"},
			{createDecimal(t, "4000.1234"), "x11"},
			{createDecimal(t, "-1000.1234"), "x12"},
			{nil, "x13"},
			{createDecimal(t, "0"), "x14"},
			{createDecimal(t, "3000.1234"), "x15"},
		}
	expectedOut :=
		[][]any{
			{createDecimal(t, "5000.1234"), "x5"},
			{createDecimal(t, "4000.1234"), "x2"},
			{createDecimal(t, "4000.1234"), "x7"},
			{createDecimal(t, "4000.1234"), "x11"},
			{createDecimal(t, "3000.1234"), "x15"},
			{createDecimal(t, "2000.1234"), "x9"},
			{createDecimal(t, "1000.1234"), "x8"},
			{createDecimal(t, "0"), "x14"},
			{createDecimal(t, "-1000.1234"), "x3"},
			{createDecimal(t, "-1000.1234"), "x12"},
			{createDecimal(t, "-2000.1234"), "x1"},
			{createDecimal(t, "-3000.1234"), "x6"},
			{createDecimal(t, "-4000.1234"), "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	columnTypes := []types.ColumnType{decType, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleStringDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{"foo03", "x1"},
			{"foo09", "x2"},
			{"foo04", "x3"},
			{nil, "x4"},
			{"foo10", "x5"},
			{"foo02", "x6"},
			{"foo09", "x7"},
			{"foo06", "x8"},
			{"foo07", "x9"},
			{"foo01", "x10"},
			{"foo09", "x11"},
			{"foo04", "x12"},
			{nil, "x13"},
			{"foo05", "x14"},
			{"foo08", "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{"foo01", "x10"},
			{"foo02", "x6"},
			{"foo03", "x1"},
			{"foo04", "x3"},
			{"foo04", "x12"},
			{"foo05", "x14"},
			{"foo06", "x8"},
			{"foo07", "x9"},
			{"foo08", "x15"},
			{"foo09", "x2"},
			{"foo09", "x7"},
			{"foo09", "x11"},
			{"foo10", "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleStringDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{"foo03", "x1"},
			{"foo09", "x2"},
			{"foo04", "x3"},
			{nil, "x4"},
			{"foo10", "x5"},
			{"foo02", "x6"},
			{"foo09", "x7"},
			{"foo06", "x8"},
			{"foo07", "x9"},
			{"foo01", "x10"},
			{"foo09", "x11"},
			{"foo04", "x12"},
			{nil, "x13"},
			{"foo05", "x14"},
			{"foo08", "x15"},
		}
	expectedOut :=
		[][]any{
			{"foo10", "x5"},
			{"foo09", "x2"},
			{"foo09", "x7"},
			{"foo09", "x11"},
			{"foo08", "x15"},
			{"foo07", "x9"},
			{"foo06", "x8"},
			{"foo05", "x14"},
			{"foo04", "x3"},
			{"foo04", "x12"},
			{"foo03", "x1"},
			{"foo02", "x6"},
			{"foo01", "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleBytesDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{[]byte("foo03"), "x1"},
			{[]byte("foo09"), "x2"},
			{[]byte("foo04"), "x3"},
			{nil, "x4"},
			{[]byte("foo10"), "x5"},
			{[]byte("foo02"), "x6"},
			{[]byte("foo09"), "x7"},
			{[]byte("foo06"), "x8"},
			{[]byte("foo07"), "x9"},
			{[]byte("foo01"), "x10"},
			{[]byte("foo09"), "x11"},
			{[]byte("foo04"), "x12"},
			{nil, "x13"},
			{[]byte("foo05"), "x14"},
			{[]byte("foo08"), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{[]byte("foo01"), "x10"},
			{[]byte("foo02"), "x6"},
			{[]byte("foo03"), "x1"},
			{[]byte("foo04"), "x3"},
			{[]byte("foo04"), "x12"},
			{[]byte("foo05"), "x14"},
			{[]byte("foo06"), "x8"},
			{[]byte("foo07"), "x9"},
			{[]byte("foo08"), "x15"},
			{[]byte("foo09"), "x2"},
			{[]byte("foo09"), "x7"},
			{[]byte("foo09"), "x11"},
			{[]byte("foo10"), "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleBytesDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{[]byte("foo03"), "x1"},
			{[]byte("foo09"), "x2"},
			{[]byte("foo04"), "x3"},
			{nil, "x4"},
			{[]byte("foo10"), "x5"},
			{[]byte("foo02"), "x6"},
			{[]byte("foo09"), "x7"},
			{[]byte("foo06"), "x8"},
			{[]byte("foo07"), "x9"},
			{[]byte("foo01"), "x10"},
			{[]byte("foo09"), "x11"},
			{[]byte("foo04"), "x12"},
			{nil, "x13"},
			{[]byte("foo05"), "x14"},
			{[]byte("foo08"), "x15"},
		}
	expectedOut :=
		[][]any{
			{[]byte("foo10"), "x5"},
			{[]byte("foo09"), "x2"},
			{[]byte("foo09"), "x7"},
			{[]byte("foo09"), "x11"},
			{[]byte("foo08"), "x15"},
			{[]byte("foo07"), "x9"},
			{[]byte("foo06"), "x8"},
			{[]byte("foo05"), "x14"},
			{[]byte("foo04"), "x3"},
			{[]byte("foo04"), "x12"},
			{[]byte("foo03"), "x1"},
			{[]byte("foo02"), "x6"},
			{[]byte("foo01"), "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleTimestampDirectExprAsc(t *testing.T) {
	dataIn :=
		[][]any{
			{types.NewTimestamp(3000), "x1"},
			{types.NewTimestamp(9000), "x2"},
			{types.NewTimestamp(4000), "x3"},
			{nil, "x4"},
			{types.NewTimestamp(10000), "x5"},
			{types.NewTimestamp(2000), "x6"},
			{types.NewTimestamp(9000), "x7"},
			{types.NewTimestamp(6000), "x8"},
			{types.NewTimestamp(7000), "x9"},
			{types.NewTimestamp(1000), "x10"},
			{types.NewTimestamp(9000), "x11"},
			{types.NewTimestamp(4000), "x12"},
			{nil, "x13"},
			{types.NewTimestamp(5000), "x14"},
			{types.NewTimestamp(8000), "x15"},
		}
	expectedOut :=
		[][]any{
			{nil, "x4"},
			{nil, "x13"},
			{types.NewTimestamp(1000), "x10"},
			{types.NewTimestamp(2000), "x6"},
			{types.NewTimestamp(3000), "x1"},
			{types.NewTimestamp(4000), "x3"},
			{types.NewTimestamp(4000), "x12"},
			{types.NewTimestamp(5000), "x14"},
			{types.NewTimestamp(6000), "x8"},
			{types.NewTimestamp(7000), "x9"},
			{types.NewTimestamp(8000), "x15"},
			{types.NewTimestamp(9000), "x2"},
			{types.NewTimestamp(9000), "x7"},
			{types.NewTimestamp(9000), "x11"},
			{types.NewTimestamp(10000), "x5"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString}
	testSort(t, []string{"k"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k asc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k ascending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortSingleTimestampDirectExprDesc(t *testing.T) {
	dataIn :=
		[][]any{
			{types.NewTimestamp(3000), "x1"},
			{types.NewTimestamp(9000), "x2"},
			{types.NewTimestamp(4000), "x3"},
			{nil, "x4"},
			{types.NewTimestamp(10000), "x5"},
			{types.NewTimestamp(2000), "x6"},
			{types.NewTimestamp(9000), "x7"},
			{types.NewTimestamp(6000), "x8"},
			{types.NewTimestamp(7000), "x9"},
			{types.NewTimestamp(1000), "x10"},
			{types.NewTimestamp(9000), "x11"},
			{types.NewTimestamp(4000), "x12"},
			{nil, "x13"},
			{types.NewTimestamp(5000), "x14"},
			{types.NewTimestamp(8000), "x15"},
		}
	expectedOut :=
		[][]any{
			{types.NewTimestamp(10000), "x5"},
			{types.NewTimestamp(9000), "x2"},
			{types.NewTimestamp(9000), "x7"},
			{types.NewTimestamp(9000), "x11"},
			{types.NewTimestamp(8000), "x15"},
			{types.NewTimestamp(7000), "x9"},
			{types.NewTimestamp(6000), "x8"},
			{types.NewTimestamp(5000), "x14"},
			{types.NewTimestamp(4000), "x3"},
			{types.NewTimestamp(4000), "x12"},
			{types.NewTimestamp(3000), "x1"},
			{types.NewTimestamp(2000), "x6"},
			{types.NewTimestamp(1000), "x10"},
			{nil, "x4"},
			{nil, "x13"},
		}
	columnNames := []string{"k", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString}
	testSort(t, []string{"k desc"}, dataIn, expectedOut, columnNames, columnTypes)
	testSort(t, []string{"k descending"}, dataIn, expectedOut, columnNames, columnTypes)
}

func TestSortMultipleColumns(t *testing.T) {
	dataIn :=
		[][]any{
			{int64(1), 1.1, "a", "x1"},
			{nil, 2.1, nil, "x2"},
			{nil, nil, "a", "x3"},
			{int64(2), nil, nil, "x4"},
			{int64(1), 2.1, "a", "x5"},
			{int64(2), nil, "b", "x6"},
			{int64(2), 2.1, "a", "x7"},
			{int64(2), 1.1, nil, "x8"},
			{nil, 1.1, "a", "x9"},
			{nil, 2.1, "a", "x10"},
			{nil, nil, nil, "x11"},
			{int64(1), 2.1, nil, "x12"},
			{int64(1), nil, "b", "x13"},
			{int64(2), 2.1, nil, "x14"},
			{int64(2), nil, "a", "x15"},
			{int64(1), nil, "a", "x16"},
			{nil, nil, "b", "x17"},
			{int64(1), 1.1, nil, "x18"},
			{int64(2), 1.1, "b", "x19"},
			{int64(1), 1.1, "b", "x20"},
			{int64(1), 2.1, "b", "x21"},
			{nil, 1.1, "b", "x22"},
			{int64(1), nil, nil, "x23"},
			{int64(2), 2.1, "b", "x24"},
			{int64(2), 1.1, "a", "x25"},
			{nil, 2.1, "b", "x26"},
			{nil, 1.1, nil, "x17"},
		}
	expectedOut :=
		[][]any{
			{nil, nil, nil, "x11"},
			{nil, nil, "a", "x3"},
			{nil, nil, "b", "x17"},

			{nil, 1.1, nil, "x17"},
			{nil, 1.1, "a", "x9"},
			{nil, 1.1, "b", "x22"},

			{nil, 2.1, nil, "x2"},
			{nil, 2.1, "a", "x10"},
			{nil, 2.1, "b", "x26"},

			{int64(1), nil, nil, "x23"},
			{int64(1), nil, "a", "x16"},
			{int64(1), nil, "b", "x13"},

			{int64(1), 1.1, nil, "x18"},
			{int64(1), 1.1, "a", "x1"},
			{int64(1), 1.1, "b", "x20"},

			{int64(1), 2.1, nil, "x12"},
			{int64(1), 2.1, "a", "x5"},
			{int64(1), 2.1, "b", "x21"},

			{int64(2), nil, nil, "x4"},
			{int64(2), nil, "a", "x15"},
			{int64(2), nil, "b", "x6"},

			{int64(2), 1.1, nil, "x8"},
			{int64(2), 1.1, "a", "x25"},
			{int64(2), 1.1, "b", "x19"},

			{int64(2), 2.1, nil, "x14"},
			{int64(2), 2.1, "a", "x7"},
			{int64(2), 2.1, "b", "x24"},
		}
	exprs := []string{"k1", "k2", "k3"}
	columnNames := []string{"k1", "k2", "k3", "v"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString, types.ColumnTypeString}
	testSort(t, exprs, dataIn, expectedOut, columnNames, columnTypes)
}

func testSort(t *testing.T, sortExprs []string, dataIn [][]any, expectedOut [][]any, columnNames []string, columnTypes []types.ColumnType) {
	evSchema := evbatch.NewEventSchema(columnNames, columnTypes)
	opSchema := &OperatorSchema{
		EventSchema:     evSchema,
		PartitionScheme: PartitionScheme{Partitions: 25},
	}

	exprs, err := toExprs(sortExprs...)
	require.NoError(t, err)

	// We use stable sort in tests so that we get deterministic behaviour for equal values
	so, err := NewSortOperator(opSchema, 1, exprs, true, &expr.ExpressionFactory{})
	require.NoError(t, err)

	batchIn := createEventBatch(columnNames, columnTypes, dataIn)
	sortState := &SortState{}
	ctx := &testQueryExecCtx{
		execID:        "test_exec_id",
		resultAddress: "test_result_address",
		last:          true,
		execState:     sortState,
	}
	b, err := so.HandleQueryBatch(batchIn, ctx)
	require.NoError(t, err)

	require.Equal(t, len(dataIn), b.RowCount)
	actualOut := convertBatchToAnyArray(b)
	require.Equal(t, expectedOut, actualOut)
}

type testQueryExecCtx struct {
	execID        string
	resultAddress string
	last          bool
	execState     any
}

func (q *testQueryExecCtx) ExecID() string {
	return q.execID
}

func (q *testQueryExecCtx) ResultAddress() string {
	return q.resultAddress
}

func (q *testQueryExecCtx) Last() bool {
	return q.last
}

func (q *testQueryExecCtx) ExecState() any {
	return q.execState
}
