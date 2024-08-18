package query

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/asl/conf"
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	defaultNumManagers   = 5
	defaultNumPartitions = 25
	defaultMaxBatchRows  = 100
	defaultSlabID        = 10
)

func TestGetPreparedQueryDeletion(t *testing.T) {
	keyCols := []int{0, 1, 2}
	columnNames := []string{"offset", "f1", "f2"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeFloat}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, _ := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(1, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	tsl := `prepare test_query1 := (scan $p1:int,$p2:string,$p3:float to end from test_slab1)`
	prepareQuery(t, tsl, ctx)
	tsl2 := `deletequery(test_query1)`
	deleteQuery(t, tsl2, ctx)
}

func TestGetPreparedQueryParamMeta(t *testing.T) {
	keyCols := []int{0, 1, 2}
	columnNames := []string{"offset", "f1", "f2"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeFloat}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, _ := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(1, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	tsl := `prepare test_query1 := (scan $p1:int,$p2:string,$p3:float to end from test_slab1)`
	paramNames := []string{"$p1:int", "$p2:string", "$p3:float"}
	paramTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeFloat}
	prepareQuery(t, tsl, ctx)
	mgr := ctx.qms[0].qm
	paramSchema, ok := mgr.GetPreparedQueryParamSchema("test_query1")
	require.NotNil(t, paramSchema)
	require.True(t, ok)
	require.Equal(t, paramNames, paramSchema.ColumnNames())
	require.Equal(t, paramTypes, paramSchema.ColumnTypes())
}

func TestFailToPrepareQueryWrongNumberofParams(t *testing.T) {
	keyCols := []int{0, 1, 2}
	columnNames := []string{"offset", "f1", "f2"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeFloat}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, _ := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(1, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	tsl := `prepare test_query1 := (get $p1:int,$p2:string,$p3:float,$p4:int from test_slab1)`

	ast, err := parser.NewParser(nil).ParseTSL(tsl)
	require.NoError(t, err)
	for _, pair := range ctx.qms {
		err = pair.qm.PrepareQuery(*ast.PrepareQuery)
		require.Error(t, err)
		require.Equal(t, "failed to prepare/execute query: number of key elements specified (4) is greater than number of columns in key (3)",
			err.Error())
	}
}

func TestFailToPrepareQueryWrongParamTypes(t *testing.T) {
	keyCols := []int{0, 1, 2}
	columnNames := []string{"offset", "f1", "f2"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeFloat}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, _ := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(1, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	tsl := `prepare test_query1 := (get $p1:timestamp,$p2:string,$p3:float from test_slab1)`
	ast, err := parser.NewParser(nil).ParseTSL(tsl)
	require.NoError(t, err)
	for _, pair := range ctx.qms {
		err = pair.qm.PrepareQuery(*ast.PrepareQuery)
		require.Error(t, err)
		require.Equal(t, `invalid type for param expression - it returns type timestamp but key column is of type int (line 1 column 29):
prepare test_query1 := (get $p1:timestamp,$p2:string,$p3:float from test_slab1)
                            ^`,
			err.Error())
	}
}

func TestQMGetAll(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", false, "val0"},
		{int64(0), "x0", true, "val1"},
		{int64(0), "x1", false, "val2"},
		{int64(0), "x1", true, "val3"},
		{int64(1), "x2", false, "val4"},
		{int64(1), "x2", true, "val5"},
		{int64(1), "x3", false, "val6"},
		{int64(1), "x3", true, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)
	tsl := `prepare test_query1 := (scan all from test_slab1)`
	prepareQuery(t, tsl, ctx)
	mgr := ctx.qms[rand.Intn(len(ctx.qms))].qm
	var totRows [][]any
	var lock sync.Mutex
	var done sync.WaitGroup
	done.Add(1)
	var lastBatchCount int
	numParts, err := mgr.ExecutePreparedQuery("test_query1", nil, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		rows := convertBatchToAnyArray(batch, schema)
		lock.Lock()
		defer lock.Unlock()
		totRows = append(totRows, rows...)
		if last {
			lastBatchCount++
			if lastBatchCount == numLastBatches {
				done.Done()
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, defaultNumPartitions, numParts)
	done.Wait()
	sortDataByKeyCols(totRows, keyCols, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool})
	require.Equal(t, data, totRows)
}

func TestMQGetRangeSimpleSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 12, "$s1:int to $e1:int", []any{int64(1), int64(3)})
}

func TestMQGetRangeSimpleDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 14, "$s1:int, $s2:string to $e1:int, $e2:string",
		[]any{int64(1), "x1", int64(3), "x1"})
}

func TestMQGetRangeSimpleTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 3, 6, "$s1:int, $s2:string, $s3:bool to $e1:int, $e2:string, $e3:bool",
		[]any{int64(0), "x1", true, int64(1), "x1", false})
}

func TestMQGetRangeRangesDifferentNumElementsTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 6, "$s1:int to $e1:int, $e2:string, $e3:bool",
		[]any{int64(1), int64(1), "x1", false})
}

func TestMQGetRangeFromStartSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 0, 12, "start to $e1:int", []any{int64(3)})
}

func TestMQGetRangeFromStartDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 0, 14, "start to $e1:int, $e2:string", []any{int64(3), "x1"})
}

func TestMQGetRangeFromStartTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 0, 6, "start to $e1:int, $e2:string, $e3:bool",
		[]any{int64(1), "x1", false})
}

func TestMQGetRangeToEndSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 16, "$s1:int to end", []any{int64(1)})
}

func TestMQGetRangeToEndDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 16, "$s1:int, $s2:string to end",
		[]any{int64(1), "x1"})
}

func TestMQGetRangeToEndTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 3, 16, "$s1:int, $s2:string, $s3:bool to end",
		[]any{int64(0), "x1", true})
}

func TestMQGetRangeStartToEnd(t *testing.T) {
	// same as a get all
	testQMGetRange(t, 0, 16, "start to end", nil)
}

func TestMQGetRangeStartInclusiveSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 12, "$s1:int incl to $e1:int", []any{int64(1), int64(3)})
}

func TestMQGetRangeStartInclusiveDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 14, "$s1:int, $s2:string incl to $e1:int, $e2:string",
		[]any{int64(1), "x1", int64(3), "x1"})
}

func TestMQGetRangeStartInclusiveTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 3, 6, "$s1:int, $s2:string, $s3:bool incl to $e1:int, $e2:string, $e3:bool",
		[]any{int64(0), "x1", true, int64(1), "x1", false})
}

func TestMQGetRangeStartExclusiveSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 8, 12, "$s1:int excl to $e1:int", []any{int64(1), int64(3)})
}

func TestMQGetRangeStartExclusiveDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 14, "$s1:int, $s2:string excl to $e1:int, $e2:string",
		[]any{int64(1), "x0", int64(3), "x1"})
}

func TestMQGetRangeStartExclusiveTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 6, "$s1:int, $s2:string, $s3:bool excl to $e1:int, $e2:string, $e3:bool",
		[]any{int64(0), "x1", true, int64(1), "x1", false})
}

func TestMQGetRangeEndInclusiveSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 16, "$s1:int to $e1:int incl", []any{int64(1), int64(3)})
}

func TestMQGetRangeEndInclusiveDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 16, "$s1:int, $s2:string to $e1:int, $e2:string incl",
		[]any{int64(1), "x1", int64(3), "x1"})
}

func TestMQGetRangeEndInclusiveTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 3, 7, "$s1:int, $s2:string, $s3:bool to $e1:int, $e2:string, $e3:bool incl",
		[]any{int64(0), "x1", true, int64(1), "x1", false})
}

func TestMQGetRangeEndExclusiveSingleKeyVal(t *testing.T) {
	testQMGetRange(t, 4, 12, "$s1:int to $e1:int excl", []any{int64(1), int64(3)})
}

func TestMQGetRangeEndExclusiveDoubleKeyVal(t *testing.T) {
	testQMGetRange(t, 6, 14, "$s1:int, $s2:string to $e1:int, $e2:string excl",
		[]any{int64(1), "x1", int64(3), "x1"})
}

func TestMQGetRangeEndExclusiveTripleKeyVal(t *testing.T) {
	testQMGetRange(t, 3, 6, "$s1:int, $s2:string, $s3:bool to $e1:int, $e2:string, $e3:bool excl",
		[]any{int64(0), "x1", true, int64(1), "x1", false})
}

func testQMGetRange(t *testing.T, expectedStart int, expectedEnd int, rangeStr string, argVals []any) {
	data := [][]any{
		{int64(0), "x0", false, "val0"},
		{int64(0), "x0", true, "val1"},
		{int64(0), "x1", false, "val2"},
		{int64(0), "x1", true, "val3"},
		{int64(1), "x0", false, "val4"},
		{int64(1), "x0", true, "val5"},
		{int64(1), "x1", false, "val6"},
		{int64(1), "x1", true, "val7"},
		{int64(2), "x0", false, "val8"},
		{int64(2), "x0", true, "val9"},
		{int64(2), "x1", false, "val10"},
		{int64(2), "x1", true, "val11"},
		{int64(3), "x0", false, "val12"},
		{int64(3), "x0", true, "val13"},
		{int64(3), "x1", false, "val14"},
		{int64(3), "x1", true, "val15"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)
	tsl := fmt.Sprintf(`prepare test_query1 := (scan %s from test_slab1)`, rangeStr)
	prepareQuery(t, tsl, ctx)
	mgr := ctx.qms[rand.Intn(len(ctx.qms))].qm
	var totRows [][]any
	var lock sync.Mutex
	var done sync.WaitGroup
	done.Add(1)
	var lastBatchCount int
	numParts, err := mgr.ExecutePreparedQuery("test_query1", argVals, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		rows := convertBatchToAnyArray(batch, schema)
		lock.Lock()
		defer lock.Unlock()
		totRows = append(totRows, rows...)
		if last {
			lastBatchCount++
			if lastBatchCount == numLastBatches {
				done.Done()
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, defaultNumPartitions, numParts)
	done.Wait()
	sortDataByKeyCols(totRows, keyCols, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool})
	expectedData := data[expectedStart:expectedEnd]
	require.Equal(t, expectedData, totRows)
}

func TestQMQueryWithConstants(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", true, "val0"},
		{int64(0), "x0", false, "val1"},
		{int64(0), "x1", true, "val2"},
		{int64(0), "x1", false, "val3"},
		{int64(1), "x2", true, "val4"},
		{int64(1), "x2", false, "val5"},
		{int64(1), "x3", true, "val6"},
		{int64(1), "x3", false, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{int64(1), "x2", true}, nil,
		[]string{"1", `"x2"`, "true"})
}

func TestQMFullKeyLookupCompositeKey(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", true, "val0"},
		{int64(0), "x0", false, "val1"},
		{int64(0), "x1", true, "val2"},
		{int64(0), "x1", false, "val3"},
		{int64(1), "x2", true, "val4"},
		{int64(1), "x2", false, "val5"},
		{int64(1), "x3", true, "val6"},
		{int64(1), "x3", false, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{int64(1), "x2", true}, []any{int64(1), "x2", true},
		[]string{"$x:int", "$y:string", "$z:bool"})
}

func TestQMFullKeyLookupCompositeKeyCompiledExpr(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", true, "val0"},
		{int64(0), "x0", false, "val1"},
		{int64(0), "x1", true, "val2"},
		{int64(0), "x1", false, "val3"},
		{int64(1), "x2", true, "val4"},
		{int64(1), "x2", false, "val5"},
		{int64(1), "x3", true, "val6"},
		{int64(1), "x3", false, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{int64(1), "x2", true}, []any{int64(2), "X2", false},
		[]string{"$x:int - 1", "to_lower($y:string)", "!$z:bool"})
}

func TestQMFullKeyLookupCompositeKeyDifferentKeyOrder(t *testing.T) {
	data := [][]any{
		{true, "val0", int64(0), "x0"},
		{false, "val1", int64(0), "x0"},
		{true, "val2", int64(0), "x1"},
		{false, "val3", int64(0), "x1"},
		{true, "val4", int64(1), "x2"},
		{false, "val5", int64(1), "x2"},
		{true, "val6", int64(1), "x3"},
		{false, "val7", int64(1), "x3"},
	}
	keyCols := []int{2, 3, 0}
	columnTypes := []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{int64(1), "x2", true}, []any{int64(1), "x2", true},
		[]string{"$x:int", "$y:string", "$z:bool"})
}

func TestQMNonFullKeyLookupCompositeKey(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", false, "val0"},
		{int64(0), "x0", true, "val1"},
		{int64(0), "x1", false, "val2"},
		{int64(0), "x1", true, "val3"},
		{int64(1), "x2", false, "val4"},
		{int64(1), "x2", true, "val5"},
		{int64(1), "x3", false, "val6"},
		{int64(1), "x3", true, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}

	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{int64(0)}, []any{int64(0)},
		[]string{"$x:int"}, defaultNumPartitions)

	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{int64(1)}, []any{int64(1)},
		[]string{"$x:int"}, defaultNumPartitions)

	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{int64(1), "x2"}, []any{int64(1), "x2"},
		[]string{"$x:int", "$y:string"}, defaultNumPartitions)
}

func TestQMFullKeyLookupCompositeKeyNullsInKey(t *testing.T) {
	data := [][]any{
		{nil, "x0", false, "val0"},
		{nil, "x0", true, "val1"},
		{nil, "x1", false, "val2"},
		{nil, "x1", true, "val3"},
		{int64(1), "x2", false, "val4"},
		{int64(1), "x2", nil, "val5"},
		{int64(1), nil, false, "val6"},
		{int64(1), nil, true, "val7"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{nil, "x0", true}, []any{nil, "x0", true},
		[]string{"$x:int", "$y:string", "$z:bool"})

	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{nil}, []any{nil},
		[]string{"$x:int"}, 25)

	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{int64(1), nil}, []any{int64(1), nil},
		[]string{"$x:int", "$y:string"}, 25)
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprIntColumn(t *testing.T) {
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{int64(2)}, []any{int64(2)}, []string{"$x:int"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprIntColumn(t *testing.T) {
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{int64(3)}, []any{int64(2)}, []string{"$x:int + 1"})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprFloatColumn(t *testing.T) {
	data := [][]any{
		{float64(0.1), "foo0"},
		{float64(1.1), "foo1"},
		{float64(2.1), "foo2"},
		{float64(3.1), "foo3"},
		{float64(4.1), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{float64(2.1)}, []any{float64(2.1)}, []string{"$x:float"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprFloatColumn(t *testing.T) {
	data := [][]any{
		{float64(0.1), "foo0"},
		{float64(1.1), "foo1"},
		{float64(2.1), "foo2"},
		{float64(3.1), "foo3"},
		{float64(4.1), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeFloat, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{float64(3.1)}, []any{float64(2.0)}, []string{"$x:float + 1.1f"})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprBoolColumn(t *testing.T) {
	data := [][]any{
		{true, int64(10)},
		{false, int64(20)},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeInt}
	testQMLookup(t, data, columnTypes, keyCols, []any{true}, []any{true}, []string{"$x:bool"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprBoolColumn(t *testing.T) {
	data := [][]any{
		{true, int64(10)},
		{false, int64(20)},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeBool, types.ColumnTypeInt}
	testQMLookup(t, data, columnTypes, keyCols, []any{false}, []any{true}, []string{"!$x:bool"})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprDecimalColumn(t *testing.T) {
	data := [][]any{
		{createDecimal(t, "1234.4321", 10, 4), "foo0"},
		{createDecimal(t, "2234.4321", 10, 4), "foo1"},
		{createDecimal(t, "3234.4321", 10, 4), "foo2"},
		{createDecimal(t, "4234.4321", 10, 4), "foo3"},
		{createDecimal(t, "5234.4321", 10, 4), "foo4"},
	}
	keyCols := []int{0}
	decType := &types.DecimalType{
		Precision: 10,
		Scale:     4,
	}
	columnTypes := []types.ColumnType{decType, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{createDecimal(t, "3234.4321", 10, 4)},
		[]any{createDecimal(t, "3234.4321", 10, 4)}, []string{"$x:decimal(10,4)"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprDecimalColumn(t *testing.T) {
	data := [][]any{
		{createDecimal(t, "1234.4321", 10, 4), "foo0"},
		{createDecimal(t, "2234.4321", 10, 4), "foo1"},
		{createDecimal(t, "3234.4321", 10, 4), "foo2"},
		{createDecimal(t, "4234.4321", 10, 4), "foo3"},
		{createDecimal(t, "5234.4321", 10, 4), "foo4"},
	}
	keyCols := []int{0}
	decType := &types.DecimalType{
		Precision: 10,
		Scale:     4,
	}
	columnTypes := []types.ColumnType{decType, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{createDecimal(t, "3234.4321", 10, 4)},
		[]any{createDecimal(t, "2234.4321", 10, 4)}, []string{`$x:decimal(10,4) + to_decimal("1000.0000",10,4)`})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprStringColumn(t *testing.T) {
	data := [][]any{
		{"x0", "foo0"},
		{"x1", "foo1"},
		{"x2", "foo2"},
		{"x3", "foo3"},
		{"x4", "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{"x2"}, []any{"x2"}, []string{"$x:string"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprStringColumn(t *testing.T) {
	data := [][]any{
		{"x0", "foo0"},
		{"x1", "foo1"},
		{"x2", "foo2"},
		{"x3", "foo3"},
		{"x4", "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeString, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{"x2"}, []any{"X2"}, []string{"to_lower($x:string)"})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprBytesColumn(t *testing.T) {
	data := [][]any{
		{[]byte("x0"), "foo0"},
		{[]byte("x1"), "foo1"},
		{[]byte("x2"), "foo2"},
		{[]byte("x3"), "foo3"},
		{[]byte("x4"), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{[]byte("x2")}, []any{[]byte("x2")}, []string{"$x:bytes"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprBytesColumn(t *testing.T) {
	data := [][]any{
		{[]byte("x0"), "foo0"},
		{[]byte("x1"), "foo1"},
		{[]byte("x2"), "foo2"},
		{[]byte("x3"), "foo3"},
		{[]byte("x4"), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{[]byte("x2")}, []any{"x2"}, []string{"to_bytes($x:string)"})
}

func TestQMFullKeyLookupSingleKeyColumnDirectKeyExprTimestampColumn(t *testing.T) {
	data := [][]any{
		{types.NewTimestamp(0), "foo0"},
		{types.NewTimestamp(1), "foo1"},
		{types.NewTimestamp(2), "foo2"},
		{types.NewTimestamp(3), "foo3"},
		{types.NewTimestamp(4), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols, []any{types.NewTimestamp(2)}, []any{types.NewTimestamp(2)},
		[]string{"$x:timestamp"})
}

func TestQMFullKeyLookupSingleKeyColumnCompiledKeyExprTimestampColumn(t *testing.T) {
	data := [][]any{
		{types.NewTimestamp(0), "foo0"},
		{types.NewTimestamp(1), "foo1"},
		{types.NewTimestamp(2), "foo2"},
		{types.NewTimestamp(3), "foo3"},
		{types.NewTimestamp(4), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString}
	testQMLookup(t, data, columnTypes, keyCols,
		[]any{types.NewTimestamp(2)}, []any{int64(2)}, []string{"to_timestamp($x:int)"})
}

func TestQueryVersionsSnapshotIsolation(t *testing.T) {
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	keyCols := []int{0}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	expectedKeyVals := []any{int64(2)}
	argVals := []any{int64(2)}

	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)
	tsl := `prepare test_query1 := (get $x:int from test_slab1)`
	prepareQuery(t, tsl, ctx)

	// Remote version same as sending version - should succeed
	mgr := ctx.qms[0].qm
	for _, mgrPair := range ctx.qms {
		mgrPair.qm.SetLastCompletedVersion(10)
	}
	executeQueryFromMgr(t, "test_query1", schema, keyCols, expectedKeyVals, argVals, data, 1, mgr)

	// Remote version greater than sending version - should succeed
	for _, mgrPair := range ctx.qms {
		mgrPair.qm.SetLastCompletedVersion(11)
	}
	executeQueryFromMgr(t, "test_query1", schema, keyCols, expectedKeyVals, argVals, data, 1, mgr)

	// Set to higher version
	for _, mgrPair := range ctx.qms {
		mgrPair.qm.SetLastCompletedVersion(12)
	}
	// Remote and local versions are same, and we insert some more data but at a higher version, it should not be seen
	data2 := [][]any{
		{int64(0), "boo0"},
		{int64(1), "boo1"},
		{int64(2), "boo2"},
		{int64(3), "boo3"},
		{int64(4), "boo4"},
	}
	writeDataToSlabWithVersion(t, "_default_", slabID, schema, keyCols, defaultNumPartitions, data2, ctx.st, 13)
	executeQueryFromMgr(t, "test_query1", schema, keyCols, expectedKeyVals, argVals, data, 1, mgr)

	// Now increase versions to 13
	for _, mgrPair := range ctx.qms {
		mgrPair.qm.SetLastCompletedVersion(13)
	}
	// Data should now be seen
	executeQueryFromMgr(t, "test_query1", schema, keyCols, expectedKeyVals, argVals, data2, 1, mgr)
}

func TestQueryFailsRemotingError(t *testing.T) {
	ctx := setupForQueryFailureTests(t)
	defer ctx.tearDown(t)

	// Test unavailabilty due to remoting
	for _, mgrPair := range ctx.qms {
		mgrPair.tm.SetUnavailable()
	}

	_, err := ctx.qms[0].qm.ExecutePreparedQuery("test_query1", []any{int64(1)}, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		return nil
	})
	require.Error(t, err)
	require.Equal(t, "transient remoting error test_unavailability", err.Error())
}

type failingClustVersionProvider struct {
}

func (f failingClustVersionProvider) ClusterVersion() int {
	return 1234
}

func (f failingClustVersionProvider) IsReadyAsOfVersion(int) bool {
	return false
}

func TestQueryFailsNotReadyWithVersion(t *testing.T) {

	versionProvider := &failingClustVersionProvider{}
	ctx := setupForQueryFailureTestsWithClusterVersionProvider(t, versionProvider)
	defer ctx.tearDown(t)

	_, err := ctx.qms[0].qm.ExecutePreparedQuery("test_query1", []any{int64(1)}, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		return nil
	})
	require.Error(t, err)
	require.Equal(t, "cannot handle remote query, remote node is not ready at required version", err.Error())
}

func setupForQueryFailureTests(t *testing.T) *mgrCtx {
	return setupForQueryFailureTestsWithClusterVersionProvider(t, &tppm.TestClustVersionProvider{ClustVersion: 1234})
}

func setupForQueryFailureTestsWithClusterVersionProvider(t *testing.T, clustVersionProvider clusterVersionProvider) *mgrCtx {
	data := [][]any{
		{int64(0), int64(10), "foo0"},
	}
	keyCols := []int{0, 1}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeString}
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)

	ctx := setupQueryManagersWithClusterVersionProvider(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows,
		slInfoProvider, clustVersionProvider)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)

	tsl := `prepare test_query1 := (get $x:int from test_slab1)`
	prepareQuery(t, tsl, ctx)
	return ctx
}

func TestManyRowsExerciseBatchSize(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 10,
		Scale:     2,
	}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType,
		types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}

	num0thKeyElements := 10
	numRowsPer0thKeyElement := int(defaultNumPartitions * defaultMaxBatchRows * 2.5)
	require.Greater(t, numRowsPer0thKeyElement, defaultMaxBatchRows)
	// Create some data with a composite key where there are many values with the same first element of the key so
	// we can look up a lot in one go
	data := prepareDataManyRows(t)
	keyCols := []int{0, 1}
	testQMLookupWithNumParts(t, data, columnTypes, keyCols,
		[]any{int64(num0thKeyElements / 2)}, []any{int64(num0thKeyElements / 2)},
		[]string{"$x:int"}, defaultNumPartitions)
}

func TestManyRowsExerciseBatchSizeConcurrentQueries(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 10,
		Scale:     2,
	}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType,
		types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	data := prepareDataManyRows(t)
	keyCols := []int{0, 1}
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)

	numQueries := 10
	for i := 0; i < numQueries; i++ {
		tsl := fmt.Sprintf(`prepare test_query%d := (get $x:int from test_slab1)`, i)
		prepareQuery(t, tsl, ctx)
	}

	var chans []chan error
	for i := 0; i < numQueries; i++ {
		ch := make(chan error, 1)
		chans = append(chans, ch)
		var ii = i
		go func() {
			executeQuery(t, ctx, fmt.Sprintf("test_query%d", ii), schema, keyCols, []any{int64(5)}, []any{int64(5)},
				data, defaultNumPartitions)
			ch <- nil
		}()
	}
	for _, ch := range chans {
		<-ch
	}
}

func prepareDataManyRows(t *testing.T) [][]any {
	num0thKeyElements := 10
	numRowsPer0thKeyElement := defaultNumPartitions * defaultMaxBatchRows * 2
	require.Greater(t, numRowsPer0thKeyElement, defaultMaxBatchRows)
	// Create some data with a composite key where there are many values with the same first element of the key so
	// we can look up a lot in one go
	var data [][]any
	c := 0
	for i := 0; i < num0thKeyElements; i++ {
		for j := 0; j < numRowsPer0thKeyElement; j++ {
			row := make([]any, 8)
			row[0] = int64(i)
			row[1] = int64(j)
			if c%4 != 0 {
				// make every 4th row null
				row[2] = float64(c) + 1.1
				row[3] = c%2 == 0
				row[4] = types.Decimal{
					Num:       decimal128.New(0, uint64(c+10000)),
					Precision: 10,
					Scale:     2,
				}
				row[5] = fmt.Sprintf("foo%d", c)
				row[6] = []byte(fmt.Sprintf("bytes%d", c))
				row[7] = types.NewTimestamp(20000 + int64(c))
			}
			data = append(data, row)
			c++
		}
	}
	return data
}

func TestQMSort(t *testing.T) {
	data := [][]any{
		{int64(0), "x0", false, "val2"},
		{int64(0), "x0", true, "val8"},
		{int64(0), "x1", false, "val5"},
		{int64(0), "x1", true, "val6"},
		{int64(1), "x2", false, "val4"},
		{int64(1), "x2", true, "val1"},
		{int64(1), "x3", false, "val7"},
		{int64(1), "x3", true, "val3"},
	}
	keyCols := []int{0, 1, 2}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeBool, types.ColumnTypeString}
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)
	tsl := `prepare test_query1 := (scan all from test_slab1)->(sort by f3)`
	prepareQuery(t, tsl, ctx)
	mgr := ctx.qms[rand.Intn(len(ctx.qms))].qm
	var results [][]any
	var lock sync.Mutex
	var done sync.WaitGroup
	done.Add(1)
	numParts, err := mgr.ExecutePreparedQuery("test_query1", nil, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		rows := convertBatchToAnyArray(batch, schema)
		lock.Lock()
		defer lock.Unlock()
		require.Nil(t, results)
		require.True(t, last)
		results = rows
		done.Done()
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, defaultNumPartitions, numParts)
	done.Wait()
	expectedOut := [][]any{
		{int64(1), "x2", true, "val1"},
		{int64(0), "x0", false, "val2"},
		{int64(1), "x3", true, "val3"},
		{int64(1), "x2", false, "val4"},
		{int64(0), "x1", false, "val5"},
		{int64(0), "x1", true, "val6"},
		{int64(1), "x3", false, "val7"},
		{int64(0), "x0", true, "val8"},
	}
	require.Equal(t, expectedOut, results)
}

func createDecimal(t *testing.T, str string, precision int, scale int) types.Decimal {
	num, err := decimal128.FromString(str, int32(precision), int32(scale))
	require.NoError(t, err)
	return types.Decimal{
		Num:       num,
		Precision: precision,
		Scale:     scale,
	}
}

func testQMLookup(t *testing.T, data [][]any, columnTypes []types.ColumnType,
	keyCols []int, expectedKeyVals []any, argVals []any, keyExprs []string) {
	testQMLookupWithNumParts(t, data, columnTypes, keyCols, expectedKeyVals, argVals, keyExprs, 1)
}

func testQMLookupWithNumParts(t *testing.T, data [][]any, columnTypes []types.ColumnType,
	keyCols []int, expectedKeyVals []any, argVals []any, keyExprs []string, expectedNumParts int) {
	require.NotNil(t, data)
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)
	tsl := fmt.Sprintf(`prepare test_query1 := (get %s from test_slab1)`, strings.Join(keyExprs, ","))
	prepareQuery(t, tsl, ctx)
	executeQuery(t, ctx, "test_query1", schema, keyCols, expectedKeyVals, argVals, data, expectedNumParts)
}

func TestDirectQueryGetAll(t *testing.T) {
	tsl := "(scan all from test_slab1)"
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	expected := data
	keyCols := []int{0}
	keyColTypes := []types.ColumnType{types.ColumnTypeInt}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testDirectQuery(t, tsl, data, expected, columnTypes, keyCols, keyColTypes)
}

func TestDirectQueryGet(t *testing.T) {
	tsl := "(get 2 from test_slab1)"
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	expected := [][]any{
		{int64(2), "foo2"},
	}
	keyCols := []int{0}
	keyColTypes := []types.ColumnType{types.ColumnTypeInt}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testDirectQuery(t, tsl, data, expected, columnTypes, keyCols, keyColTypes)
}

func TestDirectQueryGetRange(t *testing.T) {
	tsl := "(scan 2 to 4 from test_slab1)"
	data := [][]any{
		{int64(0), "foo0"},
		{int64(1), "foo1"},
		{int64(2), "foo2"},
		{int64(3), "foo3"},
		{int64(4), "foo4"},
	}
	expected := [][]any{
		{int64(2), "foo2"},
		{int64(3), "foo3"},
	}
	keyCols := []int{0}
	keyColTypes := []types.ColumnType{types.ColumnTypeInt}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}
	testDirectQuery(t, tsl, data, expected, columnTypes, keyCols, keyColTypes)
}

func testDirectQuery(t *testing.T, tsl string, data [][]any, expected [][]any, columnTypes []types.ColumnType,
	keyCols []int, keyColTypes []types.ColumnType) {
	require.NotNil(t, data)
	var columnNames []string
	for i := 0; i < len(columnTypes); i++ {
		columnNames = append(columnNames, fmt.Sprintf("f%d", i))
	}
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	slInfoProvider, slabID := createStreamInfoProvider("test_slab1", defaultSlabID, schema, defaultNumPartitions, keyCols)
	ctx := setupQueryManagers(defaultNumManagers, defaultNumPartitions, defaultMaxBatchRows, slInfoProvider)
	defer ctx.tearDown(t)
	writeDataToSlab(t, slabID, schema, keyCols, defaultNumPartitions, data, ctx.st)

	mgr := ctx.qms[rand.Intn(len(ctx.qms))].qm

	queryDesc, err := parser.NewParser(nil).ParseQuery(tsl)
	require.NoError(t, err)

	var totRows [][]any
	var lock sync.Mutex
	var done sync.WaitGroup
	done.Add(1)
	var lastBatchCount int
	err = mgr.ExecuteQueryDirect(tsl, *queryDesc, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		rows := convertBatchToAnyArray(batch, schema)
		lock.Lock()
		defer lock.Unlock()
		totRows = append(totRows, rows...)
		if last {
			lastBatchCount++
			if lastBatchCount == numLastBatches {
				done.Done()
			}
		}
		return nil
	})
	require.NoError(t, err)
	done.Wait()

	sortDataByKeyCols(totRows, keyCols, keyColTypes)
	require.Equal(t, len(expected), len(totRows))
	for i := 0; i < len(expected); i++ {
		rowExpected := expected[i]
		actual := totRows[i]
		require.Equalf(t, rowExpected, actual, "rows not same as pos %d", i)
	}
}

type mgrCtx struct {
	qms  []*mgrPair
	tnpp *tppm.TestNodePartitionProvider
	st   tppm.Store
}

type mgrPair struct {
	qm Manager
	tm *testRemoting
}

func (m *mgrCtx) tearDown(t *testing.T) {
	for _, pair := range m.qms {
		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			return pair.qm.(*manager).HandlerCount() == 0, nil
		}, 5*time.Second, 1*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)
		pair.tm.stop()
	}
}

func createStreamInfoProvider(streamName string, slabID int, schema *evbatch.EventSchema, numPartitions int,
	keyCols []int) (StreamInfoProvider, int) {
	slInfoProvider := &testStreamInfoProvider{
		streams: map[string]*opers.StreamInfo{
			streamName: {
				UserSlab: &opers.SlabInfo{
					StreamName: streamName,
					SlabID:     slabID,
					Schema: &opers.OperatorSchema{
						EventSchema:     schema,
						PartitionScheme: opers.NewPartitionScheme("_default_", numPartitions, false, conf.DefaultProcessorCount),
					},
					KeyColIndexes: keyCols,
					Type:          opers.SlabTypeUserTable,
				},
			},
		},
	}
	return slInfoProvider, slabID
}

func setupQueryManagers(numMgrs int, numPartitions int, maxBatchRows int,
	slInfoProvider StreamInfoProvider) *mgrCtx {
	return setupQueryManagersWithClusterVersionProvider(numMgrs, numPartitions, maxBatchRows, slInfoProvider,
		&tppm.TestClustVersionProvider{ClustVersion: 1234})
}

func setupQueryManagersWithClusterVersionProvider(numMgrs int, numPartitions int, maxBatchRows int,
	slInfoProvider StreamInfoProvider, clustVersionProvider clusterVersionProvider) *mgrCtx {
	nodePartitions := map[int][]int{}
	for i := 0; i < numPartitions; i++ {
		mgr := i % numMgrs
		nodePartitions[mgr] = append(nodePartitions[mgr], i)
	}
	npp := tppm.NewTestNodePartitionProvider(nodePartitions)
	procMgr := tppm.NewTestProcessorManager()
	mapping := opers.CalcProcessorPartitionMapping("_default_", numPartitions, conf.DefaultProcessorCount)
	for procID := range mapping {
		procMgr.AddActiveProcessor(procID)
	}
	pairs := make([]*mgrPair, numMgrs)
	addresses := make([]string, numMgrs)
	for i := range addresses {
		addresses[i] = fmt.Sprintf("addr-%d", i)
	}
	p := parser.NewParser(nil)
	for i := range pairs {
		tm := newTestRemoting()
		tm.start()
		mgr := NewManager(npp, clustVersionProvider, i, slInfoProvider, nil, procMgr, tm, addresses, maxBatchRows,
			&expr.ExpressionFactory{}, p)
		pair := &mgrPair{
			qm: mgr,
			tm: tm,
		}
		pairs[i] = pair
		mgr.SetLastCompletedVersion(0)
		mgr.Activate()
	}
	mgrsMap := map[string]Manager{}
	for i := 0; i < len(addresses); i++ {
		mgrsMap[addresses[i]] = pairs[i].qm
	}
	for _, pair := range pairs {
		pair.tm.mgrsMap = mgrsMap
	}
	return &mgrCtx{
		qms:  pairs,
		st:   procMgr.GetStore(),
		tnpp: npp,
	}
}

func deleteQuery(t *testing.T, query string, ctx *mgrCtx) {
	ast, err := parser.NewParser(nil).ParseTSL(query)
	require.NoError(t, err)
	for _, pair := range ctx.qms {
		err = pair.qm.DeleteQuery(*ast.DeleteQuery)
		require.NoError(t, err)
		paramSchema := pair.qm.GetPreparedQueryParamSchema(ast.DeleteQuery.QueryName)
		require.Nil(t, paramSchema)
	}
}

func prepareQuery(t *testing.T, query string, ctx *mgrCtx) {
	ast, err := parser.NewParser(nil).ParseTSL(query)
	require.NoError(t, err)
	for _, pair := range ctx.qms {
		err = pair.qm.PrepareQuery(*ast.PrepareQuery)
		require.NoError(t, err)

		// Try and prepare again - should fail
		err = pair.qm.PrepareQuery(*ast.PrepareQuery)
		require.Error(t, err)
		msg := fmt.Sprintf("query with name '%s' has already been prepared", ast.PrepareQuery.QueryName)
		require.Equal(t, msg, err.Error())
	}
}

func executeQueryFromMgr(t *testing.T, queryName string, evSchema *evbatch.EventSchema, keyCols []int, keyVals []any, argVals []any, data [][]any, expectedNumParts int, mgr Manager) {
	var totRows [][]any
	var lock sync.Mutex
	var done sync.WaitGroup
	done.Add(1)
	var lastBatchCount int
	numParts, err := mgr.ExecutePreparedQuery(queryName, argVals, func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		rows := convertBatchToAnyArray(batch, evSchema)
		lock.Lock()
		defer lock.Unlock()
		totRows = append(totRows, rows...)
		if last {
			lastBatchCount++
			if lastBatchCount == numLastBatches {
				done.Done()
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expectedNumParts, numParts)
	done.Wait()
	checkQueryResults(t, keyCols, evSchema, totRows, data, keyVals)
}

func checkQueryResults(t *testing.T, keyCols []int, evSchema *evbatch.EventSchema, totRows [][]any, data [][]any,
	keyVals []any) {
	var keyColTypes []types.ColumnType
	for _, keyCol := range keyCols {
		keyColTypes = append(keyColTypes, evSchema.ColumnTypes()[keyCol])
	}
	sortDataByKeyCols(totRows, keyCols, keyColTypes)
	expected := calcExpectedData(data, keyCols, keyVals)
	require.NotNil(t, expected)
	require.Equal(t, len(expected), len(totRows))
	for i := 0; i < len(expected); i++ {
		rowExpected := expected[i]
		actual := totRows[i]
		require.Equalf(t, rowExpected, actual, "rows not same as pos %d", i)
	}
}

func executeQuery(t *testing.T, ctx *mgrCtx, queryName string, evSchema *evbatch.EventSchema, keyCols []int,
	keyVals []any, argVals []any, data [][]any, expectedNumParts int) {
	mgr := ctx.qms[rand.Intn(len(ctx.qms))].qm
	executeQueryFromMgr(t, queryName, evSchema, keyCols, keyVals, argVals, data, expectedNumParts, mgr)
}

func calcExpectedData(data [][]any, keyCols []int, keyVals []any) [][]any {
	var expected [][]any
	for _, row := range data {
		matched := true
		for i, expectedVal := range keyVals {
			actualVal := row[keyCols[i]]
			if !reflect.DeepEqual(expectedVal, actualVal) {
				matched = false
				break
			}
		}
		if matched {
			expected = append(expected, row)
		}
	}
	return expected
}

func newTestRemoting() *testRemoting {
	return &testRemoting{
		sendChannel: make(chan sendInfo, 10),
	}
}

type testRemoting struct {
	mgrsMap     map[string]Manager
	sendChannel chan sendInfo
	unavailable atomic.Bool
}

func (t *testRemoting) SetUnavailable() {
	t.unavailable.Store(true)
}

func (t *testRemoting) start() {
	go t.sendLoop()
}

func (t *testRemoting) stop() {
	close(t.sendChannel)
}

type sendInfo struct {
	msg *clustermsgs.QueryMessage
	mgr Manager
	cf  func(remoting.ClusterMessage, error)
}

func (t *testRemoting) sendLoop() {
	for sendInfo := range t.sendChannel {
		err := sendInfo.mgr.ExecuteRemoteQuery(sendInfo.msg)
		sendInfo.cf(nil, err)
	}
}

func (t *testRemoting) SendQueryMessageAsync(completionFunc func(remoting.ClusterMessage, error),
	msg *clustermsgs.QueryMessage, address string) {
	if t.unavailable.Load() {
		completionFunc(nil, remoting.Error{Msg: "test_unavailability"})
		return
	}
	mgr, ok := t.mgrsMap[address]
	if !ok {
		panic("can't find manager")
	}
	t.sendChannel <- sendInfo{
		mgr: mgr,
		msg: msg,
		cf:  completionFunc,
	}
}

func (t *testRemoting) SendQueryResponse(msg *clustermsgs.QueryResponse, serverAddress string) error {
	mgr, ok := t.mgrsMap[serverAddress]
	if !ok {
		panic("can't find manager")
	}
	mgr.ReceiveQueryResult(msg)
	return nil
}

func (t *testRemoting) Close() {
}

func writeDataToSlab(t *testing.T, slabID int, schema *evbatch.EventSchema, keyCols []int, numPartitions int, data [][]any,
	st tppm.Store) {
	writeDataToSlabWithVersion(t, "_default_", slabID, schema, keyCols, numPartitions, data, st, 0)
}

func writeDataToSlabWithVersion(t *testing.T, mappingID string, slabID int, schema *evbatch.EventSchema, keyCols []int,
	numPartitions int, data [][]any, st tppm.Store, version uint64) {
	mb := mem.NewBatch()
	for _, row := range data {
		var keyBuff []byte
		keyColSet := map[int]struct{}{}
		for _, keyCol := range keyCols {
			keyColSet[keyCol] = struct{}{}
			if row[keyCol] == nil {
				keyBuff = append(keyBuff, 0)
				continue
			} else {
				keyBuff = append(keyBuff, 1)
			}
			ft := schema.ColumnTypes()[keyCol]
			switch ft.ID() {
			case types.ColumnTypeIDInt:
				keyBuff = encoding2.KeyEncodeInt(keyBuff, row[keyCol].(int64))
			case types.ColumnTypeIDFloat:
				keyBuff = encoding2.KeyEncodeFloat(keyBuff, row[keyCol].(float64))
			case types.ColumnTypeIDBool:
				keyBuff = encoding2.AppendBoolToBuffer(keyBuff, row[keyCol].(bool))
			case types.ColumnTypeIDDecimal:
				keyBuff = encoding2.KeyEncodeDecimal(keyBuff, row[keyCol].(types.Decimal))
			case types.ColumnTypeIDString:
				keyBuff = encoding2.KeyEncodeString(keyBuff, row[keyCol].(string))
			case types.ColumnTypeIDBytes:
				keyBuff = encoding2.KeyEncodeBytes(keyBuff, row[keyCol].([]byte))
			case types.ColumnTypeIDTimestamp:
				keyBuff = encoding2.KeyEncodeTimestamp(keyBuff, row[keyCol].(types.Timestamp))
			default:
				panic(fmt.Sprintf("unexpected column type %d", ft.ID()))
			}
		}

		hash := common.DefaultHash(keyBuff)
		partID := common.CalcPartition(hash, numPartitions)

		partitionHash := proc.CalcPartitionHash(mappingID, uint64(partID))
		prefix := encoding2.EncodeEntryPrefix(partitionHash, uint64(slabID), 24+len(keyBuff))
		keyBuff = append(prefix, keyBuff...)

		var valueBuff []byte
		for i, ft := range schema.ColumnTypes() {
			_, isKeyCol := keyColSet[i]
			if isKeyCol {
				continue
			}
			if row[i] == nil {
				valueBuff = append(valueBuff, 0)
				continue
			} else {
				valueBuff = append(valueBuff, 1)
			}
			switch ft.ID() {
			case types.ColumnTypeIDInt:
				valueBuff = encoding2.AppendUint64ToBufferLE(valueBuff, uint64(row[i].(int64)))
			case types.ColumnTypeIDFloat:
				valueBuff = encoding2.AppendFloat64ToBufferLE(valueBuff, row[i].(float64))
			case types.ColumnTypeIDBool:
				valueBuff = encoding2.AppendBoolToBuffer(valueBuff, row[i].(bool))
			case types.ColumnTypeIDDecimal:
				valueBuff = encoding2.AppendDecimalToBuffer(valueBuff, row[i].(types.Decimal))
			case types.ColumnTypeIDString:
				valueBuff = encoding2.AppendStringToBufferLE(valueBuff, row[i].(string))
			case types.ColumnTypeIDBytes:
				valueBuff = encoding2.AppendBytesToBufferLE(valueBuff, row[i].([]byte))
			case types.ColumnTypeIDTimestamp:
				valueBuff = encoding2.AppendUint64ToBufferLE(valueBuff, uint64(row[i].(types.Timestamp).Val))
			default:
				panic(fmt.Sprintf("unexpected column type %d", ft.ID()))
			}
		}
		keyBuff = encoding2.EncodeVersion(keyBuff, version)
		mb.AddEntry(common.KV{
			Key:   keyBuff,
			Value: valueBuff,
		})
	}
	err := st.Write(mb)
	require.NoError(t, err)
}

func convertBatchToAnyArray(batch *evbatch.Batch, outSchema *evbatch.EventSchema) [][]any {
	var data [][]any
	for i := 0; i < batch.RowCount; i++ {
		var row []any
		for j, colType := range outSchema.ColumnTypes() {
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

type testStreamInfoProvider struct {
	streams map[string]*opers.StreamInfo
}

func (t *testStreamInfoProvider) GetStream(streamName string) *opers.StreamInfo {
	return t.streams[streamName]
}
