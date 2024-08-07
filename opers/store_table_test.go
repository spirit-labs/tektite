package opers

import (
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestTableOperator(t *testing.T) {
	testTableOperatorWithKeyCols(t, "int_col")
	testTableOperatorWithKeyCols(t, "float_col")
	testTableOperatorWithKeyCols(t, "dec_col")
	testTableOperatorWithKeyCols(t, "string_col")
	testTableOperatorWithKeyCols(t, "bytes_col")
	testTableOperatorWithKeyCols(t, "ts_col")
	testTableOperatorWithKeyCols(t, "int_col", "float_col", "dec_col")
	testTableOperatorWithKeyCols(t, "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col")
}

func testTableOperatorWithKeyCols(t *testing.T, keyCols ...string) {
	fNames := []string{"int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	to := createTableOperator(t, keyCols, fNames, fTypes)
	data := [][]any{
		{int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(11), float64(1.1), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1)},
		{int64(12), float64(1.2), true, createDecimal(t, "32345.54321"), "str3", []byte("bytes3"), types.NewTimestamp(2)},
		{int64(13), float64(1.3), true, createDecimal(t, "42345.54321"), "str4", []byte("bytes4"), types.NewTimestamp(3)},
		{int64(14), float64(1.4), true, createDecimal(t, "52345.54321"), "str5", []byte("bytes5"), types.NewTimestamp(4)},
	}
	testTableOperator(t, to, fNames, fTypes, fNames, fTypes, data, data)
}

func TestOffsetRemoved(t *testing.T) {
	fNames := []string{"offset", "event_time", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	to := createTableOperator(t, []string{"int_col"}, fNames, fTypes)
	dataIn := [][]any{
		{int64(0), types.NewTimestamp(1000), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(1), types.NewTimestamp(1001), int64(11), float64(1.1), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1)},
		{int64(2), types.NewTimestamp(1002), int64(12), float64(1.2), true, createDecimal(t, "32345.54321"), "str3", []byte("bytes3"), types.NewTimestamp(2)},
		{int64(3), types.NewTimestamp(1003), int64(13), float64(1.3), true, createDecimal(t, "42345.54321"), "str4", []byte("bytes4"), types.NewTimestamp(3)},
		{int64(4), types.NewTimestamp(1004), int64(14), float64(1.4), true, createDecimal(t, "52345.54321"), "str5", []byte("bytes5"), types.NewTimestamp(4)},
	}
	dataOut := [][]any{
		{types.NewTimestamp(1000), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{types.NewTimestamp(1001), int64(11), float64(1.1), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1)},
		{types.NewTimestamp(1002), int64(12), float64(1.2), true, createDecimal(t, "32345.54321"), "str3", []byte("bytes3"), types.NewTimestamp(2)},
		{types.NewTimestamp(1003), int64(13), float64(1.3), true, createDecimal(t, "42345.54321"), "str4", []byte("bytes4"), types.NewTimestamp(3)},
		{types.NewTimestamp(1004), int64(14), float64(1.4), true, createDecimal(t, "52345.54321"), "str5", []byte("bytes5"), types.NewTimestamp(4)},
	}
	testTableOperator(t, to, fNames, fTypes, fNames[1:], fTypes[1:], dataIn, dataOut)
}

func createTableOperator(t *testing.T, keyCols []string, columnNamesIn []string, columnTypesIn []types.ColumnType) *StoreTableOperator {
	inSchema := evbatch.NewEventSchema(columnNamesIn, columnTypesIn)
	to, err := NewStoreTableOperator(&OperatorSchema{EventSchema: inSchema, PartitionScheme: PartitionScheme{MappingID: "mapping", Partitions: 10}}, 1001, keyCols, -1,
		&parser.StoreTableDesc{})
	require.NoError(t, err)
	return to
}

func testTableOperator(t *testing.T, to *StoreTableOperator, columnNamesIn []string, columnTypesIn []types.ColumnType,
	columnNamesOut []string, columnTypesOut []types.ColumnType, dataIn [][]any, expectedOutData [][]any) {

	partitionCount := 10

	for partID := 0; partID < partitionCount; partID++ {
		batch := createEventBatch(columnNamesIn, columnTypesIn, dataIn)
		//goland:noinspection GoDeferInLoop
		defer batch.Release()

		version := 1234
		ctx := &testExecCtx{
			version:     version,
			partitionID: partID,
		}

		out, err := to.HandleStreamBatch(batch, ctx)
		require.NoError(t, err)

		expectedOutSchema := evbatch.NewEventSchema(columnNamesOut, columnTypesOut)
		require.Equal(t, expectedOutSchema, to.OutSchema().EventSchema)

		actualOutData := convertBatchToAnyArray(out)
		require.Equal(t, expectedOutData, actualOutData)
		require.Equal(t, len(expectedOutData), len(ctx.entries))
		var keyTypes []types.ColumnType
		for _, keyCol := range to.outKeyCols {
			keyTypes = append(keyTypes, to.OutSchema().EventSchema.ColumnTypes()[keyCol])
		}
		var rowTypes []types.ColumnType
		for _, colIndex := range to.outRowCols {
			rowTypes = append(rowTypes, to.OutSchema().EventSchema.ColumnTypes()[colIndex])
		}
		var loadedOutData [][]any
		for _, kv := range ctx.entries {
			partitionHash := kv.Key[:16]
			expectedPartitionHash := proc.CalcPartitionHash(to.OutSchema().MappingID, uint64(partID))
			require.Equal(t, expectedPartitionHash, partitionHash)
			slabID, _ := encoding2.ReadUint64FromBufferBE(kv.Key, 16)
			ver, _ := encoding2.ReadUint64FromBufferBE(kv.Key, len(kv.Key)-8)
			ver = math.MaxUint64 - ver
			require.Equal(t, 1001, int(slabID))
			require.Equal(t, version, int(ver))
			key := kv.Key[24:]
			keySlice, _, err := encoding2.DecodeKeyToSlice(key, 0, keyTypes)
			require.NoError(t, err)
			rowSlice, _ := encoding2.DecodeRowToSlice(kv.Value, 0, rowTypes)
			actualOut := make([]any, len(to.OutSchema().EventSchema.ColumnTypes()))
			for i, keyCol := range to.outKeyCols {
				actualOut[keyCol] = keySlice[i]
			}
			for i, rowCol := range to.outRowCols {
				actualOut[rowCol] = rowSlice[i]
			}
			loadedOutData = append(loadedOutData, actualOut)
		}
		loadedOutData = sortDataByKeyCols(loadedOutData, to.outKeyCols, keyTypes)
		require.Equal(t, expectedOutData, loadedOutData)
	}
}
