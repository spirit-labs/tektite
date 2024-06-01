package opers

import (
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestIntKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), int64(10), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), int64(20), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestIntKeyColNoOffset(t *testing.T) {
	inColumnNames := []string{"event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{types.NewTimestamp(1000), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{types.NewTimestamp(1001), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{types.NewTimestamp(1002), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{types.NewTimestamp(1003), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{types.NewTimestamp(1004), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), int64(10), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), int64(20), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestFloatKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeFloat,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), float64(10.1), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), float64(10.1), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), float64(10.1), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), float64(20.1), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), float64(20.1), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeFloat, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), float64(10.1), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), float64(20.1), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestBoolKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeBool,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), true, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), true, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), true, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), false, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), false, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeBool, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), false, int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
		{types.NewTimestamp(1002), true, int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)}}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestDecimalKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, decType,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), createDecimal(t, "23000.123"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), createDecimal(t, "23000.123"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), createDecimal(t, "23000.123"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), createDecimal(t, "46000.123"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), createDecimal(t, "46000.123"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, decType, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), createDecimal(t, "23000.123"), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), createDecimal(t, "46000.123"), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestStringKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "foo", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), "foo", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), "foo", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), "bar", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), "bar", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), "bar", int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
		{types.NewTimestamp(1002), "foo", int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestBytesKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeBytes,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), []byte("foo"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), []byte("foo"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), []byte("foo"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), []byte("bar"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), []byte("bar"), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeBytes, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), []byte("bar"), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
		{types.NewTimestamp(1002), []byte("foo"), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestTimestampKeyCol(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), types.NewTimestamp(1000), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), types.NewTimestamp(1000), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), types.NewTimestamp(1000), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), types.NewTimestamp(2000), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), types.NewTimestamp(2000), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), types.NewTimestamp(1000), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), types.NewTimestamp(2000), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestKeyColNullValues(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), nil, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), nil, int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), nil, int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
		{types.NewTimestamp(1002), int64(10), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestMultipleKeyCols(t *testing.T) {
	inColumnNames := []string{"offset", "event_time",
		"kc_int", "kc_float", "kc_bool", "kc_dec", "kc_string", "kc_bytes", "kc_ts",
		"int_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp,
		types.ColumnTypeInt}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(2), types.NewTimestamp(1001), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(3), types.NewTimestamp(1002), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(4), types.NewTimestamp(1003), int64(11), float64(2.0), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1), int64(12)},
		{int64(5), types.NewTimestamp(1004), int64(11), float64(2.0), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1), int64(12)},
	}

	aggExprs := []string{"count(int_col)"}
	keyExprs := []string{"kc_int", "kc_float", "kc_bool", "kc_dec", "kc_string", "kc_bytes", "kc_ts"}
	outColumnNames := []string{"event_time", "kc_int", "kc_float", "kc_bool", "kc_dec", "kc_string", "kc_bytes", "kc_ts",
		"count(int_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp,
		types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(3)},
		{types.NewTimestamp(1004), int64(11), float64(2.0), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestMultipleKeyColsWithInnerExprs(t *testing.T) {
	inColumnNames := []string{"offset", "event_time",
		"kc_int", "kc_float", "kc_bool", "kc_dec", "kc_string", "kc_bytes", "kc_ts",
		"int_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp,
		types.ColumnTypeInt}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(2), types.NewTimestamp(1001), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(3), types.NewTimestamp(1002), int64(10), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0), int64(11)},
		{int64(4), types.NewTimestamp(1003), int64(11), float64(2.0), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1), int64(12)},
		{int64(5), types.NewTimestamp(1004), int64(11), float64(2.0), true, createDecimal(t, "22345.54321"), "str2", []byte("bytes2"), types.NewTimestamp(1), int64(12)},
	}

	aggExprs := []string{"count(int_col)"}
	keyExprs := []string{"kc_int + 10", "kc_float + 10f", "!kc_bool", `kc_dec + to_decimal("10.0",38,6)`, "to_upper(kc_string)", "to_string(kc_bytes)", "kc_ts"}
	outColumnNames := []string{"event_time", "kc_int + 10", "kc_float + 10f", "!kc_bool", `kc_dec + to_decimal("10.0",38,6)`, "to_upper(kc_string)", "to_string(kc_bytes)", "kc_ts",
		"count(int_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeString,
		types.ColumnTypeTimestamp,
		types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), int64(20), float64(11.0), false, createDecimal(t, "12355.54321"), "STR1", "bytes1", types.NewTimestamp(0), int64(3)},
		{types.NewTimestamp(1004), int64(21), float64(12.0), false, createDecimal(t, "22355.54321"), "STR2", "bytes2", types.NewTimestamp(1), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestColumnReordering(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc_int", "kc_string", "int_col", "float_col", "dec_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), "k1", int64(2045), float64(123.45), createDecimal(t, "12345.54321")},
		{int64(2), types.NewTimestamp(1001), int64(10), "k1", int64(1023), float64(3643.76), createDecimal(t, "46454.23484")},
		{int64(3), types.NewTimestamp(1002), int64(10), "k1", int64(-4546), float64(-23.65), createDecimal(t, "-45644.32")},
		{int64(4), types.NewTimestamp(1003), int64(10), "k1", int64(-45), float64(-6786.45), createDecimal(t, "-6767.34")},
		{int64(5), types.NewTimestamp(1004), int64(10), "k1", int64(0), float64(0), createDecimal(t, "0")},
		{int64(6), types.NewTimestamp(1005), int64(20), "k2", int64(4244), float64(543.78), createDecimal(t, "54324.5455")},
		{int64(7), types.NewTimestamp(1006), int64(20), "k2", int64(2413), float64(69859.56), createDecimal(t, "6767656.363")},
		{int64(8), types.NewTimestamp(1007), int64(20), "k2", int64(-57565), float64(-57.67), createDecimal(t, "-676584.68687")},
		{int64(9), types.NewTimestamp(1008), int64(20), "k2", int64(-6866), float64(-54576.78), createDecimal(t, "-67868.28")},
		{int64(10), types.NewTimestamp(1009), int64(20), "k2", int64(0), float64(0), createDecimal(t, "0")},
	}
	aggExprs := []string{"sum(float_col)", "sum(dec_col)", "sum(int_col)"}
	keyExprs := []string{"kc_string", "kc_int"}
	outColumnNames := []string{"event_time", "kc_string", "kc_int", "sum(float_col)", "sum(dec_col)", "sum(int_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", int64(10), float64(-3042.89), createDecimal(t, "6388.11805"), int64(-1523)},
		{types.NewTimestamp(1009), "k2", int64(20), float64(15768.89), createDecimal(t, "6077527.94163"), int64(-57774)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateSum(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(123.45), createDecimal(t, "12345.54321")},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1023), float64(3643.76), createDecimal(t, "46454.23484")},
		{int64(3), types.NewTimestamp(1002), "k1", int64(-4546), float64(-23.65), createDecimal(t, "-45644.32")},
		{int64(4), types.NewTimestamp(1003), "k1", int64(-45), float64(-6786.45), createDecimal(t, "-6767.34")},
		{int64(5), types.NewTimestamp(1004), "k1", int64(0), float64(0), createDecimal(t, "0")},

		{int64(6), types.NewTimestamp(1005), "k2", int64(4244), float64(543.78), createDecimal(t, "54324.5455")},
		{int64(7), types.NewTimestamp(1006), "k2", int64(2413), float64(69859.56), createDecimal(t, "6767656.363")},
		{int64(8), types.NewTimestamp(1007), "k2", int64(-57565), float64(-57.67), createDecimal(t, "-676584.68687")},
		{int64(9), types.NewTimestamp(1008), "k2", int64(-6866), float64(-54576.78), createDecimal(t, "-67868.28")},
		{int64(10), types.NewTimestamp(1009), "k2", int64(0), float64(0), createDecimal(t, "0")},
	}
	// Group by one col
	aggExprs := []string{"sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", int64(-1523), float64(-3042.89), createDecimal(t, "6388.11805")},
		{types.NewTimestamp(1009), "k2", int64(-57774), float64(15768.89), createDecimal(t, "6077527.94163")},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)

	// Group by zero columns
	aggExprs = []string{"sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData = [][]any{
		{types.NewTimestamp(1009), int64(-59297), float64(12726), createDecimal(t, "6083916.05968")},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateSumWithNulls(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(123.45), createDecimal(t, "12345.54321")},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1023), float64(3643.76), createDecimal(t, "46454.23484")},
		{int64(3), types.NewTimestamp(1002), "k1", nil, nil, nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(-4546), float64(-23.65), createDecimal(t, "-45644.32")},
		{int64(5), types.NewTimestamp(1004), "k1", int64(-45), float64(-6786.45), createDecimal(t, "-6767.34")},
		{int64(6), types.NewTimestamp(1005), "k1", int64(0), float64(0), createDecimal(t, "0")},

		{int64(7), types.NewTimestamp(1006), "k2", int64(4244), float64(543.78), createDecimal(t, "54324.5455")},
		{int64(8), types.NewTimestamp(1007), "k2", nil, nil, nil},
		{int64(9), types.NewTimestamp(1008), "k2", int64(2413), float64(69859.56), createDecimal(t, "6767656.363")},
		{int64(10), types.NewTimestamp(1009), "k2", int64(-57565), float64(-57.67), createDecimal(t, "-676584.68687")},
		{int64(11), types.NewTimestamp(1010), "k2", nil, float64(-54576.78), nil},
		{int64(12), types.NewTimestamp(1011), "k2", int64(-6866), nil, createDecimal(t, "-67868.28")},
		{int64(13), types.NewTimestamp(1012), "k2", int64(0), float64(0), createDecimal(t, "0")},
	}
	// Group by one col
	aggExprs := []string{"sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData := [][]any{
		{types.NewTimestamp(1005), "k1", int64(-1523), float64(-3042.89), createDecimal(t, "6388.11805")},
		{types.NewTimestamp(1012), "k2", int64(-57774), float64(15768.89), createDecimal(t, "6077527.94163")},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData = [][]any{
		{types.NewTimestamp(1012), int64(-59297), float64(12726), createDecimal(t, "6083916.05968")},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateSumReloadData(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(123.45), createDecimal(t, "12345.54321")},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1023), float64(3643.76), createDecimal(t, "46454.23484")},
		{int64(3), types.NewTimestamp(1002), "k1", nil, nil, nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(-4546), float64(-23.65), createDecimal(t, "-45644.32")},
		{int64(5), types.NewTimestamp(1004), "k1", int64(-45), float64(-6786.45), createDecimal(t, "-6767.34")},
		{int64(6), types.NewTimestamp(1005), "k1", int64(0), float64(0), createDecimal(t, "0")},

		{int64(7), types.NewTimestamp(1006), "k2", int64(4244), float64(543.78), createDecimal(t, "54324.5455")},
		{int64(8), types.NewTimestamp(1007), "k2", nil, nil, nil},
		{int64(9), types.NewTimestamp(1008), "k2", int64(2413), float64(69859.56), createDecimal(t, "6767656.363")},
		{int64(10), types.NewTimestamp(1009), "k2", int64(-57565), float64(-57.67), createDecimal(t, "-676584.68687")},
		{int64(11), types.NewTimestamp(1010), "k2", nil, float64(-54576.78), nil},
		{int64(12), types.NewTimestamp(1011), "k2", int64(-6866), nil, createDecimal(t, "-67868.28")},
		{int64(13), types.NewTimestamp(1012), "k2", int64(0), float64(0), createDecimal(t, "0")},
	}
	// Group by one col
	aggExprs := []string{"sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "sum(int_col)", "sum(float_col)", "sum(dec_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData := [][]any{
		{types.NewTimestamp(1005), "k1", int64(-1523), float64(-3042.89), createDecimal(t, "6388.11805")},
		{types.NewTimestamp(1012), "k2", int64(-57774), float64(15768.89), createDecimal(t, "6077527.94163")},
	}
	stored := testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)

	// Now add some more state to test reloading
	inData = [][]any{
		{int64(14), types.NewTimestamp(1013), "k1", int64(2323), float64(2332.45), createDecimal(t, "123123.324")},
		{int64(15), types.NewTimestamp(1014), "k1", int64(1233), float64(65446.34), createDecimal(t, "3453467.123")},

		{int64(16), types.NewTimestamp(1015), "k2", int64(4534), float64(432.56), createDecimal(t, "56757.3453")},
		{int64(17), types.NewTimestamp(1016), "k2", int64(2345), float64(23123.43), createDecimal(t, "452452.2433")},
	}
	outData = [][]any{
		{types.NewTimestamp(1014), "k1", int64(2033), float64(64735.899999999994), createDecimal(t, "3582978.56505")},
		{types.NewTimestamp(1016), "k2", int64(-50895), float64(39324.88), createDecimal(t, "6586737.53023")},
	}
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, stored)
}

func TestAggregateCount(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), "k2", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), "k2", int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData = [][]any{
		{types.NewTimestamp(1004), int64(5), int64(5), int64(5), int64(5), int64(5), int64(5), int64(5)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateCountWithNulls(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1), float64(1.0), nil, createDecimal(t, "12345.54321"), "str1", nil, nil},
		{int64(3), types.NewTimestamp(1002), "k1", nil, nil, nil, nil, nil, nil, nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), nil, nil, types.NewTimestamp(0)},

		{int64(5), types.NewTimestamp(1004), "k2", int64(1), float64(1.0), true, nil, "str1", []byte("bytes1"), nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(1), nil, nil, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1003), "k1", int64(2), int64(3), int64(2), int64(3), int64(2), int64(1), int64(2)},
		{types.NewTimestamp(1005), "k2", int64(2), int64(1), int64(1), int64(1), int64(2), int64(2), int64(1)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData = [][]any{
		{types.NewTimestamp(1005), int64(4), int64(4), int64(3), int64(4), int64(4), int64(3), int64(3)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateCountReloadData(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1), float64(1.0), nil, createDecimal(t, "12345.54321"), "str1", nil, nil},
		{int64(3), types.NewTimestamp(1002), "k1", nil, nil, nil, nil, nil, nil, nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), nil, nil, types.NewTimestamp(0)},

		{int64(5), types.NewTimestamp(1004), "k2", int64(1), float64(1.0), true, nil, "str1", []byte("bytes1"), nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(1), nil, nil, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count(int_col)", "count(float_col)", "count(bool_col)", "count(dec_col)", "count(string_col)",
		"count(bytes_col)", "count(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1003), "k1", int64(2), int64(3), int64(2), int64(3), int64(2), int64(1), int64(2)},
		{types.NewTimestamp(1005), "k2", int64(2), int64(1), int64(1), int64(1), int64(2), int64(2), int64(1)},
	}
	stored := testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)
	// Now add more data
	inData = [][]any{
		{int64(7), types.NewTimestamp(1006), "k1", nil, float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(8), types.NewTimestamp(1007), "k1", nil, nil, nil, nil, nil, nil, nil},
		{int64(9), types.NewTimestamp(1008), "k1", int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), nil, nil, types.NewTimestamp(0)},

		{int64(10), types.NewTimestamp(1009), "k2", int64(1), nil, nil, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	outData = [][]any{
		{types.NewTimestamp(1008), "k1", int64(3), int64(5), int64(4), int64(5), int64(3), int64(2), int64(4)},
		{types.NewTimestamp(1009), "k2", int64(3), int64(1), int64(1), int64(2), int64(3), int64(3), int64(2)},
	}
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, stored)
}

func TestAggregateMin(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(-123.45), createDecimal(t, "12345.54321"), "abc", []byte("zzz"), types.NewTimestamp(123)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), float64(46454.23), createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), createDecimal(t, "47645.4"), "z", []byte("aa"), types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", int64(345434), float64(-1233.45), createDecimal(t, "24323.435"), "gdd", []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), float64(3434.23), createDecimal(t, "56575.32"), "sefdsdf", []byte("ggd"), types.NewTimestamp(0)},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), createDecimal(t, "5656.5"), "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(-24234), float64(-123.45), createDecimal(t, "-56575.32"), "abc", []byte("aa"), types.NewTimestamp(123)},
		{types.NewTimestamp(1005), "k2", int64(-4574), float64(-1233.45), createDecimal(t, "5656.5"), "gdd", []byte{}, types.NewTimestamp(0)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1005), int64(-24234), float64(-1233.45), createDecimal(t, "-56575.32"), "abc", []byte{}, types.NewTimestamp(0)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateMinWithNulls(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, nil, createDecimal(t, "12345.54321"), nil, []byte("zzz"), nil},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), nil, createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), nil, "z", nil, types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", nil, float64(-1233.45), nil, nil, []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), nil, createDecimal(t, "56575.32"), "sefdsdf", nil, nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), nil, "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(-24234), float64(465.67), createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{types.NewTimestamp(1005), "k2", int64(-4574), float64(-1233.45), createDecimal(t, "56575.32"), "s", []byte{}, types.NewTimestamp(323)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1005), int64(-24234), float64(-1233.45), createDecimal(t, "-56575.32"), "defg", []byte{}, types.NewTimestamp(323)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateMinReloadData(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, nil, createDecimal(t, "12345.54321"), nil, []byte("zzz"), nil},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), nil, createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), nil, "z", nil, types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", nil, float64(-1233.45), nil, nil, []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), nil, createDecimal(t, "56575.32"), "sefdsdf", nil, nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), nil, "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "min(int_col)", "min(float_col)", "min(dec_col)", "min(string_col)", "min(bytes_col)", "min(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(-24234), float64(465.67), createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{types.NewTimestamp(1005), "k2", int64(-4574), float64(-1233.45), createDecimal(t, "56575.32"), "s", []byte{}, types.NewTimestamp(323)},
	}
	stored := testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)
	// Add some more data
	inData = [][]any{
		{int64(7), types.NewTimestamp(1006), "k1", nil, nil, createDecimal(t, "32234.434"), nil, []byte(""), nil},
		{int64(8), types.NewTimestamp(1007), "k1", int64(-57575), nil, createDecimal(t, "-234234234.32"), "", []byte("qdydty"), types.NewTimestamp(2003)},

		{int64(9), types.NewTimestamp(1008), "k2", nil, float64(-234234.45), nil, nil, []byte("z"), types.NewTimestamp(12)},
		{int64(10), types.NewTimestamp(1009), "k2", int64(123123), float64(-3234234.67), nil, "a", []byte{}, types.NewTimestamp(1233)},
	}
	outData = [][]any{
		{types.NewTimestamp(1007), "k1", int64(-57575), float64(465.67), createDecimal(t, "-234234234.32"), "", []byte(""), types.NewTimestamp(1003)},
		{types.NewTimestamp(1009), "k2", int64(-4574), float64(-3234234.67), createDecimal(t, "56575.32"), "a", []byte{}, types.NewTimestamp(12)},
	}
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, stored)
}

func TestAggregateMax(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(-123.45), createDecimal(t, "12345.54321"), "abc", []byte("zzz"), types.NewTimestamp(123)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), float64(46454.23), createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), createDecimal(t, "47645.4"), "z", []byte("aa"), types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", int64(345434), float64(-1233.45), createDecimal(t, "24323.435"), "gdd", []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), float64(3434.23), createDecimal(t, "56575.32"), "sefdsdf", []byte("ggd"), types.NewTimestamp(0)},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), createDecimal(t, "5656.5"), "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(2045), float64(46454.23), createDecimal(t, "47645.4"), "z", []byte("zzz"), types.NewTimestamp(1000000)},
		{types.NewTimestamp(1005), "k2", int64(345434), float64(3434.23), createDecimal(t, "56575.32"), "sefdsdf", []byte("wer"), types.NewTimestamp(34634)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1005), int64(345434), float64(46454.23), createDecimal(t, "56575.32"), "z", []byte("zzz"), types.NewTimestamp(1000000)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateMaxWithNulls(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, nil, createDecimal(t, "12345.54321"), nil, []byte("zzz"), nil},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), nil, createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), nil, "z", nil, types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", nil, float64(-1233.45), nil, nil, []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), nil, createDecimal(t, "56575.32"), "sefdsdf", nil, nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), nil, "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(123), float64(465.67), createDecimal(t, "12345.54321"), "z", []byte("zzz"), types.NewTimestamp(1000000)},
		{types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), createDecimal(t, "56575.32"), "sefdsdf", []byte("wer"), types.NewTimestamp(34634)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
	// Group by zero columns
	aggExprs = []string{"max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1005), int64(3123), float64(465.67), createDecimal(t, "56575.32"), "z", []byte("zzz"), types.NewTimestamp(1000000)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateMaxReloadData(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", nil, nil, createDecimal(t, "12345.54321"), nil, []byte("zzz"), nil},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-24234), nil, createDecimal(t, "-56575.32"), "defg", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(123), float64(465.67), nil, "z", nil, types.NewTimestamp(1000000)},
		{int64(4), types.NewTimestamp(1003), "k2", nil, float64(-1233.45), nil, nil, []byte("wer"), types.NewTimestamp(34634)},
		{int64(5), types.NewTimestamp(1004), "k2", int64(-4574), nil, createDecimal(t, "56575.32"), "sefdsdf", nil, nil},
		{int64(6), types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), nil, "s", []byte{}, types.NewTimestamp(323)},
	}
	// Group by one col
	aggExprs := []string{"max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "max(int_col)", "max(float_col)", "max(dec_col)", "max(string_col)", "max(bytes_col)", "max(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1002), "k1", int64(123), float64(465.67), createDecimal(t, "12345.54321"), "z", []byte("zzz"), types.NewTimestamp(1000000)},
		{types.NewTimestamp(1005), "k2", int64(3123), float64(-1223.67), createDecimal(t, "56575.32"), "sefdsdf", []byte("wer"), types.NewTimestamp(34634)},
	}
	stored := testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)
	// Now reload data
	inData = [][]any{
		{int64(7), types.NewTimestamp(1006), "k1", int64(-24234), nil, createDecimal(t, "234523424.32"), "zzz", []byte("qdydty"), types.NewTimestamp(1003)},
		{int64(8), types.NewTimestamp(1007), "k1", int64(35363), float64(34344.67), nil, "z", nil, types.NewTimestamp(0)},

		{int64(9), types.NewTimestamp(1008), "k2", int64(435345), nil, createDecimal(t, "-56575.32"), "sefdsdf", nil, nil},
		{int64(10), types.NewTimestamp(1009), "k2", int64(-2345234), float64(-12243.67), nil, "zzzzzzz", []byte("zzzzzzz"), types.NewTimestamp(234234234)},
	}
	outData = [][]any{
		{types.NewTimestamp(1007), "k1", int64(35363), float64(34344.67), createDecimal(t, "234523424.32"), "zzz", []byte("zzz"), types.NewTimestamp(1000000)},
		{types.NewTimestamp(1009), "k2", int64(435345), float64(-1223.67), createDecimal(t, "56575.32"), "zzzzzzz", []byte("zzzzzzz"), types.NewTimestamp(234234234)},
	}
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, stored)
}

func TestAggregateAvg(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "ts_col"}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(1000), float64(100.1), types.NewTimestamp(1001)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1001), float64(100.2), types.NewTimestamp(1002)},
		{int64(3), types.NewTimestamp(1002), "k1", int64(1002), float64(100.3), types.NewTimestamp(1003)},
		{int64(4), types.NewTimestamp(1003), "k1", int64(1003), float64(100.4), types.NewTimestamp(1004)},
		{int64(5), types.NewTimestamp(1004), "k1", int64(1004), float64(100.5), types.NewTimestamp(1005)},

		{int64(6), types.NewTimestamp(1005), "k2", int64(2001), float64(200.1), types.NewTimestamp(2001)},
		{int64(7), types.NewTimestamp(1006), "k2", int64(2002), float64(200.2), types.NewTimestamp(2002)},
		{int64(8), types.NewTimestamp(1007), "k2", int64(2003), float64(200.3), types.NewTimestamp(2003)},
		{int64(9), types.NewTimestamp(1008), "k2", int64(2004), float64(200.4), types.NewTimestamp(2004)},
		{int64(10), types.NewTimestamp(1009), "k2", int64(2005), float64(200.5), types.NewTimestamp(2005)},
	}
	// Group by one col
	aggExprs := []string{"avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", float64(1002), float64(100.3), types.NewTimestamp(1003)},
		{types.NewTimestamp(1009), "k2", float64(2003), float64(200.29999999999998), types.NewTimestamp(2003)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)

	// Group by zero columns
	aggExprs = []string{"avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1009), float64(1502.5), float64(150.3), types.NewTimestamp(1503)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateAvgWithNulls(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "ts_col"}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(1000), float64(100.1), types.NewTimestamp(1001)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1001), nil, types.NewTimestamp(1002)},
		{int64(3), types.NewTimestamp(1002), "k1", nil, float64(100.3), nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(1003), float64(100.4), types.NewTimestamp(1004)},
		{int64(5), types.NewTimestamp(1004), "k1", int64(1004), nil, types.NewTimestamp(1005)},

		{int64(6), types.NewTimestamp(1005), "k2", int64(2001), nil, types.NewTimestamp(2001)},
		{int64(7), types.NewTimestamp(1006), "k2", nil, float64(200.2), nil},
		{int64(8), types.NewTimestamp(1007), "k2", int64(2003), float64(200.3), nil},
		{int64(9), types.NewTimestamp(1008), "k2", nil, nil, types.NewTimestamp(2004)},
		{int64(10), types.NewTimestamp(1009), "k2", int64(2005), float64(200.5), types.NewTimestamp(2005)},
	}
	// Group by one col
	aggExprs := []string{"avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", float64(1002), float64(100.26666666666665), types.NewTimestamp(1003)},
		{types.NewTimestamp(1009), "k2", float64(2003), float64(200.333333333333333), types.NewTimestamp(2003)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)

	// Group by zero columns
	aggExprs = []string{"avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	keyExprs = []string{}
	outColumnNames = []string{"event_time", "avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	outColumnTypes = []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	outData = [][]any{
		{types.NewTimestamp(1009), float64(1431), float64(150.29999999999998), types.NewTimestamp(1431)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateAvgReloadData(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "ts_col"}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(1000), float64(100.1), types.NewTimestamp(1001)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1001), nil, types.NewTimestamp(1002)},
		{int64(3), types.NewTimestamp(1002), "k1", nil, float64(100.3), nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(1003), float64(100.4), types.NewTimestamp(1004)},
		{int64(5), types.NewTimestamp(1004), "k1", int64(1004), nil, types.NewTimestamp(1005)},

		{int64(6), types.NewTimestamp(1005), "k2", int64(2001), nil, types.NewTimestamp(2001)},
		{int64(7), types.NewTimestamp(1006), "k2", nil, float64(200.2), nil},
		{int64(8), types.NewTimestamp(1007), "k2", int64(2003), float64(200.3), nil},
		{int64(9), types.NewTimestamp(1008), "k2", nil, nil, types.NewTimestamp(2004)},
		{int64(10), types.NewTimestamp(1009), "k2", int64(2005), float64(200.5), types.NewTimestamp(2005)},
	}
	aggExprs := []string{"avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "avg(int_col)", "avg(float_col)", "avg(ts_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeFloat, types.ColumnTypeFloat, types.ColumnTypeTimestamp}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", float64(1002), float64(100.26666666666665), types.NewTimestamp(1003)},
		{types.NewTimestamp(1009), "k2", float64(2003), float64(200.333333333333333), types.NewTimestamp(2003)},
	}
	stored := testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)

	// Now add more data
	inData = [][]any{
		{int64(1), types.NewTimestamp(1010), "k1", int64(1005), float64(100.6), types.NewTimestamp(1006)},
		{int64(2), types.NewTimestamp(1011), "k1", int64(1006), nil, types.NewTimestamp(1007)},
		{int64(5), types.NewTimestamp(1012), "k1", nil, float64(100.8), types.NewTimestamp(1008)},

		{int64(6), types.NewTimestamp(1013), "k2", int64(2006), nil, types.NewTimestamp(2006)},
		{int64(7), types.NewTimestamp(1014), "k2", nil, float64(200.7), nil},
		{int64(8), types.NewTimestamp(1015), "k2", int64(2008), float64(200.8), types.NewTimestamp(2007)},
	}
	outData = [][]any{
		{types.NewTimestamp(1012), "k1", float64(1003.1666666666666), float64(100.43999999999998), types.NewTimestamp(1004)},
		{types.NewTimestamp(1015), "k2", float64(2004.6), float64(200.5), types.NewTimestamp(2004)},
	}
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, stored)
}

func TestAggregateMultipleAggFuncs(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col"}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2123)},
		{int64(2), types.NewTimestamp(1001), "k1", int64(-12314342)},
		{int64(3), types.NewTimestamp(1002), "k1", nil},
		{int64(4), types.NewTimestamp(1003), "k1", int64(234234234)},
		{int64(5), types.NewTimestamp(1004), "k1", int64(2323)},

		{int64(6), types.NewTimestamp(1005), "k2", int64(-238732)},
		{int64(7), types.NewTimestamp(1006), "k2", nil},
		{int64(8), types.NewTimestamp(1007), "k2", int64(3434)},
		{int64(9), types.NewTimestamp(1008), "k2", int64(0)},

		{int64(10), types.NewTimestamp(1009), "k3", int64(456456)},
		{int64(11), types.NewTimestamp(1010), "k3", int64(36465)},
		{int64(12), types.NewTimestamp(1011), "k3", nil},
		{int64(13), types.NewTimestamp(1012), "k3", int64(-37373)},
		{int64(14), types.NewTimestamp(1013), "k3", int64(-578575)},
		{int64(15), types.NewTimestamp(1014), "k3", int64(32323)},
	}
	// Group by one col
	aggExprs := []string{"sum(int_col)", "count(int_col)", "min(int_col)", "max(int_col)"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "sum(int_col)", "count(int_col)", "min(int_col)", "max(int_col)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", int64(221924338), int64(4), int64(-12314342), int64(234234234)},
		{types.NewTimestamp(1008), "k2", int64(-235298), int64(3), int64(-238732), int64(3434)},
		{types.NewTimestamp(1014), "k3", int64(-90704), int64(5), int64(-578575), int64(456456)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateWithInnerExprsInAggFuncs(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "dec_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString,
		types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), "k1", int64(2045), float64(123.45), createDecimal(t, "12345.54321")},
		{int64(2), types.NewTimestamp(1001), "k1", int64(1023), float64(3643.76), createDecimal(t, "46454.23484")},
		{int64(3), types.NewTimestamp(1002), "k1", int64(-4546), float64(-23.65), createDecimal(t, "-45644.32")},
		{int64(4), types.NewTimestamp(1003), "k1", int64(-45), float64(-6786.45), createDecimal(t, "-6767.34")},
		{int64(5), types.NewTimestamp(1004), "k1", int64(0), float64(0), createDecimal(t, "0")},

		{int64(6), types.NewTimestamp(1005), "k2", int64(4244), float64(543.78), createDecimal(t, "54324.5455")},
		{int64(7), types.NewTimestamp(1006), "k2", int64(2413), float64(69859.56), createDecimal(t, "6767656.363")},
		{int64(8), types.NewTimestamp(1007), "k2", int64(-57565), float64(-57.67), createDecimal(t, "-676584.68687")},
		{int64(9), types.NewTimestamp(1008), "k2", int64(-6866), float64(-54576.78), createDecimal(t, "-67868.28")},
		{int64(10), types.NewTimestamp(1009), "k2", int64(0), float64(0), createDecimal(t, "0")},
	}
	// Group by one col
	aggExprs := []string{"sum(int_col + 10)", "sum(float_col + 10f)", `sum(dec_col + to_decimal("10.00",38,6))`}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "sum(int_col + 10)", "sum(float_col + 10f)", `sum(dec_col + to_decimal("10.00",38,6))`}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeFloat, decType}
	outData := [][]any{
		{types.NewTimestamp(1004), "k1", int64(-1473), float64(-2992.89), createDecimal(t, "6438.11805")},
		{types.NewTimestamp(1009), "k2", int64(-57724), float64(15818.89), createDecimal(t, "6077577.94163")},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func TestAggregateWithColumnAliases(t *testing.T) {
	inColumnNames := []string{"offset", "event_time", "kc", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	inData := [][]any{
		{int64(1), types.NewTimestamp(1000), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(1001), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(1002), int64(10), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(1003), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(5), types.NewTimestamp(1004), int64(20), int64(1), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	// Group by one col
	aggExprs := []string{"count(int_col) as count_int", "count(float_col) as count_float", "count(bool_col) as count_bool", "count(dec_col) as count_dec", "count(string_col) as count_string",
		"count(bytes_col) as count_bytes", "count(ts_col) as count_ts"}
	keyExprs := []string{"kc"}
	outColumnNames := []string{"event_time", "kc", "count_int", "count_float", "count_bool", "count_dec", "count_string",
		"count_bytes", "count_ts"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt,
		types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeInt}
	outData := [][]any{
		{types.NewTimestamp(1002), int64(10), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3), int64(3)},
		{types.NewTimestamp(1004), int64(20), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2), int64(2)},
	}
	testAggregate(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData)
}

func testAggregate(t *testing.T, inColumnNames []string, inColumnTypes []types.ColumnType, aggExprs []string, keyExprs []string, inData [][]any,
	outColumnNames []string, outColumnTypes []types.ColumnType, outData [][]any) {
	testAggregateWithStoredData(t, inColumnNames, inColumnTypes, aggExprs, keyExprs, inData, outColumnNames, outColumnTypes, outData, nil)
}

func toExprs(exprStrs ...string) ([]parser.ExprDesc, error) {
	p := parser.NewParser(nil)
	var exprs []parser.ExprDesc
	for _, str := range exprStrs {
		tokens, err := parser.Lex(str, true)
		if err != nil {
			return nil, err
		}
		e, err := p.ParseExpression(parser.NewParseContext(p, str, tokens))
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}
	return exprs, nil

}

func testAggregateWithStoredData(t *testing.T, inColumnNames []string, inColumnTypes []types.ColumnType, aggExprStrs []string,
	keyExprStrs []string, inData [][]any, outColumnNames []string, outColumnTypes []types.ColumnType, outData [][]any, stored []common.KV) []common.KV {
	batch := createEventBatch(inColumnNames, inColumnTypes, inData)
	inSchema := evbatch.NewEventSchema(inColumnNames, inColumnTypes)
	tableID := 1001

	aggExprs, err := toExprs(aggExprStrs...)
	require.NoError(t, err)
	keyExprs, err := toExprs(keyExprStrs...)
	require.NoError(t, err)

	aggDesc := &parser.AggregateDesc{
		BaseDesc:             parser.BaseDesc{},
		AggregateExprs:       aggExprs,
		KeyExprs:             keyExprs,
		AggregateExprStrings: aggExprStrs,
		KeyExprsStrings:      keyExprStrs,
	}

	agg, err := NewAggregateOperator(&OperatorSchema{EventSchema: inSchema, PartitionScheme: PartitionScheme{MappingID: "mapping", Partitions: 200}}, aggDesc, tableID,
		-1, -1, -1, 0, 0, nil, 0, false, false,
		&expr.ExpressionFactory{})
	require.NoError(t, err)

	require.Equal(t, outColumnNames, agg.aggStateSchema.ColumnNames())
	require.Equal(t, outColumnTypes, agg.aggStateSchema.ColumnTypes())

	version := 12345
	partitionID := 123
	storedMap := map[string][]byte{}
	for _, kv := range stored {
		// We remove the version as we don't look up based on that
		storedMap[string(kv.Key[:len(kv.Key)-8])] = kv.Value
	}
	ctx := &testExecCtx{
		version:     version,
		partitionID: partitionID,
		stored:      storedMap,
	}
	outBatch, err := agg.HandleStreamBatch(batch, ctx)
	require.NoError(t, err)
	require.Nil(t, outBatch)

	verifyOutDataFromEntries(t, ctx.entries, outData, agg, tableID, partitionID, version)
	return ctx.entries
}

func verifyOutDataFromEntries(t *testing.T, entries []common.KV, outData [][]any, agg *AggregateOperator, tableID int, partitionID int, version int) {
	require.Equal(t, len(outData), len(entries))
	var actualOutData [][]any
	for _, entry := range entries {
		outValue, _ := encoding.DecodeRowToSlice(entry.Value, 0, agg.aggColTypes)
		outKey := make([]any, len(agg.keyColHolders))
		partHash := entry.Key[:16]
		tabID, _ := encoding.ReadUint64FromBufferBE(entry.Key, 16)
		ver, _ := encoding.ReadUint64FromBufferBE(entry.Key, len(entry.Key)-8)
		ver = math.MaxUint64 - ver
		require.Equal(t, tableID, int(tabID))
		expectedPartHash := proc.CalcPartitionHash(agg.outSchema.MappingID, uint64(partitionID))
		require.Equal(t, expectedPartHash, partHash)
		require.Equal(t, version, int(ver))
		key := entry.Key[24:]
		outKey, _, err := encoding.DecodeKeyToSlice(key, 0, agg.keyColTypes)
		require.NoError(t, err)
		actualOut := make([]any, len(agg.aggStateSchema.ColumnTypes()))
		for i, keyCol := range agg.keyColHolders {
			actualOut[keyCol.colIndex] = outKey[i]
		}
		for i, aggCol := range agg.aggFuncHolders {
			actualOut[aggCol.colIndex] = outValue[i]
		}
		actualOutData = append(actualOutData, actualOut)
	}
	// Now we need to sort the outdata by the key cols as it can come back in a different order for different key cols
	actualOutData = sortDataByKeyCols(actualOutData, agg.keyColIndexes, agg.keyColTypes)

	require.Equal(t, outData, actualOutData)
}

func createDecimal(t *testing.T, str string) types.Decimal {
	num, err := decimal128.FromString(str, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	require.NoError(t, err)
	return types.Decimal{
		Num:       num,
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
}
