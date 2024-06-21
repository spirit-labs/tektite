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
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSum(t *testing.T) {
	saf := &SumAggFunc{}
	res, _, err := saf.ComputeInt(int64(10), nil, []int64{5, -6, 7, -8, 9})
	require.NoError(t, err)
	require.Equal(t, int64(17), res.(int64))

	res, _, err = saf.ComputeFloat(float64(11.2), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, float64(33.599999999999994), res.(float64))

	res, _, err = saf.ComputeDecimal(createDecimal(t, "12.12"), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "-456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "-499.627"), res.(types.Decimal))
}

func TestCount(t *testing.T) {
	caf := &CountAggFunc{}
	res, _, err := caf.ComputeInt(int64(10), nil, []int64{5, -6, 7, -8, 9})
	require.NoError(t, err)
	require.Equal(t, int64(15), res.(int64))

	res, _, err = caf.ComputeFloat(int64(10), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, int64(15), res.(int64))

	res, _, err = caf.ComputeBool(int64(10), nil, []bool{true, true, false, true, false, false})
	require.NoError(t, err)
	require.Equal(t, int64(16), res.(int64))

	res, _, err = caf.ComputeDecimal(int64(10), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "-456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(15), res.(int64))

	res, _, err = caf.ComputeString(int64(10), nil, []string{"a", "b", "c", "d", "e"})
	require.NoError(t, err)
	require.Equal(t, int64(15), res.(int64))

	res, _, err = caf.ComputeBytes(int64(10), nil, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	require.NoError(t, err)
	require.Equal(t, int64(15), res.(int64))

	res, _, err = caf.ComputeTimestamp(int64(10), nil, []types.Timestamp{types.NewTimestamp(2), types.NewTimestamp(12), types.NewTimestamp(22)})
	require.NoError(t, err)
	require.Equal(t, int64(13), res.(int64))
}

func TestMin(t *testing.T) {
	maf := &MinAggFunc{}
	res, _, err := maf.ComputeInt(int64(10), nil, []int64{-12, 0, 10, 13, 2})
	require.NoError(t, err)
	require.Equal(t, int64(-12), res.(int64))

	res, _, err = maf.ComputeInt(int64(-100), nil, []int64{-12, 0, 10, 13, 2})
	require.NoError(t, err)
	require.Equal(t, int64(-100), res.(int64))

	res, _, err = maf.ComputeFloat(float64(11.2), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, float64(-7.1), res.(float64))

	res, _, err = maf.ComputeFloat(float64(-11.2), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, float64(-11.2), res.(float64))

	res, _, err = maf.ComputeDecimal(createDecimal(t, "12.12"), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "-456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "-456.45"), res.(types.Decimal))

	res, _, err = maf.ComputeDecimal(createDecimal(t, "-36353.43"), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "-456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "-36353.43"), res.(types.Decimal))

	res, _, err = maf.ComputeString("abc", nil, []string{"bdfdg", "hdgd", "djd", "", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "", res.(string))
	res, _, err = maf.ComputeString("abc", nil, []string{"bdfdg", "hdgd", "djd", "yre", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "abc", res.(string))
	res, _, err = maf.ComputeString("abc", nil, []string{"aaa", "hdgd", "djd", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "aaa", res.(string))

	res, _, err = maf.ComputeBytes([]byte("abc"), nil, [][]byte{[]byte("bdfdg"), []byte("hdgd"), []byte("djd"), {}, []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte{}, res.([]byte))
	res, _, err = maf.ComputeBytes([]byte("abc"), nil, [][]byte{[]byte("bdfdg"), []byte("hdgd"), []byte("djd"), []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), res.([]byte))
	res, _, err = maf.ComputeBytes([]byte("zsf"), nil, [][]byte{[]byte("bdfdg"), []byte("hdgd"), []byte("djd"), []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte("bdfdg"), res.([]byte))

	res, _, err = maf.ComputeTimestamp(types.NewTimestamp(123), nil,
		[]types.Timestamp{
			types.NewTimestamp(213), types.NewTimestamp(12), types.NewTimestamp(2), types.NewTimestamp(1233),
		})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(2), res.(types.Timestamp))
	res, _, err = maf.ComputeTimestamp(types.NewTimestamp(1), nil,
		[]types.Timestamp{
			types.NewTimestamp(213), types.NewTimestamp(12), types.NewTimestamp(2), types.NewTimestamp(1233),
		})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(1), res.(types.Timestamp))
}

func TestMax(t *testing.T) {
	maf := &MaxAggFunc{}
	res, _, err := maf.ComputeInt(int64(10), nil, []int64{-12, 0, 10, 13, 2})
	require.NoError(t, err)
	require.Equal(t, int64(13), res.(int64))

	res, _, err = maf.ComputeInt(int64(100), nil, []int64{-12, 0, 10, 13, 2})
	require.NoError(t, err)
	require.Equal(t, int64(100), res.(int64))

	res, _, err = maf.ComputeFloat(float64(1.2), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, float64(9.4), res.(float64))

	res, _, err = maf.ComputeFloat(float64(123.1), nil, []float64{5.1, 6.3, -7.1, 8.7, 9.4})
	require.NoError(t, err)
	require.Equal(t, float64(123.1), res.(float64))

	res, _, err = maf.ComputeDecimal(createDecimal(t, "12.12"), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "456.45"), res.(types.Decimal))

	res, _, err = maf.ComputeDecimal(createDecimal(t, "36353.43"), nil, []types.Decimal{
		createDecimal(t, "23.213"),
		createDecimal(t, "-456.45"),
		createDecimal(t, "11.34"),
		createDecimal(t, "233.45"),
		createDecimal(t, "-323.3"),
	})
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "36353.43"), res.(types.Decimal))

	res, _, err = maf.ComputeString("abc", nil, []string{"bdfdg", "hdgd", "djd", "", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "zzz", res.(string))
	res, _, err = maf.ComputeString("zzzz", nil, []string{"bdfdg", "hdgd", "djd", "yre", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "zzzz", res.(string))
	res, _, err = maf.ComputeString("abc", nil, []string{"zzzzz", "hdgd", "djd", "zzz"})
	require.NoError(t, err)
	require.Equal(t, "zzzzz", res.(string))

	res, _, err = maf.ComputeBytes([]byte("abc"), nil, [][]byte{[]byte("bdfdg"), []byte("hdgd"), []byte("djd"), {}, []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte("zzz"), res.([]byte))
	res, _, err = maf.ComputeBytes([]byte("zzzz"), nil, [][]byte{[]byte("bdfdg"), []byte("hdgd"), []byte("djd"), []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte("zzzz"), res.([]byte))
	res, _, err = maf.ComputeBytes([]byte("zsf"), nil, [][]byte{[]byte("zzzzz"), []byte("hdgd"), []byte("djd"), []byte("zzz")})
	require.NoError(t, err)
	require.Equal(t, []byte("zzzzz"), res.([]byte))

	res, _, err = maf.ComputeTimestamp(types.NewTimestamp(123), nil,
		[]types.Timestamp{
			types.NewTimestamp(213), types.NewTimestamp(12), types.NewTimestamp(2), types.NewTimestamp(1233),
		})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(1233), res.(types.Timestamp))
	res, _, err = maf.ComputeTimestamp(types.NewTimestamp(3333), nil,
		[]types.Timestamp{
			types.NewTimestamp(213), types.NewTimestamp(12), types.NewTimestamp(2), types.NewTimestamp(1233),
		})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(3333), res.(types.Timestamp))
}

func TestAvg(t *testing.T) {
	avg := &AvgAggFunc{}
	require.True(t, avg.RequiresExtraData())

	res, extra, err := avg.ComputeInt(nil, nil, []int64{10, 11, 12, 13, 14, 15})
	require.NoError(t, err)
	require.Equal(t, float64(12.5), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeInt(nil, extra, []int64{16, 17, 18, 19, 20, 21})
	require.NoError(t, err)
	require.Equal(t, float64(15.5), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeFloat(nil, nil, []float64{1.1, 1.2, 1.3, 1.4, 1.5})
	require.NoError(t, err)
	require.Equal(t, float64(1.3), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeFloat(nil, extra, []float64{1.6, 1.7, 1.8, 1.9, 2.0})
	require.NoError(t, err)
	require.Equal(t, float64(1.55), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeTimestamp(nil, nil, []types.Timestamp{
		types.NewTimestamp(1000),
		types.NewTimestamp(1001),
		types.NewTimestamp(1002),
		types.NewTimestamp(1003),
		types.NewTimestamp(1004),
	})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(1002), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeTimestamp(nil, extra, []types.Timestamp{
		types.NewTimestamp(1005),
		types.NewTimestamp(1006),
		types.NewTimestamp(1007),
		types.NewTimestamp(1008),
	})
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(1004), res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeDecimal(nil, nil, []types.Decimal{
		createDecimal(t, "1000.1234"),
		createDecimal(t, "1001.1234"),
		createDecimal(t, "1002.1234"),
		createDecimal(t, "1003.1234"),
		createDecimal(t, "1004.1234"),
	})
	require.NoError(t, err)
	numExpected, err := decimal128.FromString("1002.1234", types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	require.NoError(t, err)
	expected := types.Decimal{
		Num:       numExpected,
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	require.Equal(t, expected, res)
	require.NotNil(t, extra)

	res, extra, err = avg.ComputeDecimal(nil, extra, []types.Decimal{
		createDecimal(t, "1005.1234"),
		createDecimal(t, "1006.1234"),
		createDecimal(t, "1007.1234"),
		createDecimal(t, "1008.1234"),
	})
	require.NoError(t, err)
	numExpected, err = decimal128.FromString("1004.1234", types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	require.NoError(t, err)
	expected = types.Decimal{
		Num:       numExpected,
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	require.Equal(t, expected, res)
	require.NotNil(t, extra)
}
