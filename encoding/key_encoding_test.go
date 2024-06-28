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

package encoding

import (
	"bytes"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestKeyEncodeIntOrdering(t *testing.T) {
	vals := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		math.MinInt64 + 1000,
		-1000,
		-1,
		0,
		1,
		1000,
		math.MaxInt64 - 1000,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeInt(vals[i]), encodeInt(vals[i+1]))
	}
}

func encodeInt(val int64) []byte {
	return KeyEncodeInt([]byte{}, val)
}

func TestKeyEncodeFloatOrdering(t *testing.T) {
	vals := []float64{
		-math.MaxFloat64,
		-1.234e10,
		-1e3,
		-1.1,
		-1.0,
		-0.5,
		0.0,
		0.5,
		1.0,
		1.1,
		1e3,
		1.234e10,
		math.MaxFloat64,
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeFloat(vals[i]), encodeFloat(vals[i+1]))
	}
}

func encodeFloat(val float64) []byte {
	return KeyEncodeFloat([]byte{}, val)
}

func TestKeyEncodeBoolOrdering(t *testing.T) {
	b1 := AppendBoolToBuffer([]byte{}, false)
	b2 := AppendBoolToBuffer([]byte{}, true)
	checkLessThan(t, b1, b2)
}

func TestKeyEncodeStringOrdering(t *testing.T) {
	vals := []string{
		"",
		"a",
		"aa",
		"aaa",
		"aaaa",
		"aab",
		"ab",
		"abb",
		"antelopes",
		"b",
		"z",
		"zzz",
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeString(vals[i]), encodeString(vals[i+1]))
	}
}

func encodeString(val string) []byte {
	return KeyEncodeString([]byte{}, val)
}

func TestEncodedDecimalOrdering(t *testing.T) {
	vals := []types.Decimal{
		createDec(math.MinInt64, 0),
		createDec(math.MinInt64, 1254124),
		createDec(-62536253, 354343),
		createDec(-1, 0),
		createDec(-1, 232),
		createDec(1, 32434),
		createDec(2376376, 37363),
		createDec(math.MaxInt64, 365353),
		createDec(math.MaxInt64, math.MaxUint64),
	}
	for i := 0; i < len(vals)-1; i++ {
		dec1 := vals[i]
		dec2 := vals[i+1]
		b1 := encodeDecimal(dec1)
		b2 := encodeDecimal(dec2)
		require.True(t, dec2.Num.Greater(dec1.Num))
		checkLessThan(t, b1, b2)
	}
}

func encodeDecimal(val types.Decimal) []byte {
	return KeyEncodeDecimal([]byte{}, val)
}

func TestEncodedBytesOrdering(t *testing.T) {
	vals := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("aa"),
		[]byte("aaa"),
		[]byte("aaaa"),
		[]byte("aab"),
		[]byte("ab"),
		[]byte("abb"),
		[]byte("antelopes"),
		[]byte("b"),
		[]byte("z"),
		[]byte("zzz"),
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeBytes(vals[i]), encodeBytes(vals[i+1]))
	}
}

func encodeBytes(val []byte) []byte {
	return KeyEncodeBytes([]byte{}, val)
}

func TestEncodedTimestampOrdering(t *testing.T) {
	vals := []int64{
		0,
		1,
		1000,
		math.MaxInt64 - 1000,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}
	for i := 0; i < len(vals)-1; i++ {
		ts1 := types.NewTimestamp(vals[i])
		ts2 := types.NewTimestamp(vals[i+1])
		b1 := KeyEncodeTimestamp(nil, ts1)
		b2 := KeyEncodeTimestamp(nil, ts2)
		checkLessThan(t, b1, b2)
	}
}

func TestKeyEncodeDecodeInt(t *testing.T) {
	ints := []int64{math.MinInt64, -165213365, -1, 0, 1, 6152652, math.MaxInt64}
	var buff []byte
	for _, i := range ints {
		buff = KeyEncodeInt(buff, i)
	}
	offset := 0
	for _, i := range ints {
		var a int64
		a, offset = KeyDecodeInt(buff, offset)
		require.Equal(t, i, a)
	}
}

func TestKeyEncodeDecodeFloat(t *testing.T) {
	floats := []float64{math.MinInt64, -165213365, -1, 0, 1, 6152652, math.MaxInt64}
	var buff []byte
	for _, f := range floats {
		buff = KeyEncodeFloat(buff, f)
	}
	offset := 0
	for _, f := range floats {
		var a float64
		a, offset = KeyDecodeFloat(buff, offset)
		require.Equal(t, f, a)
	}
}

func TestKeyEncodeDecodeBool(t *testing.T) {
	bools := []bool{false, true, true, false, false, true, false, true}
	var buff []byte
	for _, b := range bools {
		buff = AppendBoolToBuffer(buff, b)
	}
	offset := 0
	for _, b := range bools {
		var a bool
		a, offset = DecodeBool(buff, offset)
		require.Equal(t, b, a)
	}
}

func TestKeyEncodeDecodeDecimal(t *testing.T) {
	decs := []types.Decimal{
		createDec(math.MinInt64, 0),
		createDec(math.MinInt64, 1254124),
		createDec(-62536253, 354343),
		createDec(-1, 0),
		createDec(-1, 232),
		createDec(1, 32434),
		createDec(2376376, 37363),
		createDec(math.MaxInt64, 365353),
		createDec(math.MaxInt64, math.MaxUint64),
	}
	var buff []byte
	for _, d := range decs {
		buff = KeyEncodeDecimal(buff, d)
	}
	offset := 0
	for _, b := range decs {
		var a types.Decimal
		a, offset = KeyDecodeDecimal(buff, offset)
		a.Precision = 32
		a.Scale = 6
		require.Equal(t, b, a)
	}
}

func TestKeyEncodeDecodeString(t *testing.T) {
	vals := []string{
		"",
		"a",
		"aa",
		"aaa",
		"aaaa",
		"aab",
		"ab",
		"abb",
		"antelopes",
		"b",
		"z",
		"zzz",
	}
	var buff []byte
	for _, s := range vals {
		buff = KeyEncodeString(buff, s)
	}
	offset := 0
	for _, b := range vals {
		var a string
		var err error
		a, offset, err = KeyDecodeString(buff, offset)
		require.NoError(t, err)
		require.Equal(t, b, a)
	}
}

func TestKeyEncodeDecodeBytes(t *testing.T) {
	vals := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("aa"),
		[]byte("aaa"),
		[]byte("aaaa"),
		[]byte("aab"),
		[]byte("ab"),
		[]byte("abb"),
		[]byte("antelopes"),
		[]byte("b"),
		[]byte("z"),
		[]byte("zzz"),
	}
	var buff []byte
	for _, b := range vals {
		buff = KeyEncodeBytes(buff, b)
	}
	offset := 0
	for _, b := range vals {
		var a []byte
		var err error
		a, offset, err = KeyDecodeBytes(buff, offset)
		require.NoError(t, err)
		require.Equal(t, b, a)
	}
}

func TestKeyEncodeDecodeTimestamp(t *testing.T) {
	vals := []int64{
		0,
		1,
		1000,
		math.MaxInt64 - 1000,
		math.MaxInt64 - 1,
		math.MaxInt64,
		-1000, // -ve timestamps!
	}
	var buff []byte
	for _, v := range vals {
		buff = KeyEncodeTimestamp(buff, types.NewTimestamp(v))
	}
	offset := 0
	for _, b := range vals {
		var a types.Timestamp
		a, offset = KeyDecodeTimestamp(buff, offset)
		require.Equal(t, types.NewTimestamp(b), a)
	}
}

func createDec(hi int64, lo uint64) types.Decimal {
	return types.Decimal{
		Num:       decimal128.New(hi, lo),
		Precision: 32,
		Scale:     6,
	}
}

func checkLessThan(t *testing.T, b1, b2 []byte) {
	t.Helper()
	diff := bytes.Compare(b1, b2)
	require.Equal(t, -1, diff, "expected %x < %x", b1, b2)
}

func TestStringKeyEncodeDecode(t *testing.T) {
	strs := generateRandomStrings(100)
	for _, str := range strs {
		testStringKeyEncodeDecode(t, str)
	}
}

func testStringKeyEncodeDecode(t *testing.T, str string) {
	t.Helper()
	buff := KeyEncodeString([]byte{}, str)
	res, offset, err := KeyDecodeString(buff, 0)
	require.NoError(t, err)
	require.Equal(t, len(buff), offset)
	require.Equal(t, str, res)
}

func TestStringKeyEncodeDecodeExistingBuffer(t *testing.T) {
	strs := generateRandomStrings(100)
	for _, str := range strs {
		testStringKeyEncodeDecodeExistingBuffer(t, str)
	}
}

func testStringKeyEncodeDecodeExistingBuffer(t *testing.T, str string) {
	t.Helper()
	buff := KeyEncodeString([]byte{}, str)
	buff = append([]byte("aardvarks"), buff...)
	off := len(buff)
	buff = append(buff, []byte("antelopes")...)
	res, offset, err := KeyDecodeString(buff, 9)
	require.NoError(t, err)
	require.Equal(t, off, offset)
	require.Equal(t, "antelopes", string(buff[off:]))
	require.Equal(t, str, res)
}

func TestBinaryOrdering(t *testing.T) {
	strs := generateRandomStrings(100)

	strsCopy := make([]string, len(strs))
	copy(strsCopy, strs)

	// First sort the strings in lexographic order
	sort.Strings(strs)

	// Encode the strings into []byte
	encodedStrs := make([][]byte, len(strs))
	for i, str := range strsCopy {
		encodedStrs[i] = KeyEncodeString([]byte{}, str)
	}

	// Then sort them in binary order
	sort.Slice(encodedStrs, func(i, j int) bool { return bytes.Compare(encodedStrs[i], encodedStrs[j]) < 0 })

	// Now decode them
	decodedStrs := make([]string, len(strs))
	for i, b := range encodedStrs {
		var err error
		decodedStrs[i], _, err = KeyDecodeString(b, 0)
		require.NoError(t, err)
	}

	// They should be in the same order as the lexographically sorted strings
	for i := 0; i < len(strs); i++ {
		require.Equal(t, strs[i], decodedStrs[i])
	}
}

func generateRandomStrings(upToLen int) []string {
	res := make([]string, upToLen)
	for i := 0; i < upToLen; i++ {
		res[i] = generateRandomString(i)
	}
	return res
}

var alpha = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func generateRandomString(length int) string {
	runes := make([]rune, length)
	for i := 0; i < length; i++ {
		runes[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(runes)
}
