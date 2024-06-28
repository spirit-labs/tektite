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
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestIsLittleEndian(t *testing.T) {
	require.True(t, IsLittleEndian)
}

func TestEncodeDecodeUint64sLittleEndianArch(t *testing.T) {
	setEndianness(t, true)
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func TestEncodeDecodeUint64sBigEndianArch(t *testing.T) {
	setEndianness(t, false)
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func testEncodeDecodeUint64s(t *testing.T, vals ...uint64) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint64(t, val)
	}
}

func testEncodeDecodeUint64(t *testing.T, val uint64) {
	t.Helper()
	buff := make([]byte, 0, 8)
	buff = AppendUint64ToBufferLE(buff, val)
	valRead, _ := ReadUint64FromBufferLE(buff, 0)
	require.Equal(t, val, valRead)
}

func TestEncodeDecodeUint32sLittleEndianArch(t *testing.T) {
	setEndianness(t, true)
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func TestEncodeDecodeUint32sBigEndianArch(t *testing.T) {
	setEndianness(t, false)
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func testEncodeDecodeUint32s(t *testing.T, vals ...uint32) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint32(t, val)
	}
}

func testEncodeDecodeUint32(t *testing.T, val uint32) {
	t.Helper()
	buff := make([]byte, 0, 4)
	buff = AppendUint32ToBufferLE(buff, val)
	valRead, _ := ReadUint32FromBufferLE(buff, 0)
	require.Equal(t, val, valRead)
}

func setEndianness(t *testing.T, endianness bool) {
	t.Helper()
	prev := IsLittleEndian
	t.Cleanup(func() {
		IsLittleEndian = prev
	})
	IsLittleEndian = endianness
}
