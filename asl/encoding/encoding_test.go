package encoding

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
