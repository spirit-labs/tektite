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

func TestAppendUint32ToBufferLEAsVarInt(t *testing.T) {
	buffer := []byte{0x01, 0x02}
	buffer = AppendUint32ToBufferLEAsVarInt(buffer, 3)
	require.Len(t, buffer, 3)

	buffer = AppendUint32ToBufferLEAsVarInt(buffer, 257)
	require.Len(t, buffer, 5)

	require.True(t, buffer[4]&0x80 == 0x00)
	require.True(t, buffer[3]&0x80 == 0x80)
}

func TestReadUint32FromBufferLEVarInt(t *testing.T) {
	buffer := []byte{}
	buffer = AppendUint32ToBufferLEAsVarInt(buffer, 3)
	buffer = AppendUint32ToBufferLEAsVarInt(buffer, 257)
	value1, off := ReadUint32FromBufferLEVarInt(buffer, 0)
	require.Equal(t, value1, uint32(3))
	value2, off := ReadUint32FromBufferLEVarInt(buffer, off)
	require.Equal(t, value2, uint32(257))
	require.Equal(t, off, 3) // end of stream

}
