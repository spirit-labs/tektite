package encoding

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/types"
)

var littleEndian = binary.LittleEndian
var bigEndian = binary.BigEndian
var IsLittleEndian = isLittleEndian()

// Are we running on a machine with a little endian architecture?
func isLittleEndian() bool {
	val := uint64(123456)
	buffer := make([]byte, 0, 8)
	buffer = AppendUint64ToBufferLE(buffer, val)
	valRead := *(*uint64)(unsafe.Pointer(&buffer[0])) // nolint: gosec
	return val == valRead
}

func AppendUint64ToBufferBE(buffer []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(buffer, v)
}

func AppendUint64ToBufferLE(buffer []byte, v uint64) []byte {
	return binary.LittleEndian.AppendUint64(buffer, v)
}

func AppendFloat64ToBufferLE(buffer []byte, value float64) []byte {
	u := math.Float64bits(value)
	return AppendUint64ToBufferLE(buffer, u)
}

func AppendStringToBufferLE(buffer []byte, value string) []byte {
	buffPtr := AppendUint32ToBufferLE(buffer, uint32(len(value)))
	buffPtr = append(buffPtr, value...)
	return buffPtr
}

func AppendUint32ToBufferLE(buffer []byte, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(buffer, v)
}

func AppendBytesToBufferLE(buffer []byte, value []byte) []byte {
	buffPtr := AppendUint32ToBufferLE(buffer, uint32(len(value)))
	buffPtr = append(buffPtr, value...)
	return buffPtr
}

func AppendBoolToBuffer(buffer []byte, val bool) []byte {
	var b byte
	if val {
		b = 1
	}
	return append(buffer, b)
}

func AppendDecimalToBuffer(buffer []byte, val types.Decimal) []byte {
	buffer = AppendUint64ToBufferLE(buffer, val.Num.LowBits())
	return AppendUint64ToBufferLE(buffer, uint64(val.Num.HighBits()))
}

func ReadUint64FromBufferBE(buffer []byte, offset int) (uint64, int) {
	if !IsLittleEndian {
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset])), offset + 8
	}
	return bigEndian.Uint64(buffer[offset:]), offset + 8
}

func ReadUint64FromBufferLE(buffer []byte, offset int) (uint64, int) {
	if IsLittleEndian {
		// If architecture is little endian we can simply cast to a pointer
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset])), offset + 8
	}
	return littleEndian.Uint64(buffer[offset:]), offset + 8
}

func ReadUint32FromBufferLE(buffer []byte, offset int) (uint32, int) {

	if IsLittleEndian {
		// nolint: gosec
		return *(*uint32)(unsafe.Pointer(&buffer[offset])), offset + 4
	}
	return littleEndian.Uint32(buffer[offset:]), offset + 4
}

func ReadFloat64FromBufferLE(buffer []byte, offset int) (val float64, off int) {
	var u uint64
	u, offset = ReadUint64FromBufferLE(buffer, offset)
	val = math.Float64frombits(u)
	return val, offset
}

func ReadStringFromBufferLE(buffer []byte, offset int) (val string, off int) {
	lu, offset := ReadUint32FromBufferLE(buffer, offset)
	l := int(lu)
	str := common.ByteSliceToStringZeroCopy(buffer[offset : offset+l])
	offset += l
	return str, offset
}

func ReadBoolFromBuffer(buffer []byte, offset int) (bool, int) {
	b := buffer[offset] == 1
	return b, offset + 1
}

func ReadBytesFromBufferLE(buffer []byte, offset int) (val []byte, off int) {
	lu, offset := ReadUint32FromBufferLE(buffer, offset)
	l := int(lu)
	b := buffer[offset : offset+l]
	offset += l
	return b, offset
}

func ReadDecimalFromBuffer(buffer []byte, offset int) (types.Decimal, int) {
	var lo uint64
	lo, offset = ReadUint64FromBufferLE(buffer, offset)
	var hi uint64
	hi, offset = ReadUint64FromBufferLE(buffer, offset)
	return types.Decimal{
		Num: decimal128.New(int64(hi), lo),
	}, offset
}
