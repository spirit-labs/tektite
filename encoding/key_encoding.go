package encoding

import (
	"encoding/binary"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/types"
	"math"
)

const (
	SignBitMask  uint64 = 1 << 63
	encGroupSize        = 8
	encMarker    byte   = 255
	encPad       byte   = 0
)

var stringKeyEncodingPads = make([]byte, encGroupSize)

func DecodeKeyToSlice(buffer []byte, offset int, columnTypes []types.ColumnType) ([]any, int, error) {
	key := make([]any, len(columnTypes))
	for i, keyColType := range columnTypes {
		isNull := buffer[offset] == 0
		offset++
		if isNull {
			continue
		}
		switch keyColType.ID() {
		case types.ColumnTypeIDInt:
			key[i], offset = KeyDecodeInt(buffer, offset)
		case types.ColumnTypeIDFloat:
			key[i], offset = KeyDecodeFloat(buffer, offset)
		case types.ColumnTypeIDBool:
			key[i], offset = DecodeBool(buffer, offset)
		case types.ColumnTypeIDDecimal:
			decType := keyColType.(*types.DecimalType)
			var dec types.Decimal
			dec, offset = KeyDecodeDecimal(buffer, offset)
			dec.Precision = decType.Precision
			dec.Scale = decType.Scale
			key[i] = dec
		case types.ColumnTypeIDString:
			var err error
			key[i], offset, err = KeyDecodeString(buffer, offset)
			if err != nil {
				return nil, 0, err
			}
		case types.ColumnTypeIDBytes:
			var err error
			key[i], offset, err = KeyDecodeBytes(buffer, offset)
			if err != nil {
				return nil, 0, err
			}
		case types.ColumnTypeIDTimestamp:
			key[i], offset = KeyDecodeTimestamp(buffer, offset)
		default:
			panic("unknown type")
		}
	}
	return key, offset, nil
}

func KeyEncodeInt(buffer []byte, val int64) []byte {
	uVal := uint64(val) ^ SignBitMask
	return AppendUint64ToBufferBE(buffer, uVal)
}

func KeyDecodeInt(buffer []byte, offset int) (int64, int) {
	u, offset := ReadUint64FromBufferBE(buffer, offset)
	return int64(u ^ SignBitMask), offset
}

func KeyEncodeFloat(buffer []byte, val float64) []byte {
	uVal := math.Float64bits(val)
	if val >= 0 {
		uVal |= SignBitMask
	} else {
		uVal = ^uVal
	}
	return AppendUint64ToBufferBE(buffer, uVal)
}

func KeyDecodeFloat(buffer []byte, offset int) (float64, int) {
	u, offset := ReadUint64FromBufferBE(buffer, offset)
	if u&SignBitMask == SignBitMask {
		//+ve
		u &= ^SignBitMask
	} else {
		u = ^u
	}
	return math.Float64frombits(u), offset
}

func DecodeBool(buffer []byte, offset int) (bool, int) {
	b := buffer[offset]
	return b == 1, offset + 1
}

func KeyEncodeDecimal(buffer []byte, val types.Decimal) []byte {
	buffer = KeyEncodeInt(buffer, val.Num.HighBits())
	return AppendUint64ToBufferBE(buffer, val.Num.LowBits())
}

func KeyDecodeDecimal(buffer []byte, offset int) (types.Decimal, int) {
	var hi int64
	hi, offset = KeyDecodeInt(buffer, offset)
	var lo uint64
	lo, offset = ReadUint64FromBufferBE(buffer, offset)
	return types.Decimal{
		Num: decimal128.New(hi, lo),
	}, offset
}

func KeyEncodeBytes(buffer []byte, val []byte) []byte {
	return KeyEncodeString(buffer, common.ByteSliceToStringZeroCopy(val))
}

func KeyDecodeBytes(buffer []byte, offset int) ([]byte, int, error) {
	s, off, err := KeyDecodeString(buffer, offset)
	if err != nil {
		return nil, 0, err
	}
	if len(s) == 0 {
		return []byte{}, off, nil
	}
	return common.StringToByteSliceZeroCopy(s), off, nil
}

func KeyEncodeTimestamp(buffer []byte, val types.Timestamp) []byte {
	return KeyEncodeInt(buffer, val.Val)
}

func KeyDecodeTimestamp(buffer []byte, offset int) (types.Timestamp, int) {
	v, off := KeyDecodeInt(buffer, offset)
	return types.NewTimestamp(v), off
}

/*
KeyEncodeString
We encode a binary string in the following way:
We split it into chunks of 8 bytes, and after each chunk append a byte which has a value from 1-8 depending on how
many significant bytes there were in the previous chunk. Final chunk is right padded out with zeros to 8 bytes.
*/
func KeyEncodeString(buff []byte, val string) []byte {

	data := common.StringToByteSliceZeroCopy(val)
	dLen := len(data)

	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			buff = append(buff, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			buff = append(buff, data[idx:]...)
			buff = append(buff, stringKeyEncodingPads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		buff = append(buff, marker)
	}
	return buff
}

func KeyDecodeString(buffer []byte, offset int) (string, int, error) {
	res := make([]byte, 0, len(buffer))

	if offset != 0 {
		buffer = buffer[offset:]
	}

	for {
		if len(buffer) < encGroupSize+1 {
			return "", 0, errors.New("insufficient bytes to decode value")
		}

		groupBytes := buffer[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		padCount := encMarker - marker

		if padCount > encGroupSize {
			return "", 0, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		res = append(res, group[:realGroupSize]...)
		buffer = buffer[encGroupSize+1:]
		offset += encGroupSize + 1

		if padCount != 0 {
			var padByte = encPad

			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return "", 0, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return common.ByteSliceToStringZeroCopy(res), offset, nil
}

func EncodeEntryPrefix(partitionHash []byte, slabID uint64, capac int) []byte {
	keyBuff := make([]byte, 24, capac)
	copy(keyBuff, partitionHash)
	binary.BigEndian.PutUint64(keyBuff[16:], slabID)
	return keyBuff
}

func EncodeVersion(key []byte, version uint64) []byte {
	// The version is appended onto the end of the key
	// We store the version inverted so that higher versions for the same key appear before lower versions
	// this is important when iterating, so we iterate over the highest versions first (which is usually the one we want)
	return AppendUint64ToBufferBE(key, math.MaxUint64-version)
}
