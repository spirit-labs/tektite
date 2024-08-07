package encoding

import (
	"fmt"
	"github.com/spirit-labs/tektite/types"
)

func DecodeRowToSlice(buffer []byte, offset int, columnTypes []types.ColumnType) ([]any, int) {
	row := make([]any, len(columnTypes))
	for i, colType := range columnTypes {
		if buffer[offset] == 0 {
			offset++
		} else {
			offset++
			var val any
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				var u uint64
				u, offset = ReadUint64FromBufferLE(buffer, offset)
				val = int64(u)
			case types.ColumnTypeIDFloat:
				val, offset = ReadFloat64FromBufferLE(buffer, offset)
			case types.ColumnTypeIDBool:
				val, offset = ReadBoolFromBuffer(buffer, offset)
			case types.ColumnTypeIDDecimal:
				decType := colType.(*types.DecimalType)
				var dec types.Decimal
				dec, offset = ReadDecimalFromBuffer(buffer, offset)
				dec.Precision = decType.Precision
				dec.Scale = decType.Scale
				val = dec
			case types.ColumnTypeIDString:
				val, offset = ReadStringFromBufferLE(buffer, offset)
			case types.ColumnTypeIDBytes:
				val, offset = ReadBytesFromBufferLE(buffer, offset)
			case types.ColumnTypeIDTimestamp:
				var u uint64
				u, offset = ReadUint64FromBufferLE(buffer, offset)
				val = types.NewTimestamp(int64(u))
			default:
				panic(fmt.Sprintf("unexpected column type %d", colType))
			}
			row[i] = val
		}
	}
	return row, offset
}
