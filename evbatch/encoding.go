package evbatch

import (
	"fmt"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/types"
)

func EncodeRowCols(batch *Batch, rowIndex int, rowCols []int, buffer []byte) []byte {
	columnTypes := batch.Schema.ColumnTypes()
	for _, colIndex := range rowCols {
		col := batch.Columns[colIndex]
		if col.IsNull(rowIndex) {
			buffer = append(buffer, 0)
			continue
		}
		buffer = append(buffer, 1)
		ft := columnTypes[colIndex]
		switch ft.ID() {
		case types.ColumnTypeIDInt:
			val := (col.(*IntColumn)).Get(rowIndex)
			buffer = encoding.AppendUint64ToBufferLE(buffer, uint64(val))
		case types.ColumnTypeIDFloat:
			val := (col.(*FloatColumn)).Get(rowIndex)
			buffer = encoding.AppendFloat64ToBufferLE(buffer, val)
		case types.ColumnTypeIDBool:
			val := (col.(*BoolColumn)).Get(rowIndex)
			buffer = encoding.AppendBoolToBuffer(buffer, val)
		case types.ColumnTypeIDDecimal:
			val := (col.(*DecimalColumn)).Get(rowIndex)
			buffer = encoding.AppendDecimalToBuffer(buffer, val)
		case types.ColumnTypeIDString:
			val := (col.(*StringColumn)).Get(rowIndex)
			buffer = encoding.AppendStringToBufferLE(buffer, val)
		case types.ColumnTypeIDBytes:
			val := (col.(*BytesColumn)).Get(rowIndex)
			buffer = encoding.AppendBytesToBufferLE(buffer, val)
		case types.ColumnTypeIDTimestamp:
			val := (col.(*TimestampColumn)).Get(rowIndex)
			buffer = encoding.AppendUint64ToBufferLE(buffer, uint64(val.Val))
		default:
			panic(fmt.Sprintf("unexpected column type %d", ft))
		}
	}
	return buffer
}

func EncodeKeyCols(evBatch *Batch, rowIndex int, colIndexes []int, buffer []byte) []byte {
	columnTypes := evBatch.Schema.columnTypes
	for _, colIndex := range colIndexes {
		colType := columnTypes[colIndex]
		col := evBatch.Columns[colIndex]
		buffer = EncodeKeyCol(rowIndex, col, colType, buffer)
	}
	return buffer
}

func EncodeKeyCol(rowIndex int, col Column, colType types.ColumnType, buffer []byte) []byte {
	if col.IsNull(rowIndex) {
		return append(buffer, 0)
	} else {
		buffer = append(buffer, 1)
	}
	// Key columns must be stored in big-endian so whole key can be compared byte-wise
	switch colType.ID() {
	case types.ColumnTypeIDInt:
		// We store as unsigned so convert signed to unsigned
		val := col.(*IntColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeInt(buffer, val)
	case types.ColumnTypeIDFloat:
		val := col.(*FloatColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeFloat(buffer, val)
	case types.ColumnTypeIDBool:
		val := col.(*BoolColumn).Get(rowIndex)
		buffer = encoding.AppendBoolToBuffer(buffer, val)
	case types.ColumnTypeIDDecimal:
		val := col.(*DecimalColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeDecimal(buffer, val)
	case types.ColumnTypeIDString:
		val := col.(*StringColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeString(buffer, val)
	case types.ColumnTypeIDBytes:
		val := col.(*BytesColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeBytes(buffer, val)
	case types.ColumnTypeIDTimestamp:
		val := col.(*TimestampColumn).Get(rowIndex)
		buffer = encoding.KeyEncodeTimestamp(buffer, val)
	default:
		panic(fmt.Sprintf("unexpected column type %d", colType))
	}
	return buffer
}
