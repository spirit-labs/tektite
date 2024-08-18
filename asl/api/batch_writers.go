package api

import (
	"encoding/binary"
	"encoding/json"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/types"
	"net/http"
)

type BatchWriter interface {
	WriteHeaders(columnNames []string, columnTypes []types.ColumnType, writer http.ResponseWriter) error
	WriteBatch(batch *evbatch.Batch, writer http.ResponseWriter) error
	WriteContentType(writer http.ResponseWriter)
}

// Each row is written as a JSON array separated by new line
type jsonLinesBatchWriter struct {
}

func (j *jsonLinesBatchWriter) WriteContentType(writer http.ResponseWriter) {
	// We use text/plain as it allows browsers to display it easily
	writer.Header().Add("Content-Type", "text/plain")
}

func (j *jsonLinesBatchWriter) WriteHeaders(columnNames []string, columnTypes []types.ColumnType, writer http.ResponseWriter) error {
	fTypesStr := make([]any, len(columnTypes))
	for i, fType := range columnTypes {
		ctName := fType.String()
		fTypesStr[i] = ctName
	}
	if err := j.writeRow(columnNames, writer); err != nil {
		return err
	}
	return j.writeRow(fTypesStr, writer)
}

func (j *jsonLinesBatchWriter) WriteBatch(batch *evbatch.Batch, writer http.ResponseWriter) error {
	columnTypes := batch.Schema.ColumnTypes()
	columnCount := len(columnTypes)
	arr := make([]interface{}, columnCount)
	for i := 0; i < batch.RowCount; i++ {
		for j, fType := range columnTypes {
			col := batch.Columns[j]
			var val interface{}
			if col.IsNull(i) {
				val = nil
			} else {
				switch fType.ID() {
				case types.ColumnTypeIDInt:
					val = col.(*evbatch.IntColumn).Get(i)
				case types.ColumnTypeIDFloat:
					val = col.(*evbatch.FloatColumn).Get(i)
				case types.ColumnTypeIDBool:
					val = col.(*evbatch.BoolColumn).Get(i)
				case types.ColumnTypeIDDecimal:
					// decimals are converted to strings to preserve precision
					d := col.(*evbatch.DecimalColumn).Get(i)
					val = d.Num.ToString(int32(d.Scale))
				case types.ColumnTypeIDString:
					val = col.(*evbatch.StringColumn).Get(i)
				case types.ColumnTypeIDBytes:
					// bytes are converted to strings
					val = string(col.(*evbatch.BytesColumn).Get(i))
				case types.ColumnTypeIDTimestamp:
					// timestamps are converted to unix millis past epoch
					val = col.(*evbatch.TimestampColumn).Get(i).Val
				default:
					panic("unknown type")
				}
			}
			arr[j] = val
		}
		if err := j.writeRow(arr, writer); err != nil {
			return err
		}
	}
	return nil
}

func (j *jsonLinesBatchWriter) writeRow(row any, writer http.ResponseWriter) error {
	bytes, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	_, err = writer.Write(newLine)
	return err
}

const TektiteArrowMimeType = "x-tektite-arrow"

type ArrowBatchWriter struct {
}

func (b *ArrowBatchWriter) WriteHeaders(columnNames []string, columnTypes []types.ColumnType, writer http.ResponseWriter) error {
	buff := make([]byte, 0, 64)
	buff = append(buff, 0, 0, 0, 0, 0, 0, 0, 0) // Leave first 8 bytes free for overall headers length
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(columnNames)))
	for i, fName := range columnNames {
		fType := columnTypes[i]
		buff = encoding.AppendStringToBufferLE(buff, fName)
		id := fType.ID()
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(id))
		if id == types.ColumnTypeIDDecimal {
			dt := fType.(*types.DecimalType)
			buff = encoding.AppendUint32ToBufferLE(buff, uint32(dt.Precision))
			buff = encoding.AppendUint32ToBufferLE(buff, uint32(dt.Scale))
		}
	}
	binary.LittleEndian.PutUint64(buff, uint64(len(buff)-8))
	_, err := writer.Write(buff)
	return err
}

func (b *ArrowBatchWriter) WriteBatch(batch *evbatch.Batch, writer http.ResponseWriter) error {
	buffs := batch.ToBytes()

	// compute the overall number of bytes to write - this number must be written first as we will wait for this
	// many bytes when reading (the total number does not include the 8 bytes total number written itself)
	totBytes := 8 + 4
	for _, buff := range buffs {
		totBytes += 8 + len(buff)
	}

	buff := make([]byte, 8+8+4)
	binary.LittleEndian.PutUint64(buff, uint64(totBytes))
	binary.LittleEndian.PutUint64(buff[8:], uint64(batch.RowCount))
	binary.LittleEndian.PutUint32(buff[16:], uint32(len(buffs)))

	if _, err := writer.Write(buff); err != nil {
		return err
	}

	for _, buff := range buffs {
		lb := make([]byte, 8)
		binary.LittleEndian.PutUint64(lb, uint64(len(buff)))
		if _, err := writer.Write(lb); err != nil {
			return err
		}
		if _, err := writer.Write(buff); err != nil {
			return err
		}
	}
	return nil
}

func (b *ArrowBatchWriter) WriteContentType(writer http.ResponseWriter) {
	writer.Header().Add("Content-Type", TektiteArrowMimeType)
}

var newLine = []byte{'\n'}

func DecodeArrowSchema(buff []byte) (*evbatch.EventSchema, int) {
	off := 0
	var nf uint32
	nf, off = encoding.ReadUint32FromBufferLE(buff, off)
	numCols := int(nf)
	fNames := make([]string, numCols)
	fTypes := make([]types.ColumnType, numCols)
	for i := 0; i < numCols; i++ {
		var fName string
		fName, off = encoding.ReadStringFromBufferLE(buff, off)
		var id uint32
		id, off = encoding.ReadUint32FromBufferLE(buff, off)
		fNames[i] = fName
		ftID := types.ColumnTypeID(id)
		switch ftID {
		case types.ColumnTypeIDInt:
			fTypes[i] = types.ColumnTypeInt
		case types.ColumnTypeIDFloat:
			fTypes[i] = types.ColumnTypeFloat
		case types.ColumnTypeIDBool:
			fTypes[i] = types.ColumnTypeBool
		case types.ColumnTypeIDDecimal:
			var p uint32
			p, off = encoding.ReadUint32FromBufferLE(buff, off)
			var s uint32
			s, off = encoding.ReadUint32FromBufferLE(buff, off)
			dt := &types.DecimalType{
				Precision: int(p),
				Scale:     int(s),
			}
			fTypes[i] = dt
		case types.ColumnTypeIDString:
			fTypes[i] = types.ColumnTypeString
		case types.ColumnTypeIDBytes:
			fTypes[i] = types.ColumnTypeBytes
		case types.ColumnTypeIDTimestamp:
			fTypes[i] = types.ColumnTypeTimestamp
		default:
			panic("unexpected type")
		}
	}
	return evbatch.NewEventSchema(fNames, fTypes), off
}

func DecodeArrowBatch(schema *evbatch.EventSchema, buff []byte) *evbatch.Batch {
	off := 0

	var rc uint64
	rc, off = encoding.ReadUint64FromBufferLE(buff, off)
	rowCount := int(rc)

	var nb uint32
	nb, off = encoding.ReadUint32FromBufferLE(buff, off)
	numBuffs := int(nb)

	batchBuffs := make([][]byte, numBuffs)
	for i := 0; i < numBuffs; i++ {
		var lb uint64
		lb, off = encoding.ReadUint64FromBufferLE(buff, off)
		buffLen := int(lb)
		batchBuffs[i] = buff[off : off+buffLen]
		off += buffLen
	}
	return evbatch.NewBatchFromBytes(schema, rowCount, batchBuffs)
}
