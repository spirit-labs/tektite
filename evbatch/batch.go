package evbatch

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/spirit-labs/tektite/asl/encoding"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"strings"
)

type Batch struct {
	Schema   *EventSchema
	Columns  []Column
	RowCount int
}

func NewBatchFromBuilders(schema *EventSchema, builders ...ColumnBuilder) *Batch {
	cols := make([]Column, len(builders))
	for i, colBuilder := range builders {
		cols[i] = colBuilder.Build()
	}
	return NewBatch(schema, cols...)
}

func NewBatch(schema *EventSchema, columns ...Column) *Batch {
	rc := -1
	for i, col := range columns {
		cl := col.Len()
		if rc != -1 && cl != rc {
			panic(fmt.Sprintf("column %s not same length (%d) as others (%d) col_names: %v col_types:%v",
				schema.ColumnNames()[i], cl, rc, schema.ColumnNames(), schema.ColumnTypes()))
		}
		rc = cl
	}
	return &Batch{
		Schema:   schema,
		Columns:  columns,
		RowCount: rc,
	}
}

func NewBatchFromBytes(schema *EventSchema, rowCount int, bytes [][]byte) *Batch {
	cols := make([]Column, len(schema.columnTypes))
	buffPos := 0
	for i, columnType := range schema.columnTypes {
		switch columnType.ID() {
		case types.ColumnTypeIDInt:
			cols[i] = NewIntColumnFromBytes(bytes[buffPos:buffPos+2], rowCount)
			buffPos += 2
		case types.ColumnTypeIDFloat:
			cols[i] = NewFloatColumnFromBytes(bytes[buffPos:buffPos+2], rowCount)
			buffPos += 2
		case types.ColumnTypeIDBool:
			cols[i] = NewBoolColumnFromBytes(bytes[buffPos:buffPos+2], rowCount)
			buffPos += 2
		case types.ColumnTypeIDDecimal:
			decType := columnType.(*types.DecimalType)
			cols[i] = NewDecimalColumnFromBytes(bytes[buffPos:buffPos+2], rowCount, decType.Precision, decType.Scale)
			buffPos += 2
		case types.ColumnTypeIDString:
			cols[i] = NewStringColumnFromBytes(bytes[buffPos:buffPos+3], rowCount)
			buffPos += 3
		case types.ColumnTypeIDBytes:
			cols[i] = NewBytesColumnFromBytes(bytes[buffPos:buffPos+3], rowCount)
			buffPos += 3
		case types.ColumnTypeIDTimestamp:
			cols[i] = NewTimestampColumnFromBytes(bytes[buffPos:buffPos+2], rowCount)
			buffPos += 2
		default:
			panic("unexpected type")
		}
	}
	return NewBatch(schema, cols...)
}

func (b *Batch) ToBytes() [][]byte {
	buffs := make([][]byte, 0, 3*len(b.Columns)) // This will likely overallocate a little, but should never underallocate
	for _, col := range b.Columns {
		var mBuffs []*memory.Buffer
		switch c := col.(type) {
		case *IntColumn:
			mBuffs = c.array.Data().Buffers()
		case *FloatColumn:
			mBuffs = c.array.Data().Buffers()
		case *BoolColumn:
			mBuffs = c.array.Data().Buffers()
		case *DecimalColumn:
			mBuffs = c.array.Data().Buffers()
		case *StringColumn:
			mBuffs = c.array.Data().Buffers()
		case *BytesColumn:
			mBuffs = c.array.Data().Buffers()
		case *TimestampColumn:
			mBuffs = c.array.Data().Buffers()
		default:
			panic("unknown type")
		}
		for _, mBuff := range mBuffs {
			if mBuff == nil {
				buffs = append(buffs, nil)
			} else {
				buffs = append(buffs, mBuff.Bytes())
			}
		}
	}
	return buffs
}

func (b *Batch) Serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(b.RowCount))
	buffs := b.ToBytes()
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(buffs)))
	for _, buf := range buffs {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(buf)))
		buff = append(buff, buf...)
	}
	return buff
}

func NewBatchFromSingleBuff(schema *EventSchema, buff []byte) *Batch {
	off := 0
	var rc uint64
	rc, off = encoding.ReadUint64FromBufferLE(buff, off)
	var nb uint32
	nb, off = encoding.ReadUint32FromBufferLE(buff, off)
	buffs := make([][]byte, 0, int(nb))
	for i := 0; i < int(nb); i++ {
		var bl uint32
		bl, off = encoding.ReadUint32FromBufferLE(buff, off)
		ibl := int(bl)
		buffs = append(buffs, buff[off:off+ibl])
		off += ibl
	}
	return NewBatchFromBytes(schema, int(rc), buffs)
}

func (b *Batch) Release() {
	// we currently use the go allocator so retain/release does nothing
}

func (b *Batch) Retain() {
	// we currently use the go allocator so retain/release does nothing
}

func (b *Batch) GetIntColumn(colIndex int) *IntColumn {
	return b.Columns[colIndex].(*IntColumn)
}

func (b *Batch) GetFloatColumn(colIndex int) *FloatColumn {
	return b.Columns[colIndex].(*FloatColumn)
}

func (b *Batch) GetDecimalColumn(colIndex int) *DecimalColumn {
	return b.Columns[colIndex].(*DecimalColumn)
}

func (b *Batch) GetBoolColumn(colIndex int) *BoolColumn {
	return b.Columns[colIndex].(*BoolColumn)
}

func (b *Batch) GetStringColumn(colIndex int) *StringColumn {
	return b.Columns[colIndex].(*StringColumn)
}

func (b *Batch) GetBytesColumn(colIndex int) *BytesColumn {
	return b.Columns[colIndex].(*BytesColumn)
}

func (b *Batch) GetTimestampColumn(colIndex int) *TimestampColumn {
	return b.Columns[colIndex].(*TimestampColumn)
}

type Column interface {
	IsNull(row int) bool
	Len() int
	Retain()
	Release()
}

type ColumnBuilder interface {
	AppendNull()
	Build() Column
}

func NewIntColBuilder() *IntColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewInt64Builder(allocator)
	return &IntColBuilder{
		builder: builder,
	}
}

type IntColBuilder struct {
	builder *array.Int64Builder
}

func (ib *IntColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *IntColBuilder) Append(val int64) {
	ib.builder.Append(val)
}

func (ib *IntColBuilder) BuildIntColumn() *IntColumn {
	return &IntColumn{array: ib.builder.NewInt64Array()}
}

func (ib *IntColBuilder) Build() Column {
	return ib.BuildIntColumn()
}

var _ Column = &IntColumn{}

type IntColumn struct {
	array *array.Int64
}

func (ic *IntColumn) GetData() []int64 {
	return ic.array.Int64Values()
}

func NewIntColumnFromBytes(bytes [][]byte, length int) *IntColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(arrow.PrimitiveTypes.Int64, length, mbs, nil, 0, 0)
	arr := array.NewInt64Data(data)
	ic := &IntColumn{array: arr}
	return ic
}

func (ic *IntColumn) Retain() {
	ic.array.Retain()
}

func (ic *IntColumn) Release() {
	ic.array.Release()
}

func (ic *IntColumn) Get(row int) int64 {
	return ic.array.Value(row)
}

func (ic *IntColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *IntColumn) Len() int {
	return ic.array.Len()
}

func NewFloatColBuilder() *FloatColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(allocator)
	return &FloatColBuilder{
		builder: builder,
	}
}

type FloatColBuilder struct {
	builder *array.Float64Builder
}

func NewFloatColumnFromBytes(bytes [][]byte, length int) *FloatColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(arrow.PrimitiveTypes.Float64, length, mbs, nil, 0, 0)
	arr := array.NewFloat64Data(data)
	ic := &FloatColumn{array: arr}
	return ic
}

func (ib *FloatColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *FloatColBuilder) Append(val float64) {
	ib.builder.Append(val)
}

func (ib *FloatColBuilder) BuildFloatColumn() *FloatColumn {
	return &FloatColumn{array: ib.builder.NewFloat64Array()}
}

func (ib *FloatColBuilder) Build() Column {
	return ib.BuildFloatColumn()
}

var _ Column = &FloatColumn{}

type FloatColumn struct {
	array *array.Float64
}

func (ic *FloatColumn) Retain() {
	ic.array.Retain()
}

func (ic *FloatColumn) Release() {
	ic.array.Release()
}

func (ic *FloatColumn) Get(row int) float64 {
	return ic.array.Value(row)
}

func (ic *FloatColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *FloatColumn) Len() int {
	return ic.array.Len()
}

func NewBoolColBuilder() *BoolColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(allocator)
	return &BoolColBuilder{
		builder: builder,
	}
}

type BoolColBuilder struct {
	builder *array.BooleanBuilder
}

func (ib *BoolColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *BoolColBuilder) Append(val bool) {
	ib.builder.Append(val)
}

func (ib *BoolColBuilder) BuildBoolColumn() *BoolColumn {
	return &BoolColumn{array: ib.builder.NewBooleanArray()}
}

func (ib *BoolColBuilder) Build() Column {
	return ib.BuildBoolColumn()
}

var _ Column = &BoolColumn{}

type BoolColumn struct {
	array *array.Boolean
}

func NewBoolColumnFromBytes(bytes [][]byte, length int) *BoolColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(&arrow.BooleanType{}, length, mbs, nil, 0, 0)
	arr := array.NewBooleanData(data)
	ic := &BoolColumn{array: arr}
	return ic
}

func (ic *BoolColumn) Retain() {
	ic.array.Retain()
}

func (ic *BoolColumn) Release() {
	ic.array.Release()
}

func (ic *BoolColumn) Get(row int) bool {
	return ic.array.Value(row)
}

func (ic *BoolColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *BoolColumn) Len() int {
	return ic.array.Len()
}

func NewDecimalColBuilder(decimalType *types.DecimalType) *DecimalColBuilder {
	dt := &arrow.Decimal128Type{
		Precision: int32(decimalType.Precision),
		Scale:     int32(decimalType.Scale),
	}
	allocator := memory.NewGoAllocator()
	builder := array.NewDecimal128Builder(allocator, dt)
	return &DecimalColBuilder{
		decimalType: decimalType,
		builder:     builder,
	}
}

type DecimalColBuilder struct {
	decimalType *types.DecimalType
	builder     *array.Decimal128Builder
}

func (ib *DecimalColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *DecimalColBuilder) Append(val types.Decimal) {
	ib.builder.Append(val.Num)
}

func (ib *DecimalColBuilder) BuildDecimalColumn() *DecimalColumn {
	return &DecimalColumn{
		precision: ib.decimalType.Precision,
		scale:     ib.decimalType.Scale,
		array:     ib.builder.NewDecimal128Array(),
	}
}

func (ib *DecimalColBuilder) Build() Column {
	return ib.BuildDecimalColumn()
}

var _ Column = &DecimalColumn{}

type DecimalColumn struct {
	precision int
	scale     int
	array     *array.Decimal128
}

func NewDecimalColumnFromBytes(bytes [][]byte, length int, precision int, scale int) *DecimalColumn {
	mbs := bytesToMBuffs(bytes)
	dt, err := arrow.NewDecimalType(arrow.DECIMAL128, 0, 0)
	if err != nil {
		panic(err)
	}
	data := array.NewData(dt, length, mbs, nil, 0, 0)
	arr := array.NewDecimal128Data(data)
	ic := &DecimalColumn{
		precision: precision,
		scale:     scale,
		array:     arr,
	}
	return ic
}

func (ic *DecimalColumn) Retain() {
	ic.array.Retain()
}

func (ic *DecimalColumn) Release() {
	ic.array.Release()
}

func (ic *DecimalColumn) Get(row int) types.Decimal {
	return types.Decimal{
		Num:       ic.array.Value(row),
		Precision: ic.precision,
		Scale:     ic.scale,
	}
}

func (ic *DecimalColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *DecimalColumn) Len() int {
	return ic.array.Len()
}

func NewStringColBuilder() *StringColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewStringBuilder(allocator)
	return &StringColBuilder{
		builder: builder,
	}
}

type StringColBuilder struct {
	builder *array.StringBuilder
}

func (ib *StringColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *StringColBuilder) Append(val string) {
	ib.builder.Append(val)
}

func (ib *StringColBuilder) BuildStringColumn() *StringColumn {
	return &StringColumn{array: ib.builder.NewStringArray()}
}

func (ib *StringColBuilder) Build() Column {
	return ib.BuildStringColumn()
}

var _ Column = &StringColumn{}

type StringColumn struct {
	array *array.String
}

func NewStringColumnFromBytes(bytes [][]byte, length int) *StringColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(&arrow.StringType{}, length, mbs, nil, 0, 0)
	arr := array.NewStringData(data)
	ic := &StringColumn{array: arr}
	return ic
}

func (ic *StringColumn) Retain() {
	ic.array.Retain()
}

func (ic *StringColumn) Release() {
	ic.array.Release()
}

func (ic *StringColumn) Get(row int) string {
	return ic.array.Value(row)
}

func (ic *StringColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *StringColumn) Len() int {
	return ic.array.Len()
}

func NewBytesColBuilder() *BytesColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	return &BytesColBuilder{
		builder: builder,
	}
}

type BytesColBuilder struct {
	builder *array.BinaryBuilder
}

func (ib *BytesColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *BytesColBuilder) Append(val []byte) {
	ib.builder.Append(val)
}

func (ib *BytesColBuilder) BuildBytesColumn() *BytesColumn {
	return &BytesColumn{array: ib.builder.NewBinaryArray()}
}

func (ib *BytesColBuilder) Build() Column {
	return ib.BuildBytesColumn()
}

var _ Column = &BytesColumn{}

type BytesColumn struct {
	array *array.Binary
}

func NewBytesColumnFromBytes(bytes [][]byte, length int) *BytesColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(&arrow.BinaryType{}, length, mbs, nil, 0, 0)
	arr := array.NewBinaryData(data)
	ic := &BytesColumn{array: arr}
	return ic
}

func (ic *BytesColumn) Retain() {
	ic.array.Retain()
}

func (ic *BytesColumn) Release() {
	ic.array.Release()
}

func (ic *BytesColumn) Get(row int) []byte {
	return ic.array.Value(row)
}

func (ic *BytesColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *BytesColumn) Len() int {
	return ic.array.Len()
}

func NewTimestampColBuilder() *TimestampColBuilder {
	allocator := memory.NewGoAllocator()
	builder := array.NewInt64Builder(allocator)
	return &TimestampColBuilder{
		builder: builder,
	}
}

type TimestampColBuilder struct {
	builder *array.Int64Builder
}

func (ib *TimestampColBuilder) AppendNull() {
	ib.builder.AppendNull()
}

func (ib *TimestampColBuilder) Append(val types.Timestamp) {
	ib.builder.Append(val.Val)
}

func (ib *TimestampColBuilder) BuildTimestampColumn() *TimestampColumn {
	return &TimestampColumn{array: ib.builder.NewInt64Array()}
}

func (ib *TimestampColBuilder) Build() Column {
	return ib.BuildTimestampColumn()
}

var _ Column = &TimestampColumn{}

type TimestampColumn struct {
	array *array.Int64
}

func NewTimestampColumnFromBytes(bytes [][]byte, length int) *TimestampColumn {
	mbs := bytesToMBuffs(bytes)
	data := array.NewData(arrow.PrimitiveTypes.Int64, length, mbs, nil, 0, 0)
	arr := array.NewInt64Data(data)
	ic := &TimestampColumn{array: arr}
	return ic
}

func (ic *TimestampColumn) Retain() {
	ic.array.Retain()
}

func (ic *TimestampColumn) Release() {
	ic.array.Release()
}

func (ic *TimestampColumn) Get(row int) types.Timestamp {
	return types.NewTimestamp(ic.array.Value(row))
}

func (ic *TimestampColumn) IsNull(row int) bool {
	return ic.array.IsNull(row)
}

func (ic *TimestampColumn) Len() int {
	return ic.array.Len()
}

func CreateColBuilders(columnTypes []types.ColumnType) []ColumnBuilder {
	colBuilders := make([]ColumnBuilder, len(columnTypes))
	for colIndex, ft := range columnTypes {
		switch ft.ID() {
		case types.ColumnTypeIDInt:
			colBuilders[colIndex] = NewIntColBuilder()
		case types.ColumnTypeIDFloat:
			colBuilders[colIndex] = NewFloatColBuilder()
		case types.ColumnTypeIDBool:
			colBuilders[colIndex] = NewBoolColBuilder()
		case types.ColumnTypeIDDecimal:
			colBuilders[colIndex] = NewDecimalColBuilder(ft.(*types.DecimalType))
		case types.ColumnTypeIDString:
			colBuilders[colIndex] = NewStringColBuilder()
		case types.ColumnTypeIDBytes:
			colBuilders[colIndex] = NewBytesColBuilder()
		case types.ColumnTypeIDTimestamp:
			colBuilders[colIndex] = NewTimestampColBuilder()
		default:
			panic(fmt.Sprintf("unknown column type %d", ft.ID()))
		}
	}
	return colBuilders
}

func CopyColumnEntry(ft types.ColumnType, colBuilders []ColumnBuilder, colIndex int, rowIndex int, batch *Batch) {
	col := batch.Columns[colIndex]
	if col.IsNull(rowIndex) {
		colBuilders[colIndex].AppendNull()
		return
	}
	colBuilder := colBuilders[colIndex]
	CopyColumnEntryWithCol(ft, col, colBuilder, rowIndex)
}

func CopyColumnEntryWithCol(ft types.ColumnType, col Column, colBuilder ColumnBuilder, rowIndex int) {
	if col.IsNull(rowIndex) {
		colBuilder.AppendNull()
		return
	}
	switch ft.ID() {
	case types.ColumnTypeIDInt:
		colBuilder.(*IntColBuilder).Append(col.(*IntColumn).Get(rowIndex))
	case types.ColumnTypeIDFloat:
		colBuilder.(*FloatColBuilder).Append(col.(*FloatColumn).Get(rowIndex))
	case types.ColumnTypeIDBool:
		colBuilder.(*BoolColBuilder).Append(col.(*BoolColumn).Get(rowIndex))
	case types.ColumnTypeIDDecimal:
		colBuilder.(*DecimalColBuilder).Append(col.(*DecimalColumn).Get(rowIndex))
	case types.ColumnTypeIDString:
		colBuilder.(*StringColBuilder).Append(col.(*StringColumn).Get(rowIndex))
	case types.ColumnTypeIDBytes:
		colBuilder.(*BytesColBuilder).Append(col.(*BytesColumn).Get(rowIndex))
	case types.ColumnTypeIDTimestamp:
		colBuilder.(*TimestampColBuilder).Append(col.(*TimestampColumn).Get(rowIndex))
	default:
		panic(fmt.Sprintf("unknown column type %d", ft.ID()))
	}
}

func bytesToMBuffs(bytes [][]byte) []*memory.Buffer {
	mbs := make([]*memory.Buffer, len(bytes))
	for i, buff := range bytes {
		mBuff := memory.NewBufferBytes(buff)
		mBuff.Retain()
		mbs[i] = mBuff
	}
	return mbs
}

func CreateEmptyBatch(schema *EventSchema) *Batch {
	colBuilders := CreateColBuilders(schema.columnTypes)
	return NewBatchFromBuilders(schema, colBuilders...)
}

func (b *Batch) Equal(other *Batch) bool {
	if b.RowCount != other.RowCount {
		return false
	}
	if len(b.Schema.columnNames) != len(other.Schema.columnNames) {
		return false
	}
	for i := 0; i < b.RowCount; i++ {
		for j, ft := range b.Schema.columnTypes {
			col1 := b.Columns[j]
			col2 := other.Columns[j]
			if col1.IsNull(i) != col2.IsNull(i) {
				return false
			}
			switch ft.ID() {
			case types.ColumnTypeIDInt:
				if col1.(*IntColumn).Get(i) != col2.(*IntColumn).Get(i) {
					return false
				}
			case types.ColumnTypeIDFloat:
				if col1.(*FloatColumn).Get(i) != col2.(*FloatColumn).Get(i) {
					return false
				}
			case types.ColumnTypeIDBool:
				if col1.(*BoolColumn).Get(i) != col2.(*BoolColumn).Get(i) {
					return false
				}
			case types.ColumnTypeIDDecimal:
				d1 := col1.(*DecimalColumn).Get(i)
				d2 := col2.(*DecimalColumn).Get(i)
				if d1.Num != d2.Num || d1.Scale != d2.Scale || d1.Precision != d2.Precision {
					return false
				}
			case types.ColumnTypeIDString:
				if col1.(*StringColumn).Get(i) != col2.(*StringColumn).Get(i) {
					return false
				}
			case types.ColumnTypeIDBytes:
				if !bytes.Equal(col1.(*BytesColumn).Get(i), col2.(*BytesColumn).Get(i)) {
					return false
				}
			case types.ColumnTypeIDTimestamp:
				if col1.(*TimestampColumn).Get(i).Val != col2.(*TimestampColumn).Get(i).Val {
					return false
				}
			default:
				panic("unexpected type")
			}
		}
	}
	return true
}

func (b *Batch) Dump() {
	builder := strings.Builder{}
	for i, fn := range b.Schema.ColumnNames() {
		builder.WriteString(fn)
		if i != len(b.Columns)-1 {
			builder.WriteString(", ")
		}
	}
	log.Debug(builder.String())
	builder = strings.Builder{}
	for i, ft := range b.Schema.ColumnTypes() {
		builder.WriteString(ft.String())
		if i != len(b.Columns)-1 {
			builder.WriteString(", ")
		}
	}
	log.Info(builder.String())
	for i := 0; i < b.RowCount; i++ {
		builder := strings.Builder{}
		for j, col := range b.Columns {
			switch b.Schema.ColumnTypes()[j].ID() {
			case types.ColumnTypeIDInt:
				builder.WriteString(fmt.Sprintf("%d", col.(*IntColumn).Get(i)))
			case types.ColumnTypeIDFloat:
				builder.WriteString(fmt.Sprintf("%f", col.(*FloatColumn).Get(i)))
			case types.ColumnTypeIDBool:
				builder.WriteString(fmt.Sprintf("%t", col.(*BoolColumn).Get(i)))
			case types.ColumnTypeIDDecimal:
				decValue := col.(*DecimalColumn).Get(i)
				builder.WriteString(fmt.Sprintf("%s", decValue.String()))
			case types.ColumnTypeIDString:
				builder.WriteString(fmt.Sprintf("%s", col.(*StringColumn).Get(i)))
			case types.ColumnTypeIDBytes:
				builder.WriteString(fmt.Sprintf("%v", col.(*BytesColumn).Get(i)))
			case types.ColumnTypeIDTimestamp:
				builder.WriteString(fmt.Sprintf("%d", col.(*TimestampColumn).Get(i).Val))
			}
			if j != len(b.Columns)-1 {
				builder.WriteString(", ")
			}
		}
		log.Info(builder.String())
	}
}
