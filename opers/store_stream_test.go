package opers

import (
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"sync"
	"testing"
)

func TestStoreOperator(t *testing.T) {
	fNames := []string{"offset", "event_time", "int_col", "float_col", "bool_col", "dec_col", "string_col", "bytes_col", "ts_col"}
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	to := createStoreOperator(t, fNames, fTypes)
	data := [][]any{
		{int64(0), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(1), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(2), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(3), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(4), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	testStoreOperator(t, to, fNames, fTypes, data, fNames, fTypes, data, false)
	// Add some more data
	data = [][]any{
		{int64(5), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(6), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(7), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(8), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
		{int64(9), types.NewTimestamp(10), int64(12), float64(1.0), true, createDecimal(t, "12345.54321"), "str1", []byte("bytes1"), types.NewTimestamp(0)},
	}
	testStoreOperator(t, to, fNames, fTypes, data, fNames, fTypes, data, false)
}

func createStoreOperator(t *testing.T, columnNamesIn []string, columnTypesIn []types.ColumnType) *StoreStreamOperator {
	inSchema := evbatch.NewEventSchema(columnNamesIn, columnTypesIn)
	partitionCount := 10
	opSchema := &OperatorSchema{
		EventSchema:     inSchema,
		PartitionScheme: NewPartitionScheme("test_stream", partitionCount, false, 10),
	}
	to, err := NewStoreStreamOperator(opSchema, 1001, -1, -1)
	require.NoError(t, err)
	return to
}

func testStoreOperator(t *testing.T, to *StoreStreamOperator, columnNamesIn []string, columnTypesIn []types.ColumnType, dataIn [][]any,
	columnNamesOut []string, columnTypesOut []types.ColumnType, expectedOutData [][]any, offsetAdded bool) {

	partitionCount := 9

	for partID := 0; partID < partitionCount; partID++ {
		batch := createEventBatch(columnNamesIn, columnTypesIn, dataIn)
		//goland:noinspection GoDeferInLoop
		defer batch.Release()

		version := 1234
		ctx := &testExecCtx{
			version:     version,
			partitionID: partID,
		}

		out, err := to.HandleStreamBatch(batch, ctx)
		require.NoError(t, err)

		expectedOutSchema := evbatch.NewEventSchema(columnNamesOut, columnTypesOut)
		require.Equal(t, expectedOutSchema, to.OutSchema().EventSchema)

		actualOutData := convertBatchToAnyArray(out)
		require.Equal(t, expectedOutData, actualOutData)
		expectedNumEntries := len(expectedOutData)
		if offsetAdded {
			// There's an extra row written to record the offset
			expectedNumEntries++
		}
		require.Equal(t, expectedNumEntries, len(ctx.entries))
		var keyTypes []types.ColumnType
		for _, keyCol := range to.keyCols {
			keyTypes = append(keyTypes, to.OutSchema().EventSchema.ColumnTypes()[keyCol])
		}
		var rowTypes []types.ColumnType
		for _, colIndex := range to.rowCols {
			rowTypes = append(rowTypes, to.OutSchema().EventSchema.ColumnTypes()[colIndex])
		}
		var loadedOutData [][]any
		foundOffsetEntry := false
		for _, kv := range ctx.entries {
			partitionHash := kv.Key[:16]
			expectedPartitionHash := proc.CalcPartitionHash(to.OutSchema().MappingID, uint64(partID))
			require.Equal(t, expectedPartitionHash, partitionHash)
			slabID, _ := encoding2.ReadUint64FromBufferBE(kv.Key, 16)
			if !foundOffsetEntry && slabID == uint64(to.offsetsSlabID) {
				foundOffsetEntry = true
				continue
			}
			ver, _ := encoding2.ReadUint64FromBufferBE(kv.Key, len(kv.Key)-8)
			ver = math.MaxUint64 - ver
			require.Equal(t, 1001, int(slabID))

			require.Equal(t, version, int(ver))
			key := kv.Key[24:]
			keySlice, _, err := encoding2.DecodeKeyToSlice(key, 0, keyTypes)
			require.NoError(t, err)
			rowSlice, _ := encoding2.DecodeRowToSlice(kv.Value, 0, rowTypes)
			actualOut := make([]any, len(to.OutSchema().EventSchema.ColumnTypes()))
			for i, keyCol := range to.keyCols {
				actualOut[keyCol] = keySlice[i]
			}
			for i, rowCol := range to.rowCols {
				actualOut[rowCol] = rowSlice[i]
			}
			loadedOutData = append(loadedOutData, actualOut)
		}
		loadedOutData = sortDataByKeyCols(loadedOutData, to.keyCols, to.OutSchema().EventSchema.ColumnTypes())
		require.Equal(t, expectedOutData, loadedOutData)
	}
}

type testExecCtx struct {
	version               int
	partitionID           int
	forwardingProcessorID int
	forwardSequence       int
	processor             proc.Processor
	stored                map[string][]byte
	entries               []common.KV
	wmLock                sync.Mutex
	waterMark             int
	backFill              bool
}

func (t *testExecCtx) ReceiverID() int {
	return 0
}

func (t *testExecCtx) EventBatchBytes() []byte {
	return nil
}

func (t *testExecCtx) WaterMark() int {
	t.wmLock.Lock()
	defer t.wmLock.Unlock()
	return t.waterMark
}

func (t *testExecCtx) SetWaterMark(waterMark int) {
	t.wmLock.Lock()
	defer t.wmLock.Unlock()
	t.waterMark = waterMark
}

func (t *testExecCtx) CheckInProcessorLoop() {
}

func (t *testExecCtx) BackFill() bool {
	return t.backFill
}

func (t *testExecCtx) StoreEntry(kv common.KV, _ bool) {
	t.entries = append(t.entries, kv)
}

func (t *testExecCtx) ForwardEntry(int, int, int, int, *evbatch.Batch, *evbatch.EventSchema) {
}

func (t *testExecCtx) ForwardBarrier(int, int) {
}

func (t *testExecCtx) WriteVersion() int {
	return t.version
}

func (t *testExecCtx) PartitionID() int {
	return t.partitionID
}

func (t *testExecCtx) ForwardingProcessorID() int {
	return t.forwardingProcessorID
}

func (t *testExecCtx) ForwardSequence() int {
	return t.forwardSequence
}

func (t *testExecCtx) Processor() proc.Processor {
	return t.processor
}

func (t *testExecCtx) Get(key []byte) ([]byte, error) {
	v, ok := t.stored[string(key)]
	if !ok {
		return nil, nil
	}
	return v, nil
}
