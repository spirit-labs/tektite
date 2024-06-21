package opers

import (
	"bytes"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestWindowedAggCloseSingleWindow(t *testing.T) {
	testWindowedAggCloseSingleWindow(t, 109)
}

func TestWindowedAggCloseSingleWindow2(t *testing.T) {
	testWindowedAggCloseSingleWindow(t, 110)
}

func TestWindowedAggCloseSingleWindow3(t *testing.T) {
	testWindowedAggCloseSingleWindow(t, 111)
}

func testWindowedAggCloseSingleWindow(t *testing.T, waterMark int) {
	inData := [][]any{
		{types.NewTimestamp(107), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(106), "USA", int64(7)},
		{types.NewTimestamp(104), "USA", int64(5)},
	}
	outData := [][]any{
		{types.NewTimestamp(107), types.NewTimestamp(10), types.NewTimestamp(110), "UK", int64(11)},
		{types.NewTimestamp(106), types.NewTimestamp(10), types.NewTimestamp(110), "USA", int64(12)},
	}
	testWindowedAgg(t, inData, outData, waterMark, 0, false, 10)
}

func TestWindowedAggCloseSingleWindowWithOffset(t *testing.T) {
	inData := [][]any{
		{int64(0), types.NewTimestamp(107), "UK", int64(3)},
		{int64(1), types.NewTimestamp(105), "UK", int64(8)},
		{int64(2), types.NewTimestamp(106), "USA", int64(7)},
		{int64(3), types.NewTimestamp(104), "USA", int64(5)},
	}
	outData := [][]any{
		{types.NewTimestamp(107), types.NewTimestamp(10), types.NewTimestamp(110), "UK", int64(11)},
		{types.NewTimestamp(106), types.NewTimestamp(10), types.NewTimestamp(110), "USA", int64(12)},
	}
	testWindowedAgg(t, inData, outData, 110, 0, true, 10)
}

func TestWindowedAggCloseMultipleWindows(t *testing.T) {

	inData := [][]any{
		{types.NewTimestamp(110), "UK", int64(3)},
		{types.NewTimestamp(111), "UK", int64(8)},
		{types.NewTimestamp(115), "USA", int64(7)},
		{types.NewTimestamp(119), "UK", int64(2)},
		{types.NewTimestamp(123), "USA", int64(5)},
		{types.NewTimestamp(127), "USA", int64(10)},
		{types.NewTimestamp(120), "USA", int64(9)},
		{types.NewTimestamp(125), "UK", int64(1)},
		{types.NewTimestamp(133), "USA", int64(11)},
		{types.NewTimestamp(150), "UK", int64(5)},
		{types.NewTimestamp(151), "USA", int64(8)},
		{types.NewTimestamp(200), "USA", int64(6)},
	}

	outData := [][]any{
		{types.NewTimestamp(119), types.NewTimestamp(20), types.NewTimestamp(120), "UK", int64(13)},
		{types.NewTimestamp(115), types.NewTimestamp(20), types.NewTimestamp(120), "USA", int64(7)},
		{types.NewTimestamp(125), types.NewTimestamp(30), types.NewTimestamp(130), "UK", int64(14)},
		{types.NewTimestamp(127), types.NewTimestamp(30), types.NewTimestamp(130), "USA", int64(31)},
		{types.NewTimestamp(125), types.NewTimestamp(40), types.NewTimestamp(140), "UK", int64(14)},
		{types.NewTimestamp(133), types.NewTimestamp(40), types.NewTimestamp(140), "USA", int64(42)},
		{types.NewTimestamp(125), types.NewTimestamp(50), types.NewTimestamp(150), "UK", int64(14)},
		{types.NewTimestamp(133), types.NewTimestamp(50), types.NewTimestamp(150), "USA", int64(42)},
		{types.NewTimestamp(150), types.NewTimestamp(60), types.NewTimestamp(160), "UK", int64(19)},
		{types.NewTimestamp(151), types.NewTimestamp(60), types.NewTimestamp(160), "USA", int64(50)},
		{types.NewTimestamp(150), types.NewTimestamp(70), types.NewTimestamp(170), "UK", int64(19)},
		{types.NewTimestamp(151), types.NewTimestamp(70), types.NewTimestamp(170), "USA", int64(50)},
		{types.NewTimestamp(150), types.NewTimestamp(80), types.NewTimestamp(180), "UK", int64(19)},
		{types.NewTimestamp(151), types.NewTimestamp(80), types.NewTimestamp(180), "USA", int64(50)},
		{types.NewTimestamp(150), types.NewTimestamp(90), types.NewTimestamp(190), "UK", int64(19)},
		{types.NewTimestamp(151), types.NewTimestamp(90), types.NewTimestamp(190), "USA", int64(50)},
		{types.NewTimestamp(150), types.NewTimestamp(100), types.NewTimestamp(200), "UK", int64(19)},
		{types.NewTimestamp(151), types.NewTimestamp(100), types.NewTimestamp(200), "USA", int64(50)},
		{types.NewTimestamp(150), types.NewTimestamp(110), types.NewTimestamp(210), "UK", int64(19)},
		{types.NewTimestamp(200), types.NewTimestamp(110), types.NewTimestamp(210), "USA", int64(56)},
		{types.NewTimestamp(150), types.NewTimestamp(120), types.NewTimestamp(220), "UK", int64(6)},
		{types.NewTimestamp(200), types.NewTimestamp(120), types.NewTimestamp(220), "USA", int64(49)},
		{types.NewTimestamp(150), types.NewTimestamp(130), types.NewTimestamp(230), "UK", int64(5)},
		{types.NewTimestamp(200), types.NewTimestamp(130), types.NewTimestamp(230), "USA", int64(25)},
	}
	testWindowedAgg(t, inData, outData, 230, 0, false, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130)
}

func TestWindowedAggWindowNotClosed1(t *testing.T) {
	testWindowedAggWindowNotClosed(t, 50)
}

func TestWindowedAggWindowNotClosed2(t *testing.T) {
	testWindowedAggWindowNotClosed(t, 108)
}

func testWindowedAggWindowNotClosed(t *testing.T, waterMark int) {
	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	var outData [][]any
	testWindowedAgg(t, inData, outData, waterMark, 0, false)
}

func TestWindowedAggWindowNotClosedWithLateness1(t *testing.T) {
	testWindowedAggWindowNotClosedWithLateness(t, 100, 208)
}

func TestWindowedAggWindowNotClosedWithLateness2(t *testing.T) {
	testWindowedAggWindowNotClosedWithLateness(t, 100, 150)
}

func testWindowedAggWindowNotClosedWithLateness(t *testing.T, lateness int, waterMark int) {
	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	var outData [][]any
	testWindowedAgg(t, inData, outData, waterMark, lateness, false, 10)
}

func TestWindowedAggCloseSingleWindowWithLateness1(t *testing.T) {
	testWindowedAggCloseSingleWindowWithLateness(t, 100, 209)
}

func TestWindowedAggCloseSingleWindowWithLateness2(t *testing.T) {
	testWindowedAggCloseSingleWindowWithLateness(t, 100, 210)
}

func TestWindowedAggCloseSingleWindowWithLateness3(t *testing.T) {
	testWindowedAggCloseSingleWindowWithLateness(t, 100, 211)
}

func TestWindowedAggCloseMultipleWindowsWithLateness4(t *testing.T) {
	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	outData := [][]any{
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(20), types.NewTimestamp(120), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(20), types.NewTimestamp(120), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(30), types.NewTimestamp(130), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(30), types.NewTimestamp(130), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(40), types.NewTimestamp(140), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(40), types.NewTimestamp(140), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(50), types.NewTimestamp(150), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(50), types.NewTimestamp(150), "USA", int64(12)},
	}
	testWindowedAgg(t, inData, outData, 250, 100, false, 10, 20, 30, 40, 50)
}

func testWindowedAggCloseSingleWindowWithLateness(t *testing.T, lateness int, waterMark int) {
	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	outData := [][]any{
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "USA", int64(12)},
	}
	testWindowedAgg(t, inData, outData, waterMark, lateness, false, 10)
}

func TestWindowedAggLateEventsDropped1(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 109)
}

func TestWindowedAggLateEventsDropped2(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 110)
}

func TestWindowedAggLateEventsDropped3(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 111)
}

func TestWindowedAggLateEventsDropped4(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 200)
}

func TestWindowedAggLateEventsDropped5(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 100, 209)
}

func TestWindowedAggLateEventsDropped6(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 210)
}

func TestWindowedAggLateEventsDropped7(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 211)
}

func TestWindowedAggLateEventsDropped8(t *testing.T) {
	testWindowedAggLateEventsDropped(t, 0, 300)
}

func testWindowedAggLateEventsDropped(t *testing.T, latenessMs int, waterMark int) {
	tableID := 1001
	agg, st := setupAgg(t, latenessMs, tableID, false)
	partitionID := 1
	ppm := agg.processSchema.PartitionScheme.PartitionProcessorMapping
	processorID := ppm[partitionID]
	version := 12345

	// Send initial watermark
	sendWaterMarkAndGetEntries(t, agg, waterMark, processorID, version)

	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	sendAggWindowBatch(t, inData, agg, st, version, partitionID)

	entries := sendWaterMarkAndGetEntries(t, agg, waterMark+10, processorID, version)
	require.Equal(t, 0, len(entries))
}

func TestWindowedAggEventsNotDropped1(t *testing.T) {
	testWindowedAggEventsNotDropped(t, 0, 104)
}

func TestWindowedAggEventsNotDropped2(t *testing.T) {
	testWindowedAggEventsNotDropped(t, 0, 50)
}

func TestWindowedAggEventsNotDropped3(t *testing.T) {
	testWindowedAggEventsNotDropped(t, 100, 204)
}

func TestWindowedAggEventsNotDropped4(t *testing.T) {
	testWindowedAggEventsNotDropped(t, 100, 150)
}

func testWindowedAggEventsNotDropped(t *testing.T, latenessMs int, waterMark int) {
	tableID := 1001
	agg, st := setupAgg(t, latenessMs, tableID, false)
	partitionID := 1
	ppm := agg.processSchema.PartitionScheme.PartitionProcessorMapping
	processorID := ppm[partitionID]
	version := 12345

	// Send initial watermark
	sendWaterMarkAndGetEntries(t, agg, waterMark, processorID, version)

	inData := [][]any{
		{types.NewTimestamp(105), "UK", int64(3)},
		{types.NewTimestamp(105), "UK", int64(8)},
		{types.NewTimestamp(105), "USA", int64(7)},
		{types.NewTimestamp(105), "USA", int64(5)},
	}
	sendAggWindowBatch(t, inData, agg, st, version, partitionID)

	outData := [][]any{
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(10), types.NewTimestamp(110), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(20), types.NewTimestamp(120), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(20), types.NewTimestamp(120), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(30), types.NewTimestamp(130), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(30), types.NewTimestamp(130), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(40), types.NewTimestamp(140), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(40), types.NewTimestamp(140), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(50), types.NewTimestamp(150), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(50), types.NewTimestamp(150), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(60), types.NewTimestamp(160), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(60), types.NewTimestamp(160), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(70), types.NewTimestamp(170), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(70), types.NewTimestamp(170), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(80), types.NewTimestamp(180), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(80), types.NewTimestamp(180), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(90), types.NewTimestamp(190), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(90), types.NewTimestamp(190), "USA", int64(12)},
		{types.NewTimestamp(105), types.NewTimestamp(100), types.NewTimestamp(200), "UK", int64(11)},
		{types.NewTimestamp(105), types.NewTimestamp(100), types.NewTimestamp(200), "USA", int64(12)},
	}

	// Send a high watermark to make sure any non dropped batch gets published

	sendWaterMarkAndVerifyEntries(t, agg, 1000, partitionID, processorID, version, outData, 10, 20,
		30, 40, 50, 60, 70, 80, 90, 100)
}

func testWindowedAgg(t *testing.T, inData [][]any, outData [][]any, waterMark int, latenessMs int, offset bool, closedWindows ...int) {
	tableID := 1001
	agg, st := setupAgg(t, latenessMs, tableID, offset)

	partitionID := 1
	version := 12345
	sendAggWindowBatch(t, inData, agg, st, version, partitionID)
	processorID := agg.processSchema.PartitionScheme.PartitionProcessorMapping[partitionID]

	sendWaterMarkAndVerifyEntries(t, agg, waterMark, partitionID, processorID, version, outData, closedWindows...)
}

func setupAgg(t *testing.T, latenessMs int, tableID int, offset bool) (*AggregateOperator, *store2.Store) {
	inColumnNames := []string{"offset", "event_time", "country", "amount"}
	inColumnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt}
	if !offset {
		inColumnNames = inColumnNames[1:]
		inColumnTypes = inColumnTypes[1:]
	}

	aggExprStrs := []string{"sum(amount)"}
	keyExprStrs := []string{"country"}
	outColumnNames := []string{"event_time", "ws", "we", "country", "sum(amount)"}
	outColumnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt}

	inSchema := evbatch.NewEventSchema(inColumnNames, inColumnTypes)
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	operSchema := &OperatorSchema{
		EventSchema:     inSchema,
		PartitionScheme: NewPartitionScheme("test_stream", 10, false, 10),
	}
	aggExprs, err := toExprs(aggExprStrs...)
	require.NoError(t, err)
	keyExprs, err := toExprs(keyExprStrs...)
	require.NoError(t, err)
	aggDesc := &parser.AggregateDesc{
		AggregateExprs:       aggExprs,
		KeyExprs:             keyExprs,
		AggregateExprStrings: aggExprStrs,
		KeyExprsStrings:      keyExprStrs,
	}
	agg, err := NewAggregateOperator(operSchema, aggDesc, tableID,
		1002, 1003, 1004, time.Duration(100)*time.Millisecond,
		time.Duration(10)*time.Millisecond, st, time.Duration(latenessMs)*time.Millisecond, false, true,
		&expr.ExpressionFactory{})
	require.NoError(t, err)
	require.Equal(t, outColumnNames, agg.aggStateSchema.ColumnNames())
	require.Equal(t, outColumnTypes, agg.aggStateSchema.ColumnTypes())
	return agg, st
}

func sendAggWindowBatch(t *testing.T, inData [][]any, agg *AggregateOperator,
	st *store2.Store, version int, partitionID int) {

	inSchema := agg.inSchema.EventSchema

	// Send batch
	batch := createEventBatch(inSchema.ColumnNames(), inSchema.ColumnTypes(), inData)

	ppm := agg.inSchema.PartitionScheme.PartitionProcessorMapping
	processorID := ppm[partitionID]
	ctx := &testExecCtx{
		version:     version,
		partitionID: partitionID,
		processor:   &testProcessor{id: processorID},
	}
	_, err := agg.HandleStreamBatch(batch, ctx)
	require.NoError(t, err)

	// Write the interim entries to the store
	mb := mem.NewBatch()
	for _, entry := range ctx.entries {
		mb.AddEntry(entry)
	}
	err = st.Write(mb)
	require.NoError(t, err)
}

func sendWaterMarkAndVerifyEntries(t *testing.T, agg *AggregateOperator, waterMark int, partitionID int,
	processorID int, version int, expectedOutData [][]any, closedWindows ...int) {

	captureOper := &capturingOperator{}
	agg.AddDownStreamOperator(captureOper)

	// Send watermark and get published entries
	entries := sendWaterMarkAndGetEntries(t, agg, waterMark, processorID, version)

	if len(entries) == 0 {
		require.Equal(t, len(expectedOutData), 0)
	} else {
		// extract the closed window entries
		var closedWindowEntries []common.KV
		partitionHash := proc.CalcPartitionHash(agg.outSchema.MappingID, uint64(partitionID))
		closedWindowKey := encoding.EncodeEntryPrefix(partitionHash, agg.openWindowsSlabID, 24)
		for _, entry := range entries {
			if len(entry.Key) >= len(closedWindowKey) && bytes.Equal(closedWindowKey, entry.Key[:len(closedWindowKey)]) {
				closedWindowEntries = append(closedWindowEntries, entry)
			}
		}

		require.Equal(t, len(closedWindows), len(closedWindowEntries))
		closedWindowSet := map[int]struct{}{}
		for _, closedWindow := range closedWindows {
			closedWindowSet[closedWindow] = struct{}{}
		}
		for _, closedWindowEntry := range closedWindowEntries {
			closedWindowKey := closedWindowEntry.Key
			windowStartTs, _ := encoding.KeyDecodeTimestamp(closedWindowKey, 24)
			_, ok := closedWindowSet[int(windowStartTs.Val)]
			require.True(t, ok)
		}
	}
	outBatches := captureOper.getBatches()
	var actualOut [][]any
	for _, batch := range outBatches {
		out := convertBatchToAnyArray(batch)
		actualOut = append(actualOut, out...)
	}
	if len(expectedOutData) == 0 {
		require.Equal(t, 0, len(actualOut))
	} else {
		require.Equal(t, expectedOutData, actualOut)
	}
}

func sendWaterMarkAndGetEntries(t *testing.T, ao *AggregateOperator, waterMark int, processorID int, version int) []common.KV {
	ctx := &windowedAggExecCtx{
		version:   version,
		waterMark: waterMark,
	}
	processor := &windowedAggProcessor{
		id:         processorID,
		agg:        ao,
		sendingCtx: ctx,
	}
	ctx.processor = processor
	err := ao.HandleBarrier(ctx)
	require.NoError(t, err)
	return ctx.entries
}

type windowedAggExecCtx struct {
	version      int
	partitionID  int
	processor    proc.Processor
	stored       map[string][]byte
	entries      []common.KV
	wmLock       sync.Mutex
	waterMark    int
	evBatchBytes []byte
}

func (t *windowedAggExecCtx) ReceiverID() int {
	return 0
}

func (t *windowedAggExecCtx) EventBatchBytes() []byte {
	return t.evBatchBytes
}

func (t *windowedAggExecCtx) WaterMark() int {
	t.wmLock.Lock()
	defer t.wmLock.Unlock()
	return t.waterMark
}

func (t *windowedAggExecCtx) SetWaterMark(waterMark int) {
	t.wmLock.Lock()
	defer t.wmLock.Unlock()
	t.waterMark = waterMark
}

func (t *windowedAggExecCtx) CheckInProcessorLoop() {
}

func (t *windowedAggExecCtx) IngestBatch() error {
	return nil
}

func (t *windowedAggExecCtx) BackFill() bool {
	return false
}

func (t *windowedAggExecCtx) StoreEntry(kv common.KV, _ bool) {
	t.entries = append(t.entries, kv)
}

func (t *windowedAggExecCtx) ForwardEntry(int, int, int, int, *evbatch.Batch, *evbatch.EventSchema) {
}

func (t *windowedAggExecCtx) ForwardBarrier(int, int) {
}

func (t *windowedAggExecCtx) WriteVersion() int {
	return t.version
}

func (t *windowedAggExecCtx) PartitionID() int {
	return t.partitionID
}

func (t *windowedAggExecCtx) ForwardingProcessorID() int {
	return 0
}

func (t *windowedAggExecCtx) ForwardSequence() int {
	return 0
}

func (t *windowedAggExecCtx) Processor() proc.Processor {
	return t.processor
}

func (t *windowedAggExecCtx) Get(key []byte) ([]byte, error) {
	v, ok := t.stored[string(key)]
	if !ok {
		return nil, nil
	}
	return v, nil
}

type windowedAggProcessor struct {
	id         int
	agg        *AggregateOperator
	sendingCtx *windowedAggExecCtx
}

func (w *windowedAggProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}

func (w *windowedAggProcessor) WriteCache() *proc.WriteCache {
	return nil
}

func (w *windowedAggProcessor) ID() int {
	return w.id
}

func (w *windowedAggProcessor) IngestBatch(processBatch *proc.ProcessBatch, completionFunc func(error)) {
	// Just forward to agg receiver
	ctx := &windowedAggExecCtx{
		version:      w.sendingCtx.version,
		partitionID:  processBatch.PartitionID,
		waterMark:    processBatch.Watermark,
		evBatchBytes: processBatch.EvBatchBytes,
	}
	_, err := w.agg.ReceiveBatch(processBatch.EvBatch, ctx)
	if err != nil {
		panic(err)
	}
	w.sendingCtx.entries = append(w.sendingCtx.entries, ctx.entries...)
	completionFunc(nil)
}

func (w *windowedAggProcessor) IngestBatchSync(*proc.ProcessBatch) error {
	return nil
}

func (w *windowedAggProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (w *windowedAggProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (w *windowedAggProcessor) SetLeader() {
}

func (w *windowedAggProcessor) IsLeader() bool {
	return false
}

func (w *windowedAggProcessor) CheckInProcessorLoop() {
}

func (w *windowedAggProcessor) Stop() {
}

func (w *windowedAggProcessor) InvalidateCachedReceiverInfo() {
}

func (w *windowedAggProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (w *windowedAggProcessor) SetNotIdleNotifier(func()) {
}

func (w *windowedAggProcessor) IsIdle(int) bool {
	return false
}

func (w *windowedAggProcessor) IsStopped() bool {
	return false
}

func (w *windowedAggProcessor) SetReplicator(proc.Replicator) {
}

func (w *windowedAggProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (w *windowedAggProcessor) SubmitAction(func() error) bool {
	return false
}

func (w *windowedAggProcessor) CloseVersion(int, []int) {
}
