package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestLoadBackfill(t *testing.T) {
	version := 123
	fromSlabID := 1234
	offsetsSlabID := 1235
	maxBackFillBatchSize := 50
	fs, sinkOper, schema := setup(t, maxBackFillBatchSize, fromSlabID, offsetsSlabID, 9)
	pm := tppm.NewTestProcessorManager()
	numRowsPerPartition := 100

	pm.SetBatchHandler(&operatorBatchHandler{
		oper: fs,
	})

	partitionProcessorMapping := fs.OutSchema().PartitionScheme.PartitionProcessorMapping

	// Write some rows in partition 0
	writeRowsToPartition(t, 0, numRowsPerPartition, fromSlabID, pm.GetStore(), version)

	// Write some rows in partition 5
	writeRowsToPartition(t, 5, numRowsPerPartition, fromSlabID, pm.GetStore(), version)

	partition0Processor, ok := partitionProcessorMapping[0]
	require.True(t, ok)
	partition5Processor, ok := partitionProcessorMapping[5]
	require.True(t, ok)
	require.NotEqual(t, partition0Processor, partition5Processor)

	// Start up backfill operator with the above two partitions having data already
	pm.AddActiveProcessor(partition0Processor)
	pm.AddActiveProcessor(partition5Processor)
	err := fs.Setup(&testStreamExecCtx{pm: pm})
	require.NoError(t, err)

	waitForRows(t, 2*numRowsPerPartition, sinkOper)

	partitionBatches := sinkOper.GetPartitionBatches()
	require.Equal(t, 2, len(partitionBatches))
	verifyPartitionBatches(t, partitionBatches[0], 0, 0, numRowsPerPartition)
	verifyPartitionBatches(t, partitionBatches[5], 5, 0, numRowsPerPartition)

	// partitions 0 and 5 should be non lagging now, so the operator should be able to handle rows directly
	ib := createBatchForInjection(schema, 130, 100, 0)
	sinkOper.Clear()
	err = injectBatchInFS(fs, ib, &testExecCtx{
		version:     version,
		partitionID: 0,
		processor:   pm.GetProcessor(partition0Processor),
	})

	require.NoError(t, err)

	ib = createBatchForInjection(schema, 130, 100, 5)
	sinkOper.Clear()
	err = injectBatchInFS(fs, ib, &testExecCtx{
		version:     version,
		partitionID: 5,
		processor:   pm.GetProcessor(partition5Processor),
		backFill:    false,
	})
	require.NoError(t, err)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[0], 0, 100, 130)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[0], 5, 100, 130)

	// Write some rows in partitions 1 and 4
	sinkOper.Clear()
	writeRowsToPartition(t, 1, numRowsPerPartition, fromSlabID, pm.GetStore(), version)
	writeRowsToPartition(t, 4, numRowsPerPartition, fromSlabID, pm.GetStore(), version)

	partition1Processor, ok := partitionProcessorMapping[1]
	require.True(t, ok)
	partition4Processor, ok := partitionProcessorMapping[4]
	require.True(t, ok)

	require.NotEqual(t, partition1Processor, partition4Processor)
	require.NotEqual(t, partition0Processor, partition1Processor)

	// These partitions are owned by processors which are not active yet, so no data should be received
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(sinkOper.GetPartitionBatches()))

	// Now make those processors active
	pm.AddActiveProcessor(partition1Processor)
	pm.AddActiveProcessor(partition4Processor)

	// Data should be received
	waitForRows(t, 2*numRowsPerPartition, sinkOper)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[1], 1, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[4], 4, 0, numRowsPerPartition)

	//time.Sleep(250 * time.Millisecond)

	// activate the rest of the processors
	for _, procID := range fs.OutSchema().PartitionScheme.ProcessorIDs {
		if procID != partition0Processor && procID != partition1Processor && procID != partition4Processor &&
			procID != partition5Processor {
			pm.AddActiveProcessor(procID)
		}
	}

	// All partitions should now be not lagging, so should accept injected batches
	sinkOper.Clear()
	numRows2 := 65
	for partID := 0; partID < 9; partID++ {
		offsetStart := 0
		if partID == 0 || partID == 1 || partID == 4 || partID == 5 {
			offsetStart = 100
		}
		ib = createBatchForInjection(schema, numRows2, offsetStart, partID)
		procID, ok := partitionProcessorMapping[partID]
		require.True(t, ok)
		err = injectBatchInFS(fs, ib, &testExecCtx{
			version:     version,
			partitionID: partID,
			processor:   pm.GetProcessor(procID),
		})
		require.NoError(t, err)
	}

	waitForRows(t, 9*numRows2, sinkOper)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[1], 1, 100, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[4], 4, 100, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[0], 0, 100, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[5], 5, 100, numRowsPerPartition)

	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[2], 2, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[3], 3, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[6], 6, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[7], 7, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[8], 8, 0, numRowsPerPartition)
	verifyPartitionBatches(t, sinkOper.GetPartitionBatches()[9], 9, 0, numRowsPerPartition)
}

func injectBatchInFS(fs *BackfillOperator, batch *evbatch.Batch, ctx StreamExecContext) error {
	ch := make(chan error, 1)
	pb := proc.NewProcessBatch(ctx.Processor().ID(), batch, fs.receiverID, ctx.PartitionID(), -1)
	ctx.Processor().IngestBatch(pb, func(err error) {
		ch <- err
	})
	return <-ch
}

func TestBackfillStoreLoadOffsets(t *testing.T) {

	maxBackFillBatchSize := 50
	fromTableID := 1234
	offsetsSlabID := 1235
	fs, _, _ := setup(t, maxBackFillBatchSize, fromTableID, offsetsSlabID, 9)

	store := tppm.NewTestStore()

	for i := 0; i < 9; i++ {
		ctx := newLoadExecCtx(i, store)
		_, ok, err := fs.loadCommittedOffsetForPartition(ctx)
		require.NoError(t, err)
		require.False(t, ok)
	}

	mb := mem.NewBatch()
	ctx1 := &loadExecCtx{
		partID:  2,
		store:   store,
		entries: mb,
	}

	fs.storeCommittedOffSetForPartition(2234, ctx1)
	require.Equal(t, 1, mb.Len())
	writeEntriesToStore(t, mb, store)

	ctx2 := &loadExecCtx{
		partID:  2,
		store:   store,
		entries: mb,
	}

	off, ok, err := fs.loadCommittedOffsetForPartition(ctx2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2234), off)

	for i := 0; i < 9; i++ {
		if i == 2 {
			continue
		}
		ctx := newLoadExecCtx(i, store)
		_, ok, err := fs.loadCommittedOffsetForPartition(ctx)
		require.NoError(t, err)
		require.False(t, ok)
	}
	for i := 0; i < 9; i++ {
		if i == 2 {
			continue
		}
		mb := mem.NewBatch()
		ctx := &loadExecCtx{
			partID:  i,
			store:   store,
			entries: mb,
		}
		fs.storeCommittedOffSetForPartition(int64(i*1000+234), ctx)
		require.Equal(t, 1, mb.Len())
		writeEntriesToStore(t, mb, store)
	}

	for i := 0; i < 9; i++ {
		ctx := newLoadExecCtx(i, store)
		off, ok, err := fs.loadCommittedOffsetForPartition(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(i*1000+234), off)
	}
}

func writeEntriesToStore(t *testing.T, mb *mem.Batch, store tppm.Store) {
	err := store.Write(mb)
	require.NoError(t, err)
}

type loadExecCtx struct {
	partID  int
	store   tppm.Store
	entries *mem.Batch
}

func (l *loadExecCtx) ReceiverID() int {
	return 0
}

func (l *loadExecCtx) EventBatchBytes() []byte {
	return nil
}

func newLoadExecCtx(partID int, store tppm.Store) *loadExecCtx {
	return &loadExecCtx{
		partID:  partID,
		store:   store,
		entries: mem.NewBatch(),
	}
}

func (l *loadExecCtx) StoreEntry(kv common.KV, _ bool) {
	l.entries.AddEntry(kv)
}

func (l *loadExecCtx) ForwardEntry(int, int, int, int, *evbatch.Batch, *evbatch.EventSchema) {
}

func (l *loadExecCtx) ForwardBarrier(int, int) {
}

func (l *loadExecCtx) CheckInProcessorLoop() {
}

func (l *loadExecCtx) IngestBatch() error {
	return nil
}

func (l *loadExecCtx) WriteVersion() int {
	return 0
}

func (l *loadExecCtx) PartitionID() int {
	return l.partID
}

func (l *loadExecCtx) ForwardingProcessorID() int {
	return 0
}

func (l *loadExecCtx) ForwardSequence() int {
	return 0
}

func (l *loadExecCtx) Processor() proc.Processor {
	return nil
}

func (l *loadExecCtx) Get(key []byte) ([]byte, error) {
	return l.store.GetWithMaxVersion(key, math.MaxUint64)
}

func (l *loadExecCtx) BackFill() bool {
	return false
}

func (l *loadExecCtx) WaterMark() int {
	return 0
}

func (l *loadExecCtx) SetWaterMark(int) {
}

func createBatchForInjection(schema *evbatch.EventSchema, numRows int, offsetStart int, partID int) *evbatch.Batch {
	builders := evbatch.CreateColBuilders(schema.ColumnTypes())
	for i := 0; i < numRows; i++ {
		builders[0].(*evbatch.IntColBuilder).Append(int64(i + offsetStart))
		builders[1].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(int64(i + offsetStart)))
		builders[2].(*evbatch.StringColBuilder).Append(fmt.Sprintf("foo-part-%d-%d", partID, i+offsetStart))
		builders[3].(*evbatch.FloatColBuilder).Append(float64(10*partID+i+offsetStart) + 0.1)
	}
	return evbatch.NewBatchFromBuilders(schema, builders...)
}

func setup(t *testing.T, maxBackFillBatchSize int, fromSlabID int, offsetsSlabID int, partitionCount int) (*BackfillOperator, *testSinkOper, *evbatch.EventSchema) {
	schema := evbatch.NewEventSchema([]string{"offset", "event_time", "f1", "f2"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeFloat})

	receiverID := 1001
	operSchema := &OperatorSchema{EventSchema: schema,
		PartitionScheme: NewPartitionScheme("bar", partitionCount, false, 48)}
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	fs := NewBackfillOperator(operSchema, cfg, fromSlabID, offsetsSlabID, maxBackFillBatchSize, receiverID, false)
	sinkOper := newTestSinkOper(operSchema)
	fs.AddDownStreamOperator(sinkOper)

	return fs, sinkOper, schema
}

func writeRowsToPartition(t *testing.T, partID int, numRows int, tableID int, store tppm.Store, version int) {
	mb := mem.NewBatch()
	for j := 0; j < numRows; j++ {
		partitionHash := proc.CalcPartitionHash("bar", uint64(partID))
		keyPrefix := encoding.EncodeEntryPrefix(partitionHash, uint64(tableID), 41)
		key := append(keyPrefix, 1) // not null byte
		key = encoding.KeyEncodeInt(key, int64(j))
		key = encoding.EncodeVersion(key, uint64(version))
		value := []byte{1}
		value = encoding.AppendUint64ToBufferLE(value, uint64(j))
		value = append(value, 1)
		value = encoding.AppendStringToBufferLE(value, fmt.Sprintf("foo-part-%d-%d", partID, j))
		value = append(value, 1)
		value = encoding.AppendFloat64ToBufferLE(value, float64(10*partID+j)+0.1)
		mb.AddEntry(common.KV{
			Key:   key,
			Value: value,
		})
	}
	err := store.Write(mb)
	require.NoError(t, err)
}

func waitForRows(t *testing.T, expectedRows int, sinkOper *testSinkOper) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		partitionBatches := sinkOper.GetPartitionBatches()
		numRows := 0
		for _, batches := range partitionBatches {
			for _, batch := range batches {
				numRows += batch.RowCount
			}
		}
		//log.Infof("num rows %d expected %d", numRows, expectedRows)
		return numRows == expectedRows, nil
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func verifyPartitionBatches(t *testing.T, partitionBatches []*evbatch.Batch, partID int,
	offsetStart int, numRows int) {
	for _, batch := range partitionBatches {
		batchRows := numRows
		if batchRows > batch.RowCount {
			batchRows = batch.RowCount
		}
		verifyBatchRows(t, batch, uint64(partID), offsetStart, batchRows)
		offsetStart += batchRows
		numRows -= batchRows
	}
}

func verifyBatchRows(t *testing.T, batch *evbatch.Batch, partID uint64, offsetStart int, numRows int) {
	require.Equal(t, numRows, batch.RowCount)
	for i := 0; i < batch.RowCount; i++ {
		off := int64(offsetStart + i)
		require.Equal(t, off, batch.GetIntColumn(0).Get(i))
		require.Equal(t, off, batch.GetTimestampColumn(1).Get(i).Val)
		require.Equal(t, fmt.Sprintf("foo-part-%d-%d", partID, off), batch.GetStringColumn(2).Get(i))
		require.Equal(t, float64(10*partID+uint64(off))+0.1, batch.GetFloatColumn(3).Get(i))
	}
}

type testStreamExecCtx struct {
	pm ProcessorManager
}

func (t *testStreamExecCtx) RegisterReceiver(int, Receiver) {
}

func (t *testStreamExecCtx) UnregisterReceiver(int) {
}

func (t *testStreamExecCtx) ProcessorManager() ProcessorManager {
	return t.pm
}

type operatorBatchHandler struct {
	oper Operator
}

func (o *operatorBatchHandler) GetInjectableReceiverIDs() []int {
	return nil
}

func (o *operatorBatchHandler) GetForwardInfoForReceiver() ([]int, bool) {
	return nil, false
}

func (o *operatorBatchHandler) GetBarrierAwareReceivers() int {
	return 0
}

func (o *operatorBatchHandler) HandleProcessBatch(processor proc.Processor, processBatch *proc.ProcessBatch,
	_ bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	ctx := &testExecCtx{
		version:     processBatch.Version,
		partitionID: processBatch.PartitionID,
		processor:   processor,
		backFill:    processBatch.BackFill,
	}
	mb := mem.NewBatch()
	for _, entry := range ctx.entries {
		mb.AddEntry(entry)
	}
	_, err := o.oper.HandleStreamBatch(processBatch.EvBatch, ctx)
	return true, mb, nil, err
}
