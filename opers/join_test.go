package opers

import (
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"sync"
	"testing"
	"time"
)

/*
Most of the join testing is done in script test
*/

func TestInnerJoinSingleRowSingleJoinColLeftBeforeRight(t *testing.T) {
	testInnerJoinSingleRowSingleJoinCol(t, true)
}

func TestInnerJoinSingleRowSingleJoinColRightBeforeLeft(t *testing.T) {
	testInnerJoinSingleRowSingleJoinCol(t, false)
}

func testInnerJoinSingleRowSingleJoinCol(t *testing.T, leftBeforeRight bool) {

	costType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	leftSchema := evbatch.NewEventSchema([]string{EventTimeColName, "tx_id", "cust_id", "prod_id", "quantity", "cost"},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeInt, costType})

	rightSchema := evbatch.NewEventSchema([]string{EventTimeColName, "customer_id", "payment_id", "amount", "currency"},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeString, types.ColumnTypeInt, costType, types.ColumnTypeString})

	resSchema := evbatch.NewEventSchema([]string{"event_time", "l_event_time", "l_tx_id", "l_cust_id", "l_prod_id", "l_quantity", "l_cost", "r_event_time", "r_payment_id", "r_amount", "r_currency"},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeInt, costType,
			types.ColumnTypeTimestamp, types.ColumnTypeInt, costType, types.ColumnTypeString})

	joinElements := []parser.JoinElement{{
		LeftCol:  "cust_id",
		RightCol: "customer_id",
		JoinType: "=",
	}}
	join, out, left, right, st := setupJoinOperator(t, leftSchema, rightSchema, joinElements)
	defer stopStore(t, st)

	leftBuilders := evbatch.CreateColBuilders(leftSchema.ColumnTypes())
	leftBuilders[0].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(100000))
	leftBuilders[1].(*evbatch.IntColBuilder).Append(1000)
	leftBuilders[2].(*evbatch.StringColBuilder).Append("customer-1234")
	leftBuilders[3].(*evbatch.StringColBuilder).Append("prod-3000")
	leftBuilders[4].(*evbatch.IntColBuilder).Append(23)
	leftBuilders[5].(*evbatch.DecimalColBuilder).Append(createDecimal(t, "23.76"))
	leftBatch := evbatch.NewBatchFromBuilders(leftSchema, leftBuilders...)

	rightBuilders := evbatch.CreateColBuilders(rightSchema.ColumnTypes())
	rightBuilders[0].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(200000))
	rightBuilders[1].(*evbatch.StringColBuilder).Append("customer-1234")
	rightBuilders[2].(*evbatch.IntColBuilder).Append(76543)
	rightBuilders[3].(*evbatch.DecimalColBuilder).Append(createDecimal(t, "25.76"))
	rightBuilders[4].(*evbatch.StringColBuilder).Append("GBP")

	rightBatch := evbatch.NewBatchFromBuilders(rightSchema, rightBuilders...)

	resBatch := injectBatchesAndWaitForResult(t, leftBatch, rightBatch, leftBeforeRight, left, right, join, st, out)

	require.Equal(t, 1, resBatch.RowCount)
	require.Equal(t, 11, len(resBatch.Columns))

	verifyJoined(t, leftBatch, rightBatch, 0, 0, 0, resBatch, resSchema)
}

func setupJoinOperator(t *testing.T, leftSchema *evbatch.EventSchema,
	rightSchema *evbatch.EventSchema, joinElements []parser.JoinElement) (*JoinOperator, *testSinkOper, Operator, Operator, *store2.Store) {
	mappingID := "test_mapping_id"
	partitions := 10
	processorCount := 48
	partitionScheme := NewPartitionScheme(mappingID, partitions, false, processorCount)

	leftOperSchema := &OperatorSchema{
		EventSchema:     leftSchema,
		PartitionScheme: partitionScheme,
	}
	left := &testSourceOper{
		schema: leftOperSchema,
	}
	rightOperSchema := &OperatorSchema{
		EventSchema:     rightSchema,
		PartitionScheme: partitionScheme,
	}
	right := &testSourceOper{
		schema: rightOperSchema,
	}
	leftSlabID := 1000
	rightSlabID := 1001
	receiverID := 2000
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	within := 5 * time.Minute
	join, err := NewJoinOperator(leftSlabID, rightSlabID, left, right, false, false,
		nil, nil,
		joinElements, within, st, 0, receiverID, &parser.JoinDesc{})
	require.NoError(t, err)
	left.AddDownStreamOperator(join.leftInput)
	right.AddDownStreamOperator(join.rightInput)
	out := newTestSinkOper(join.OutSchema())
	join.AddDownStreamOperator(out)
	return join, out, left, right, st
}

func injectBatchesAndWaitForResult(t *testing.T, leftBatch *evbatch.Batch, rightBatch *evbatch.Batch, leftBeforeRight bool, left Operator, right Operator,
	join *JoinOperator, st *store2.Store, out *testSinkOper) *evbatch.Batch {
	procID := injectBatches(t, leftBatch, rightBatch, leftBeforeRight, left, right, join, st)
	res := waitForBatchesOnProcessor(t, procID, 1, out)[0]
	res.Dump()
	return res
}

func injectBatches(t *testing.T, leftBatch *evbatch.Batch, rightBatch *evbatch.Batch, leftBeforeRight bool, left Operator, right Operator,
	join *JoinOperator, st *store2.Store) int {
	partitionScheme := join.outSchema.PartitionScheme
	procID := partitionScheme.ProcessorIDs[0]
	partitionID := partitionScheme.ProcessorPartitionMapping[procID][0]
	processor := newJoinTestProcessor(procID, join.batchReceiver, st)

	execCtx := &testExecCtx{
		version:               0,
		partitionID:           partitionID,
		forwardingProcessorID: -1,
		processor:             processor,
	}

	if leftBeforeRight {
		_, err := left.HandleStreamBatch(leftBatch, execCtx)
		require.NoError(t, err)
		_, err = right.HandleStreamBatch(rightBatch, execCtx)
		require.NoError(t, err)
	} else {
		_, err := right.HandleStreamBatch(rightBatch, execCtx)
		require.NoError(t, err)
		_, err = left.HandleStreamBatch(leftBatch, execCtx)
		require.NoError(t, err)
	}
	return procID
}

func verifyJoined(t *testing.T, leftBatch *evbatch.Batch, rightBatch *evbatch.Batch, leftRow int, rightRow int, resRow int, resBatch *evbatch.Batch, expectedSchema *evbatch.EventSchema) {
	leftEventTime := leftBatch.GetTimestampColumn(leftRow).Get(leftRow).Val
	require.Equal(t, leftEventTime, resBatch.GetTimestampColumn(1).Get(resRow).Val)
	require.Equal(t, leftBatch.GetIntColumn(1).Get(leftRow), resBatch.GetIntColumn(2).Get(resRow))
	require.Equal(t, leftBatch.GetStringColumn(2).Get(leftRow), resBatch.GetStringColumn(3).Get(resRow))
	require.Equal(t, leftBatch.GetStringColumn(3).Get(leftRow), resBatch.GetStringColumn(4).Get(resRow))
	require.Equal(t, leftBatch.GetIntColumn(4).Get(leftRow), resBatch.GetIntColumn(5).Get(resRow))
	require.Equal(t, leftBatch.GetDecimalColumn(5).Get(leftRow), resBatch.GetDecimalColumn(6).Get(resRow))

	rightEventTime := rightBatch.GetTimestampColumn(0).Get(rightRow).Val
	require.Equal(t, rightEventTime, resBatch.GetTimestampColumn(7).Get(resRow).Val)
	require.Equal(t, rightBatch.GetIntColumn(2).Get(rightRow), resBatch.GetIntColumn(8).Get(resRow))
	require.Equal(t, rightBatch.GetDecimalColumn(3).Get(rightRow), resBatch.GetDecimalColumn(9).Get(resRow))
	require.Equal(t, rightBatch.GetStringColumn(4).Get(rightRow), resBatch.GetStringColumn(10).Get(resRow))

	maxEventTime := leftEventTime
	if rightEventTime > maxEventTime {
		maxEventTime = rightEventTime
	}
	require.Equal(t, maxEventTime, resBatch.GetTimestampColumn(0).Get(resRow).Val)

	require.Equal(t, expectedSchema, resBatch.Schema)
}

func waitForBatchesOnProcessor(t *testing.T, procID int, numBatches int, sink *testSinkOper) []*evbatch.Batch {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		batches, ok := sink.GetProcessorBatches()[procID]
		if !ok {
			return false, nil
		}
		return len(batches) == numBatches, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
	return sink.GetProcessorBatches()[procID]
}

type joinTestProcessor struct {
	id         int
	receiver   Receiver
	writeCache *proc.WriteCache
	st         store
	lock       sync.Mutex
}

func newJoinTestProcessor(id int, receiver Receiver, st store) *joinTestProcessor {
	return &joinTestProcessor{
		id:         id,
		receiver:   receiver,
		writeCache: proc.NewWriteCache(st, math.MaxInt64, id),
		st:         st,
	}
}

func (j *joinTestProcessor) ID() int {
	return j.id
}

func (j *joinTestProcessor) IngestBatch(processBatch *proc.ProcessBatch, completionFunc func(error)) {
	j.lock.Lock() // Make sure only one runs at once
	go func() {
		defer j.lock.Unlock()
		execCtx := &execContext{
			processBatch: processBatch,
			processor:    j,
		}
		if _, err := j.receiver.ReceiveBatch(processBatch.EvBatch, execCtx); err != nil {
			panic(err)
		}
		err := j.st.Write(execCtx.entries)
		completionFunc(err)
	}()
}

func (j *joinTestProcessor) IngestBatchSync(*proc.ProcessBatch) error {
	return nil
}

func (j *joinTestProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (j *joinTestProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (j *joinTestProcessor) SetLeader() {
}

func (j *joinTestProcessor) IsLeader() bool {
	return true
}

func (j *joinTestProcessor) CheckInProcessorLoop() {
}

func (j *joinTestProcessor) Stop() {
}

func (j *joinTestProcessor) InvalidateCachedReceiverInfo() {
}

func (j *joinTestProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (j *joinTestProcessor) SetNotIdleNotifier(func()) {
}

func (j *joinTestProcessor) IsIdle(int) bool {
	return false
}

func (j *joinTestProcessor) IsStopped() bool {
	return false
}

func (j *joinTestProcessor) SetReplicator(proc.Replicator) {
}

func (j *joinTestProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (j *joinTestProcessor) SubmitAction(func() error) bool {
	return false
}

func (j *joinTestProcessor) CloseVersion(int, []int) {
}

func (j *joinTestProcessor) WriteCache() *proc.WriteCache {
	return j.writeCache
}

func (j *joinTestProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}
