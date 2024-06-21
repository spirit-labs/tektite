// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opers

import (
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPartitionOperator(t *testing.T) {
	testPartitionOperator(t, 10)
	testPartitionOperator(t, 100)
}

func testPartitionOperator(t *testing.T, numPartitions int) {
	fNames := []string{"offset", "event_time", "f0", "f1", "f2", "f3", "f4", "f5", "f6"}
	decType := &types.DecimalType{
		Precision: 12,
		Scale:     4,
	}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	schema := evbatch.NewEventSchema(fNames, fTypes)

	keyCols := []string{"f0"}

	forwardReceiverID := 777
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	po, err := NewPartitionOperator(&OperatorSchema{EventSchema: schema}, keyCols, numPartitions, forwardReceiverID,
		"test_stream", cfg, &parser.PartitionDesc{})
	require.NoError(t, err)

	numRows := 1000
	var dataIn [][]any
	var expectedOut [][]any
	for i := 0; i < numRows; i++ {
		row := []any{int64(i), types.NewTimestamp(int64(i)), int64(i), float64(1.23), true, types.Decimal{Num: decimal128.New(0, 1000000), Precision: 12, Scale: 4},
			"foo", []byte("bar"), types.NewTimestamp(1234)}
		dataIn = append(dataIn, row)
		expectedOut = append(expectedOut, row[1:]) // remove offset
	}
	batch := createEventBatch(fNames, fTypes, dataIn)
	ppm := po.outSchema.PartitionScheme.PartitionProcessorMapping
	testProc := &testProcessor{id: 12}
	execCtx := &execContext{processor: testProc}
	_, err = po.HandleStreamBatch(batch, execCtx)
	require.NoError(t, err)
	forwardBatches := execCtx.GetForwardBatches()
	require.LessOrEqual(t, len(forwardBatches), numPartitions)
	rc := 0
	var dataOut [][]any
	// Remove offset col
	for _, pb := range forwardBatches {
		require.Equal(t, forwardReceiverID, pb.ReceiverID)
		require.Equal(t, ppm[pb.PartitionID], pb.ProcessorID)
		require.Equal(t, ppm[pb.PartitionID], pb.ProcessorID)
		require.Equal(t, testProc.id, pb.ForwardingProcessorID)
		eb := pb.EvBatch
		rc += eb.RowCount
		arr := convertBatchToAnyArray(eb)
		dataOut = append(dataOut, arr...)
	}
	require.Equal(t, numRows, rc)

	sortDataByKeyCols(dataOut, []int{1}, []types.ColumnType{types.ColumnTypeInt})
	require.Equal(t, expectedOut, dataOut)
}

func TestDedup(t *testing.T) {
	fNames := []string{"offset", "event_time", "f0"}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt}
	schema := evbatch.NewEventSchema(fNames, fTypes)
	numProcessors := 48
	numPartitions := 10
	keyCols := []string{"f0"}
	forwardReceiverID := 777
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ProcessorCount = numProcessors
	po, err := NewPartitionOperator(&OperatorSchema{EventSchema: schema}, keyCols, numPartitions, forwardReceiverID,
		"test_stream", cfg, &parser.PartitionDesc{})
	require.NoError(t, err)
	co := &capturingOperator{}
	po.AddDownStreamOperator(co)

	batch1 := injectBatchToReceiver(t, schema, po, 0, 5, 9)
	batch2 := injectBatchToReceiver(t, schema, po, 1, 5, 9)
	batch3 := injectBatchToReceiver(t, schema, po, 2, 5, 9)
	require.Equal(t, []*evbatch.Batch{batch1, batch2, batch3}, co.getBatches())

	injectBatchToReceiver(t, schema, po, 1, 5, 9)
	require.Equal(t, []*evbatch.Batch{batch1, batch2, batch3}, co.getBatches())

	injectBatchToReceiver(t, schema, po, 2, 5, 9)
	require.Equal(t, []*evbatch.Batch{batch1, batch2, batch3}, co.getBatches())

	batch4 := injectBatchToReceiver(t, schema, po, 3, 5, 9)
	require.Equal(t, []*evbatch.Batch{batch1, batch2, batch3, batch4}, co.getBatches())

	batch5 := injectBatchToReceiver(t, schema, po, 4, 5, 9)
	require.Equal(t, []*evbatch.Batch{batch1, batch2, batch3, batch4, batch5}, co.getBatches())

	co.resetBatches()
	batch2_1 := injectBatchToReceiver(t, schema, po, 0, 6, 9)
	batch2_2 := injectBatchToReceiver(t, schema, po, 1, 6, 9)
	batch2_3 := injectBatchToReceiver(t, schema, po, 2, 6, 9)
	require.Equal(t, []*evbatch.Batch{batch2_1, batch2_2, batch2_3}, co.getBatches())

	co.resetBatches()
	batch3_1 := injectBatchToReceiver(t, schema, po, 0, 5, 8)
	batch3_2 := injectBatchToReceiver(t, schema, po, 1, 5, 8)
	batch3_3 := injectBatchToReceiver(t, schema, po, 2, 5, 8)
	require.Equal(t, []*evbatch.Batch{batch3_1, batch3_2, batch3_3}, co.getBatches())
}

func injectBatchToReceiver(t *testing.T, schema *evbatch.EventSchema, po *PartitionOperator,
	forwardSeq int, forwardingProcessorID int, processorID int) *evbatch.Batch {
	ec := &testExecCtx{
		forwardingProcessorID: forwardingProcessorID,
		forwardSequence:       forwardSeq,
		processor:             &testProcessor{id: processorID},
	}
	batch := createBatch(schema)
	_, err := po.receiver.ReceiveBatch(batch, ec)
	require.NoError(t, err)
	return batch
}

func createBatch(schema *evbatch.EventSchema) *evbatch.Batch {
	colBuilders := evbatch.CreateColBuilders(schema.ColumnTypes())
	batch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
	return batch
}

type capturingOperator struct {
	lock    sync.Mutex
	batches []*evbatch.Batch
}

func (c *capturingOperator) getBatches() []*evbatch.Batch {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.batches
}

func (c *capturingOperator) resetBatches() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.batches = nil
}

func (c *capturingOperator) HandleStreamBatch(batch *evbatch.Batch, _ StreamExecContext) (*evbatch.Batch, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.batches = append(c.batches, batch)
	return nil, nil
}

func (c *capturingOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	return nil, nil
}

func (c *capturingOperator) HandleBarrier(StreamExecContext) error {
	return nil
}

func (c *capturingOperator) InSchema() *OperatorSchema {
	return nil
}

func (c *capturingOperator) OutSchema() *OperatorSchema {
	return nil
}

func (c *capturingOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (c *capturingOperator) AddDownStreamOperator(Operator) {
}

func (c *capturingOperator) GetDownStreamOperators() []Operator {
	return nil
}

func (c *capturingOperator) RemoveDownStreamOperator(Operator) {

}

func (c *capturingOperator) GetParentOperator() Operator {
	return nil
}

func (c *capturingOperator) SetParentOperator(Operator) {
}

func (c *capturingOperator) SetStreamInfo(*StreamInfo) {
}

func (c *capturingOperator) GetStreamInfo() *StreamInfo {
	return nil
}

func (c *capturingOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}

func (t testProcessor) SetLeader() {
}

func (t testProcessor) SetReplicator(proc.Replicator) {
}

func (t testProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (t testProcessor) SubmitAction(func() error) bool {
	return false
}

type testProcessor struct {
	id int
}

func (t testProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}

func (t testProcessor) WriteCache() *proc.WriteCache {
	return nil
}

func (t testProcessor) CloseVersion(int, []int) {
}

func (t testProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (t testProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (t testProcessor) IsStopped() bool {
	return false
}

func (t testProcessor) SetBarriersInjected() {
}

func (t testProcessor) SetNotIdleNotifier(func()) {
}

func (t testProcessor) IsIdle(int) bool {
	return false
}

func (t testProcessor) InvalidateCachedReceiverInfo() {
}

func (t testProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (t testProcessor) ID() int {
	return t.id
}

func (t testProcessor) IngestBatch(*proc.ProcessBatch, func(error)) {
}

func (t testProcessor) IngestBatchSync(*proc.ProcessBatch) error {
	return nil
}

func (t testProcessor) IsLeader() bool {
	return true
}

func (t testProcessor) CheckInProcessorLoop() {
}

func (t testProcessor) Pause() {
}

func (t testProcessor) Unpause() {
}

func (t testProcessor) Stop() {
}
