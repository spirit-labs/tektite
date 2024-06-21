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
	"github.com/spirit-labs/tektite/evbatch"
	"sync"
)

func newTestSinkOper(schema *OperatorSchema) *testSinkOper {
	return &testSinkOper{
		schema:             schema,
		batchesByProcessor: map[int][]*evbatch.Batch{},
		batchesByPartition: map[int][]*evbatch.Batch{},
	}
}

type testSinkOper struct {
	lock               sync.Mutex
	schema             *OperatorSchema
	batchesByProcessor map[int][]*evbatch.Batch
	batchesByPartition map[int][]*evbatch.Batch
	parentOperator     Operator
}

func (ts *testSinkOper) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in streams")
}

func (ts *testSinkOper) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	batchesByProc := ts.batchesByProcessor[execCtx.Processor().ID()]
	batchesByProc = append(batchesByProc, batch)
	ts.batchesByProcessor[execCtx.Processor().ID()] = batchesByProc

	batchesByPart := ts.batchesByPartition[execCtx.PartitionID()]
	batchesByPart = append(batchesByPart, batch)
	ts.batchesByPartition[execCtx.PartitionID()] = batchesByPart

	return nil, nil
}

func (ts *testSinkOper) HandleBarrier(StreamExecContext) error {
	return nil
}

func (ts *testSinkOper) GetProcessorBatches() map[int][]*evbatch.Batch {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	return copyBatches(ts.batchesByProcessor)
}

func (ts *testSinkOper) GetPartitionBatches() map[int][]*evbatch.Batch {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	return copyBatches(ts.batchesByPartition)
}

func copyBatches(batches map[int][]*evbatch.Batch) map[int][]*evbatch.Batch {
	cp := make(map[int][]*evbatch.Batch, len(batches))
	for k, v := range batches {
		cp[k] = v
	}
	return cp
}

func (ts *testSinkOper) Clear() {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	ts.batchesByProcessor = map[int][]*evbatch.Batch{}
	ts.batchesByPartition = map[int][]*evbatch.Batch{}
}

func (ts *testSinkOper) InSchema() *OperatorSchema {
	return ts.schema
}

func (ts *testSinkOper) OutSchema() *OperatorSchema {
	return ts.schema
}

func (ts *testSinkOper) AddDownStreamOperator(Operator) {
}

func (ts *testSinkOper) GetDownStreamOperators() []Operator {
	return nil
}

func (ts *testSinkOper) RemoveDownStreamOperator(Operator) {
}

func (ts *testSinkOper) GetParentOperator() Operator {
	return ts.parentOperator
}

func (ts *testSinkOper) SetParentOperator(operator Operator) {
	ts.parentOperator = operator
}

func (ts *testSinkOper) SetStreamInfo(*StreamInfo) {
}

func (ts *testSinkOper) GetStreamInfo() *StreamInfo {
	return nil
}

func (ts *testSinkOper) Setup(StreamManagerCtx) error {
	return nil
}

func (ts *testSinkOper) Teardown(StreamManagerCtx, *sync.RWMutex) {
}

type testSourceOper struct {
	schema       *OperatorSchema
	nextOperator Operator
	receiverID   int
}

func (t *testSourceOper) GetParentOperator() Operator {
	return nil
}

func (t *testSourceOper) SetParentOperator(Operator) {
}

func (t *testSourceOper) ForwardingProcessorCount() int {
	return len(t.schema.PartitionScheme.ProcessorIDs)
}

func (t *testSourceOper) HandleBarrier(execCtx StreamExecContext) error {
	return t.nextOperator.HandleBarrier(execCtx)
}

func (t *testSourceOper) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return t.nextOperator.HandleStreamBatch(batch, execCtx)
}

func (t *testSourceOper) ReceiveBarrier(execCtx StreamExecContext) error {
	return t.nextOperator.HandleBarrier(execCtx)
}

func (t *testSourceOper) RequiresBarriersInjection() bool {
	return false
}

func (t *testSourceOper) Schema() *evbatch.EventSchema {
	return t.schema.EventSchema
}

func (t *testSourceOper) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not used")
}

func (t *testSourceOper) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return t.nextOperator.HandleStreamBatch(batch, execCtx)
}

func (t *testSourceOper) InSchema() *OperatorSchema {
	return t.schema
}

func (t *testSourceOper) OutSchema() *OperatorSchema {
	return t.schema
}

func (t *testSourceOper) AddDownStreamOperator(downstream Operator) {
	t.nextOperator = downstream
}

func (t *testSourceOper) GetDownStreamOperators() []Operator {
	return []Operator{t.nextOperator}
}

func (t *testSourceOper) RemoveDownStreamOperator(Operator) {
}

func (t *testSourceOper) SetStreamInfo(*StreamInfo) {
}

func (t *testSourceOper) GetStreamInfo() *StreamInfo {
	return nil
}

func (t *testSourceOper) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(t.receiverID, t)
	return nil
}

func (t *testSourceOper) Teardown(mgr StreamManagerCtx, _ *sync.RWMutex) {
	mgr.UnregisterReceiver(t.receiverID)
}
