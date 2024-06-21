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
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"sync"
)

func NewUnionOperator(receiverID int, inputs []Operator) *UnionOperator {

	schema := inputs[0].OutSchema()
	var outEventSchema *evbatch.EventSchema
	if HasOffsetColumn(schema.EventSchema) {
		outEventSchema = evbatch.NewEventSchema(schema.EventSchema.ColumnNames()[1:], schema.EventSchema.ColumnTypes()[1:])
	} else {
		outEventSchema = schema.EventSchema
	}
	outSchema := &OperatorSchema{
		EventSchema:     outEventSchema,
		PartitionScheme: schema.PartitionScheme,
	}
	uo := &UnionOperator{
		receiverID: receiverID,
		inputs:     inputs,
		schema:     outSchema,
	}
	procIDs := map[int]struct{}{} // Unique set of processor ids
	for _, input := range inputs {
		for _, procID := range input.OutSchema().ProcessorIDs {
			procIDs[procID] = struct{}{}
		}
	}
	maxProcID := -1
	for procID := range procIDs {
		uo.forwardProcIDs = append(uo.forwardProcIDs, procID)
		if procID > maxProcID {
			maxProcID = procID
		}
	}
	uo.forwardingProcCount = len(procIDs)

	// For each sending processor - we maintain a map of receiver id to last barrier version forwarded
	uo.procReceiverBarrierVersions = make([]map[int]int, maxProcID+1)
	for _, procID := range uo.forwardProcIDs {
		uo.procReceiverBarrierVersions[procID] = map[int]int{}
	}
	return uo
}

type UnionOperator struct {
	BaseOperator
	receiverID                  int
	inputs                      []Operator
	schema                      *OperatorSchema
	forwardingProcCount         int
	forwardProcIDs              []int
	procReceiverBarrierVersions []map[int]int
}

func (u *UnionOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	// forward batch to receiver (see comment in JoinOperator for why we do this)
	removeOffset := batch.Schema.ColumnNames()[0] == OffsetColName
	var forwardBatch *evbatch.Batch
	if removeOffset {
		// We remove offset as different inputs can have same offset so would not be meaningful after union
		forwardBatch = evbatch.NewBatch(u.schema.EventSchema, batch.Columns[1:]...)
	} else {
		forwardBatch = evbatch.NewBatch(u.schema.EventSchema, batch.Columns...)
	}
	pid := execCtx.Processor().ID()
	pb := proc.NewProcessBatch(pid, forwardBatch, u.receiverID, execCtx.PartitionID(), pid)
	pb.Version = execCtx.WriteVersion()
	execCtx.Processor().IngestBatch(pb, func(err error) {
		if err != nil {
			log.Errorf("failed to forward batch for join: %v", err)
		}
	})
	return nil, nil
}

func (u *UnionOperator) HandleBarrier(execCtx StreamExecContext) error {
	// We forward barriers to all processors that the union can have inputs from - this means those barriers will be
	// delayed in the receiving processors and not output until barriers have been received from all expected processors.
	// If we processed directly without forwarding we would not know what version to assign a batch to, as different inputs
	// can have different versions.
	// However, when different inputs are coming from the same receiver, we will receive the barrier for each
	// input, but we don't want to forward it multiple times for each sending receiver -just once. So we have some
	// dedup code here that checks the version of the last barrier forwarded for the sending receiver, and doesn't
	// forward it if its already been forwarded. We maintain one such map per sending processor to avoid having to
	// lock access to a single map and create a point of contention, as all events on processor are processed on
	// same event loop.
	sendingProcessorID := execCtx.Processor().ID()
	lastSentMap := u.procReceiverBarrierVersions[sendingProcessorID]
	lastSent, ok := lastSentMap[execCtx.ReceiverID()]
	if ok && lastSent == execCtx.WriteVersion() {
		return nil
	}
	for _, processorID := range u.forwardProcIDs {
		execCtx.ForwardBarrier(processorID, u.receiverID)
	}
	lastSentMap[execCtx.ReceiverID()] = execCtx.WriteVersion()
	return nil
}

func (u *UnionOperator) InSchema() *OperatorSchema {
	return u.schema
}

func (u *UnionOperator) OutSchema() *OperatorSchema {
	return u.schema
}

func (u *UnionOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(u.receiverID, u)
	return nil
}

func (u *UnionOperator) Teardown(mgr StreamManagerCtx, _ *sync.RWMutex) {
	mgr.UnregisterReceiver(u.receiverID)
}

func (u *UnionOperator) ForwardingProcessorCount() int {
	return u.forwardingProcCount
}

func (u *UnionOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return nil, u.sendBatchDownStream(batch, execCtx)
}

func (u *UnionOperator) ReceiveBarrier(execCtx StreamExecContext) error {
	return u.BaseOperator.HandleBarrier(execCtx)
}

func (u *UnionOperator) RequiresBarriersInjection() bool {
	return false
}

func (u *UnionOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}
