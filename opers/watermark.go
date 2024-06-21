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
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"sync"
	"time"
)

type WatermarkType int

const WaterMarkTypeEventTime = WatermarkType(1)
const WaterMarkTypeProcessingTime = WatermarkType(2)

// WaterMarkOperator is currently not deployable directly, but used in kafka_in and bridge_from
type WaterMarkOperator struct {
	BaseOperator
	processorMaxEventTimes    []int
	processorLastBatchHandled []uint64
	waterMarkType             WatermarkType
	schema                    *OperatorSchema
	latenessMillis            int
	idleTimeoutNanos          uint64
	testIdleProcessors        bool
	idleProcessors            []bool
}

func NewWaterMarkOperator(inSchema *OperatorSchema, waterMarkTypeStr string, lateness time.Duration, idleTimeout time.Duration) *WaterMarkOperator {
	// Find the max processor id
	maxProcessorID := inSchema.PartitionScheme.MaxProcessorID
	// We maintain max event time for each processor
	processorMaxEventTimes := make([]int, maxProcessorID+1)

	var waterMarkType WatermarkType
	switch waterMarkTypeStr {
	case "event_time":
		waterMarkType = WaterMarkTypeEventTime
	case "processing_time":
		waterMarkType = WaterMarkTypeProcessingTime
	default:
		panic("unexpected watermark type")
	}

	var processorLastBatchHandled []uint64
	if waterMarkType == WaterMarkTypeEventTime {
		processorLastBatchHandled = make([]uint64, maxProcessorID+1)
	}

	idleTimeoutNanos := uint64(idleTimeout.Nanoseconds())
	return &WaterMarkOperator{
		schema:                    inSchema,
		waterMarkType:             waterMarkType,
		processorMaxEventTimes:    processorMaxEventTimes,
		processorLastBatchHandled: processorLastBatchHandled,
		latenessMillis:            int(lateness.Milliseconds()),
		idleTimeoutNanos:          idleTimeoutNanos,
	}
}

func (w *WaterMarkOperator) GetPartitionProcessorMapping() map[int]int {
	return w.schema.PartitionScheme.PartitionProcessorMapping
}

func (w *WaterMarkOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if w.waterMarkType == WaterMarkTypeEventTime {
		// Maintain max event time
		col := batch.GetTimestampColumn(1)
		procID := execCtx.Processor().ID()
		max := w.processorMaxEventTimes[procID]
		for i := 0; i < batch.RowCount; i++ {
			eventTs := col.Get(i)
			eventTime := int(eventTs.Val)
			if eventTime > max {
				max = eventTime
			}
		}
		w.updateMaxEventTime(max, procID)
	}
	return batch, w.sendBatchDownStream(batch, execCtx)
}

func (w *WaterMarkOperator) updateMaxEventTime(maxEventTime int, procID int) {
	w.processorMaxEventTimes[procID] = maxEventTime
	w.processorLastBatchHandled[procID] = common.NanoTime()
}

func (w *WaterMarkOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (w *WaterMarkOperator) HandleBarrier(execCtx StreamExecContext) error {
	w.setWatermark(execCtx)
	return w.BaseOperator.HandleBarrier(execCtx)
}

func (w *WaterMarkOperator) setWatermark(execCtx StreamExecContext) {
	// Get the latest watermark and set it on the barrier.
	var waterMark int
	switch w.waterMarkType {
	case WaterMarkTypeProcessingTime:
		nowMillis := time.Now().UTC().UnixMilli()
		waterMark = int(nowMillis) - w.latenessMillis
	case WaterMarkTypeEventTime:
		procID := execCtx.Processor().ID()
		maxEventTime := w.processorMaxEventTimes[procID]
		lastHandledTime := w.processorLastBatchHandled[procID]
		now := common.NanoTime()
		if w.testIdleProcessors && w.idleProcessors[procID] {
			// used in testing to force idle watermark
			waterMark = -1
		} else if lastHandledTime == 0 || now-lastHandledTime >= w.idleTimeoutNanos {
			// idle - no data on this processor - we set watermark to -1 -this will be ignored when waiting
			// for barriers if there are other non -1 watermarks, otherwise it will be let through. Window
			// operator can close windows on idle timeout if it receives -1 watermark.
			waterMark = -1
		} else if maxEventTime > 0 {
			waterMark = maxEventTime - w.latenessMillis
		}
	}
	execCtx.SetWaterMark(waterMark)
}

func (w *WaterMarkOperator) InSchema() *OperatorSchema {
	return w.schema
}

func (w *WaterMarkOperator) OutSchema() *OperatorSchema {
	return w.schema
}

func (w *WaterMarkOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (w *WaterMarkOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}

// Receiver implementation is only used for injecting batches in testing

func (w *WaterMarkOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return w.HandleStreamBatch(batch, execCtx)
}

func (w *WaterMarkOperator) ReceiveBarrier(execCtx StreamExecContext) error {
	return w.HandleBarrier(execCtx)
}

func (w *WaterMarkOperator) RequiresBarriersInjection() bool {
	return false
}

func (w *WaterMarkOperator) ForwardingProcessorCount() int {
	return len(w.schema.PartitionScheme.ProcessorIDs)
}

// SetIdleForProcessor - Used in testing only to force idleness for a processor
func (w *WaterMarkOperator) SetIdleForProcessor(processorID int) {
	w.testIdleProcessors = true
	if w.idleProcessors == nil {
		w.idleProcessors = make([]bool, len(w.processorMaxEventTimes))
	}
	w.idleProcessors[processorID] = true
}
