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
	"fmt"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestWaterMarkOperatorEventTime(t *testing.T) {
	operSchema := &OperatorSchema{
		EventSchema:     KafkaSchema,
		PartitionScheme: NewPartitionScheme("test_stream", 10, false, 48),
	}
	lag := 1 * time.Second
	idleTimeout := 3 * time.Second
	wo := NewWaterMarkOperator(operSchema, "event_time", lag, idleTimeout)
	numBatches := 100
	rowsPerBatch := 10
	var offset int64
	baseTime := time.Now().UnixMilli()

	// For each processor, send a bunch of batches and keep track of the max event time for each processor
	maxTimes := map[int]int64{}

	for _, procID := range operSchema.PartitionScheme.ProcessorIDs {
		partitionID := operSchema.PartitionScheme.ProcessorPartitionMapping[procID][0]
		ec := &testExecCtx{
			partitionID: partitionID,
			processor:   &testProcessor{id: procID},
		}

		var maxTime int64

		for i := 0; i < numBatches; i++ {
			colBuilders := evbatch.CreateColBuilders(KafkaSchema.ColumnTypes())
			for j := 0; j < rowsPerBatch; j++ {
				colBuilders[0].(*evbatch.IntColBuilder).Append(offset)
				eventTime := baseTime + int64(rand.Intn(1000)-500)
				colBuilders[1].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(eventTime))
				if eventTime > maxTime {
					maxTime = eventTime
				}
				colBuilders[2].(*evbatch.BytesColBuilder).Append([]byte(fmt.Sprintf("key-%06d", offset)))
				colBuilders[3].AppendNull()
				colBuilders[4].(*evbatch.BytesColBuilder).Append([]byte(fmt.Sprintf("val-%06d", offset)))
				offset++
			}
			batch := evbatch.NewBatchFromBuilders(KafkaSchema, colBuilders...)
			_, err := wo.HandleStreamBatch(batch, ec)
			require.NoError(t, err)
		}

		maxTimes[procID] = maxTime
	}

	// Now send a barrier for each processor - should pick up watermark for that processor

	for _, procID := range operSchema.PartitionScheme.ProcessorIDs {
		partitionID := operSchema.PartitionScheme.ProcessorPartitionMapping[procID][0]
		ec := &testExecCtx{
			partitionID: partitionID,
			processor:   &testProcessor{id: procID},
		}
		err := wo.HandleBarrier(ec)
		require.NoError(t, err)
		expectedMaxTime := maxTimes[procID]
		expectedWM := int(expectedMaxTime - lag.Milliseconds())
		require.Equal(t, expectedWM, ec.WaterMark())
	}
}

func TestWaterMarkOperatorProcessingTime(t *testing.T) {
	operSchema := &OperatorSchema{
		EventSchema:     KafkaSchema,
		PartitionScheme: NewPartitionScheme("test_stream", 10, false, 48),
	}
	lag := 100 * time.Millisecond
	idleTimeout := 1 * time.Second
	wo := NewWaterMarkOperator(operSchema, "processing_time", lag, idleTimeout)

	// send barriers
	for _, procID := range operSchema.PartitionScheme.ProcessorIDs {
		partitionID := operSchema.PartitionScheme.ProcessorPartitionMapping[procID][0]
		for i := 0; i < 4; i++ {
			ec := &testExecCtx{
				partitionID: partitionID,
				processor:   &testProcessor{id: procID},
			}
			expectedWM := time.Now().Add(-lag).UnixMilli()
			err := wo.HandleBarrier(ec)
			require.NoError(t, err)
			require.GreaterOrEqual(t, ec.WaterMark(), int(expectedWM))
			require.Less(t, ec.WaterMark(), int(expectedWM+10))
			time.Sleep(15 * time.Millisecond)
		}
	}

}
