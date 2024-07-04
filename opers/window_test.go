package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAugmentWithWindows(t *testing.T) {
	size := 100
	hop := 10
	aggExprStr := "count(val)"
	aggExprs, err := toExprs(aggExprStr)
	require.NoError(t, err)
	keyExprStr := "key"
	keyExprs, err := toExprs(keyExprStr)
	require.NoError(t, err)
	aggDesc := &parser.AggregateDesc{
		BaseDesc:             parser.BaseDesc{},
		AggregateExprs:       aggExprs,
		KeyExprs:             keyExprs,
		AggregateExprStrings: []string{aggExprStr},
		KeyExprsStrings:      []string{keyExprStr},
		Size:                 nil,
		Hop:                  nil,
		Lateness:             nil,
		Store:                nil,
		IncludeWindowCols:    nil,
		Retention:            nil,
	}
	agg, err := NewAggregateOperator(&OperatorSchema{EventSchema: KafkaSchema,
		PartitionScheme: NewPartitionScheme("foo", 10, false, 48)},
		aggDesc, 0,
		-1, -1, -1, time.Duration(size)*time.Millisecond,
		time.Duration(hop)*time.Millisecond, 0, false, false, &expr.ExpressionFactory{})
	require.NoError(t, err)

	eventTimes := []int{100, 101, 105, 107, 109}
	res := augmentBatch(t, agg, eventTimes...)
	ew := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	// All the events will be in the same set of windows
	checkRowsInWindows(t, res, [][]int{ew, ew, ew, ew, ew}, size, eventTimes...)

	eventTimes = []int{105, 107, 109, 110, 115, 119}
	res = augmentBatch(t, agg, eventTimes...)
	ew2 := []int{20, 30, 40, 50, 60, 70, 80, 90, 100, 110}
	checkRowsInWindows(t, res, [][]int{ew, ew, ew, ew2, ew2, ew2}, size, eventTimes...)

	eventTimes = []int{105, 115, 140, 143, 160, 165, 201}
	res = augmentBatch(t, agg, eventTimes...)
	ew0 := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	ew1 := []int{20, 30, 40, 50, 60, 70, 80, 90, 100, 110}
	ew2 = []int{50, 60, 70, 80, 90, 100, 110, 120, 130, 140}
	ew3 := []int{50, 60, 70, 80, 90, 100, 110, 120, 130, 140}
	ew4 := []int{70, 80, 90, 100, 110, 120, 130, 140, 150, 160}
	ew5 := []int{70, 80, 90, 100, 110, 120, 130, 140, 150, 160}
	ew6 := []int{110, 120, 130, 140, 150, 160, 170, 180, 190, 200}
	checkRowsInWindows(t, res, [][]int{ew0, ew1, ew2, ew3, ew4, ew5, ew6}, size, eventTimes...)
}

func augmentBatch(t *testing.T, agg *AggregateOperator, eventTimes ...int) *evbatch.Batch {
	batch := createBatchWithEventTimes(0, eventTimes...)
	batchRes, err := agg.augmentWithWindows(batch, &testExecCtx{partitionID: 1, processor: &testProcessor{id: 1, st: tppm.NewTestStore()}})
	require.NoError(t, err)
	return batchRes
}

func createBatchWithEventTimes(offsetStart int, times ...int) *evbatch.Batch {
	colBuilders := evbatch.CreateColBuilders(KafkaSchema.ColumnTypes())
	offset := int64(offsetStart)
	for _, et := range times {
		colBuilders[0].(*evbatch.IntColBuilder).Append(offset)
		colBuilders[1].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(int64(et)))
		colBuilders[2].(*evbatch.BytesColBuilder).Append([]byte(fmt.Sprintf("key-%06d", offset)))
		colBuilders[3].AppendNull()
		colBuilders[4].(*evbatch.BytesColBuilder).Append([]byte(fmt.Sprintf("val-%06d", offset)))
		offset++
	}
	return evbatch.NewBatchFromBuilders(KafkaSchema, colBuilders...)
}

func checkRowsInWindows(t *testing.T, evBatch *evbatch.Batch, eventWindowStarts [][]int, windowSize int,
	eventTimes ...int) {
	eventIndex := 0
	for i, et := range eventTimes {
		windowStarts := eventWindowStarts[i]
		for _, windowStart := range windowStarts {
			windowEnd := windowStart + windowSize
			require.Equal(t, windowStart, int(evBatch.GetTimestampColumn(0).Get(eventIndex).Val))
			require.Equal(t, windowEnd, int(evBatch.GetTimestampColumn(1).Get(eventIndex).Val))
			require.Equal(t, et, int(evBatch.GetTimestampColumn(2).Get(eventIndex).Val))
			eventIndex++
		}
	}
}
