//go:build !release

package testutils

import (
	"fmt"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/types"
	"hash/crc32"
	"math"
	"time"
)

func CreateKafkaRecordBatch(messages []RawKafkaMessage, offsetStart int64) []byte {
	batchBytes := make([]byte, 61)
	first := true
	var firstTimestamp types.Timestamp
	var timestamp types.Timestamp
	offset := offsetStart
	for _, msg := range messages {
		var ok bool
		timestamp = types.Timestamp{Val: msg.Timestamp}
		if first {
			firstTimestamp = timestamp
		}
		batchBytes, ok = kafkaencoding.AppendToBatch(batchBytes, offset, msg.Key, nil, msg.Value, timestamp,
			firstTimestamp, offsetStart, math.MaxInt, first)
		if !ok {
			panic("failed to append")
		}
		first = false
	}
	kafkaencoding.SetBatchHeader(batchBytes, offsetStart, offset, firstTimestamp, timestamp, len(messages), crc32.NewIEEE())
	return batchBytes
}

func CreateKafkaRecordBatchWithIncrementingKVs(offsetStart int, numMessages int) []byte {
	var msgs []RawKafkaMessage
	for i := offsetStart; i < offsetStart+numMessages; i++ {
		msgs = append(msgs, RawKafkaMessage{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte(fmt.Sprintf("key%d", i)),
			Value:     []byte(fmt.Sprintf("val%d", i)),
		})
	}
	return CreateKafkaRecordBatch(msgs, int64(offsetStart))
}

type RawKafkaMessage struct {
	Key       []byte
	Value     []byte
	Headers   []byte
	Timestamp int64
}
