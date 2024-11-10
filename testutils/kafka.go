//go:build !release

package testutils

import (
	"encoding/binary"
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
	for i, msg := range messages {
		var ok bool
		timestamp = types.Timestamp{Val: msg.Timestamp}
		if first {
			firstTimestamp = timestamp
		}
		batchBytes, ok = kafkaencoding.AppendToBatch(batchBytes, int64(i), msg.Key, nil, msg.Value, timestamp,
			firstTimestamp, math.MaxInt, first)
		if !ok {
			panic("failed to append")
		}
		first = false
	}
	kafkaencoding.SetBatchHeader(batchBytes, offsetStart, offset, firstTimestamp, timestamp, len(messages), crc32.NewIEEE())
	// Set producer id to -1 (no idempotent producer)
	minusOne := int64(-1)
	binary.BigEndian.PutUint64(batchBytes[43:], uint64(minusOne))
	return batchBytes
}

func CreateKafkaRecordBatchWithIncrementingKVs(offsetStart int, numMessages int) []byte {
	var msgs []RawKafkaMessage
	for i := offsetStart; i < offsetStart+numMessages; i++ {
		msgs = append(msgs, RawKafkaMessage{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte(fmt.Sprintf("key%09d", i)),
			Value:     []byte(fmt.Sprintf("val%09d", i)),
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
