package lsm

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestCompactTopicSingleTableSingleBatch(t *testing.T) {
	testCompactTopic(t, 1, 1)
}

func TestCompactTopicSingleTableMultipleBatches(t *testing.T) {
	testCompactTopic(t, 1, 5)
}

func TestCompactTopicMultipleTablesMultipleBatches(t *testing.T) {
	testCompactTopic(t, 5, 5)
}

func testCompactTopic(t *testing.T, numTables int, numBatchesPerTable int) {
	numKeys := 100
	numEntriesPerKey := 100
	latestOffsets := map[string]int64{}
	topicID := int64(1234)
	partitionID := int64(23)
	partitionHash, err := parthash.CreatePartitionHash(int(topicID), int(partitionID))
	require.NoError(t, err)
	offsetStart := int64(100)
	offset := offsetStart

	var kvs []common.KV
	for j := 0; j < numBatchesPerTable; j++ {
		batch := createBatchWithKeys(numKeys, numEntriesPerKey, offset, latestOffsets)
		offset += int64(numEntriesPerKey * numKeys)
		batch = common.AppendValueMetadata(batch, topicID, partitionID)
		key := make([]byte, 0, 33)
		key = append(key, partitionHash...)
		key = append(key, common.EntryTypeTopicData)
		key = encoding.KeyEncodeInt(key, offset)
		key = encoding.EncodeVersion(key, 0)
		kv := common.KV{
			Key:   key,
			Value: batch,
		}
		kvs = append(kvs, kv)
	}
	si := iteration.NewStaticIterator(kvs)

	iter := NewCompactedTopicIterator(si, func(topicID int64) (bool, error) {
		return true, nil
	}, func(partHash []byte, key []byte) (int64, bool, error) {
		key2 := make([]byte, len(partHash)+len(key))
		copy(key2, partHash)
		copy(key2[len(partHash):], key)
		lastOffset, ok := latestOffsets[string(key2)]
		return lastOffset, ok, nil
	})

	var res []common.KV
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		res = append(res, kv)
	}

	// Number of batches must be preserved
	require.Equal(t, numTables*numBatchesPerTable, len(res))

	var receivedMsgs []kafkaencoding.RawKafkaMessage
	for _, kv := range res {
		_, receivedBatch := common.ReadAndRemoveValueMetadata(kv.Value)
		msgs := kafkaencoding.BatchToRawMessages(receivedBatch)
		receivedMsgs = append(receivedMsgs, msgs...)
	}
	offsetsForKey := map[string]int64{}
	for _, msg := range receivedMsgs {
		_, ok := offsetsForKey[string(msg.Key)]
		require.False(t, ok)
		offsetsForKey[string(msg.Key)] = msg.Offset
	}
	require.Equal(t, latestOffsets, offsetsForKey)
}

func createBatchWithKeys(numKeys int, numEntriesPerKey int, startOffset int64, latestOffsets map[string]int64) []byte {
	var msgs []kafkaencoding.RawKafkaMessage
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%05d", i)
		for j := 0; j < numEntriesPerKey; j++ {
			value := fmt.Sprintf("value-%05d-%05d", i, j)
			msgs = append(msgs, kafkaencoding.RawKafkaMessage{
				Key:       []byte(key),
				Value:     []byte(value),
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	for i, msg := range msgs {
		offset := startOffset + int64(i)
		latestOffsets[string(msg.Key)] = offset
	}
	batch := testutils.CreateKafkaRecordBatch(msgs, startOffset)
	return batch
}
