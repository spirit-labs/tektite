package agent

import (
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestCompactedTopic(t *testing.T) {
	topicName := "test-topic-1"
	topicID := 1000
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:                topicName,
			PartitionCount:      1,
			MaxMessageSizeBytes: math.MaxInt,
			Compacted:           true,
		},
	}
	cfg := NewConf()
	cfg.PusherConf.WriteTimeout = 1 * time.Millisecond
	cfg.PusherConf.CompactedTopicLastOffsetSnapshotInterval = 1 * time.Millisecond
	agent, objStore, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	latestOffsetsMap := map[string]int64{}
	batch := createBatchWithKeys(100, 10, 0, latestOffsetsMap)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   0,
						Records: batch,
					},
				},
			},
		},
	}

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	require.NoError(t, err)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp := produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	// Make sure compacted topic last offsets have been written
	testutils.WaitUntil(t, func() (bool, error) {
		return !agent.TablePusher().HasPendingCompactedTopicOffsetsToWrite(), nil
	})

	controllerCl, err := agent.controller.Client()
	require.NoError(t, err)
	defer func() {
		err := controllerCl.Close()
		require.NoError(t, err)
	}()

	numCompactions := agent.controller.LsmManager().GetCompactionStats().CompletedJobs
	require.Equal(t, 0, numCompactions)
	err = agent.controller.ForceCompaction(0)
	require.NoError(t, err)
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		return agent.controller.LsmManager().GetCompactionStats().CompletedJobs == 1, nil
	}, 5*time.Second, 1*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	prefix, err := partHashes.GetPartitionHash(topicID, 0)
	require.NoError(t, err)
	keyStart := append(common.ByteSliceCopy(prefix), common.EntryTypeTopicData)
	keyStart = encoding.KeyEncodeInt(keyStart, 0)
	keyStart = append(keyStart, common.EntryTypeTopicData)
	keyEnd := common.IncBigEndianBytes(prefix)

	tg := &tableGetter{
		bucketName: cfg.PusherConf.DataBucketName,
		objStore:   objStore,
	}
	iter, err := queryutils.CreateIteratorForKeyRange(keyStart, keyEnd, controllerCl, tg.GetSSTable)
	require.NoError(t, err)
	var batches [][]byte
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		if kv.Key[16] == common.EntryTypeTopicData {
			batches = append(batches, kv.Value)
		}
	}
	// Make sure only latest values remain
	var receivedMsgs []kafkaencoding.RawKafkaMessage
	for _, batch := range batches {
		_, receivedBatch := common.ReadAndRemoveValueMetadata(batch)
		msgs := kafkaencoding.BatchToRawMessages(receivedBatch)
		receivedMsgs = append(receivedMsgs, msgs...)
	}
	offsetsForKey := map[string]int64{}
	for _, msg := range receivedMsgs {
		_, ok := offsetsForKey[string(msg.Key)]
		require.False(t, ok)
		offsetsForKey[string(msg.Key)] = msg.Offset
	}
	require.Equal(t, latestOffsetsMap, offsetsForKey)
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
