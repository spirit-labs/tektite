package agent

import (
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestListOffsetsByTimestamp(t *testing.T) {
	topicName := "test-topic-1"
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           "footopic",
			PartitionCount: 23,
			MaxMessageSizeBytes: math.MaxInt,
		},
		{
			Name:           topicName,
			PartitionCount: 100,
			MaxMessageSizeBytes: math.MaxInt,
		},
	}
	cfg := NewConf()
	cfg.PusherConf.OffsetSnapshotInterval = 1 * time.Millisecond
	cfg.PusherConf.WriteTimeout = 1 * time.Millisecond
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	offsetTimestampMap := map[int64]int64{}
	numOffsets := 100
	maxOffsetsPerBatch := 10

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	offset := 0
	timestamp := time.Now().UnixMilli()
	for offset < numOffsets {
		numOffsetsPerBatch := 1 + rand.Intn(maxOffsetsPerBatch)
		batch := testutils.CreateKafkaRecordBatchWithTimestampAndIncrementingKVs(offset, numOffsetsPerBatch, timestamp)
		offsetTimestampMap[int64(offset)] = timestamp
		timestamp++
		offset += numOffsetsPerBatch
		req := kafkaprotocol.ProduceRequest{
			Acks:      -1,
			TimeoutMs: 1234,
			TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
				{
					Name: common.StrPtr(topicName),
					PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
						{
							Index:   int32(partitionID),
							Records: batch,
						},
					},
				},
			},
		}

		var resp kafkaprotocol.ProduceResponse
		r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
		require.NoError(t, err)
		produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
		require.True(t, ok)

		require.Equal(t, 1, len(produceResp.Responses))
		require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
		partResp := produceResp.Responses[0].PartitionResponses[0]
		require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	}

	info, ok, err := agent.topicMetaCache.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, ok)

	// Make sure they have been persisted
	for offset, timestamp := range offsetTimestampMap {
		// We execute with retry as the result won't appear until the data has been persisted
		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			storedOff, err := agent.tablePusher.GetOffsetForTimestamp(info.ID, partitionID, timestamp)
			if err != nil {
				return false, err
			}
			return offset == storedOff, nil
		}, 10*time.Second, 2*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)

		// now test using Kafka API
		resp := &kafkaprotocol.ListOffsetsResponse{}
		req := &kafkaprotocol.ListOffsetsRequest{
			ReplicaId:      0,
			IsolationLevel: 0,
			Topics: []kafkaprotocol.ListOffsetsRequestListOffsetsTopic{
				{
					Name: common.StrPtr(topicName),
					Partitions: []kafkaprotocol.ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: int32(partitionID),
							Timestamp:      timestamp,
						},
					},
				},
			},
		}
		r, err := conn.SendRequest(req, kafkaprotocol.APIKeyListOffsets, 1, resp)
		require.NoError(t, err)
		resp = r.(*kafkaprotocol.ListOffsetsResponse)

		require.Equal(t, 1, len(resp.Topics))
		topicResp := resp.Topics[0]
		require.Equal(t, 1, len(topicResp.Partitions))
		partResp := topicResp.Partitions[0]
		require.Equal(t, offset, partResp.Offset)
	}
}
