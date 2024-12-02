package agent

import (
	"errors"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestListLatestOffset(t *testing.T) {
	off := int64(123213)
	testListOffsets(t, off, off, -1, nil, kafkaprotocol.ErrorCodeNone)
}

func TestListEarliestOffset1(t *testing.T) {
	off := int64(123213)
	testListOffsets(t, 0, off, -2, nil, kafkaprotocol.ErrorCodeNone)
}

func TestListEarliestOffset2(t *testing.T) {
	off := int64(123213)
	testListOffsets(t, 0, off, -4, nil, kafkaprotocol.ErrorCodeNone)
}

func TestListOffsetInjectUnavailable(t *testing.T) {
	off := int64(123213)
	testListOffsets(t, 0, off, -4, common.NewTektiteErrorf(common.Unavailable, "injected"), kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
}

func TestListOffsetInjectUnexpectedError(t *testing.T) {
	off := int64(123213)
	testListOffsets(t, 0, off, -4, errors.New("foobar"), kafkaprotocol.ErrorCodeUnknownServerError)
}

func testListOffsets(t *testing.T, expectedOffset int64, offset int64, timestamp int64, expectedError error, expectedErrCode int) {
	t.Parallel()
	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	cl, err := apiclient.NewKafkaApiClientWithClientID("")
	require.NoError(t, err)
	conn, err := cl.NewConnection(agents[0].Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	partitionID := 3

	agents[0].controller.OffsetsCache().SetLastReadableOffset(topicmeta.TopicIDSequenceBase+3, partitionID, offset)
	if expectedError != nil {
		agents[0].controlClientCache.SetInjectedError(expectedError)
	}

	resp := &kafkaprotocol.ListOffsetsResponse{}
	req := &kafkaprotocol.ListOffsetsRequest{
		ReplicaId:      0,
		IsolationLevel: 0,
		Topics: []kafkaprotocol.ListOffsetsRequestListOffsetsTopic{
			{
				Name: common.StrPtr("topic-00003"),
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
	require.Equal(t, "topic-00003", common.SafeDerefStringPtr(resp.Topics[0].Name))
	require.Equal(t, 1, len(resp.Topics[0].Partitions))
	require.Equal(t, expectedErrCode, int(resp.Topics[0].Partitions[0].ErrorCode))
	require.Equal(t, partitionID, int(resp.Topics[0].Partitions[0].PartitionIndex))
	if expectedErrCode == kafkaprotocol.ErrorCodeNone {
		require.Equal(t, expectedOffset, resp.Topics[0].Partitions[0].Offset)
	}
}
