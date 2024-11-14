package pusher

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserializeDirectWriteRequest(t *testing.T) {
	req := DirectWriteRequest{
		WriterKey:   "g.mygoup123123",
		WriterEpoch: 123,
		KVs: []common.KV{
			{
				Key:   []byte("key-1234"),
				Value: []byte("value-1234"),
			},
			{
				Key:   []byte("key-3434"),
				Value: []byte("value-3434"),
			},
			{
				Key:   []byte("key-56767"),
				Value: []byte("value-56767"),
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 DirectWriteRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeDirectProduceRequest(t *testing.T) {
	req := DirectProduceRequest{
		TopicProduceRequests: []TopicProduceRequest{
			{
				TopicID: 1234,
				PartitionProduceRequests: []PartitionProduceRequest{
					{
						PartitionID: 7456565,
						Batch:       []byte("batch1"),
					},
					{
						PartitionID: 23444,
						Batch:       []byte("batch2"),
					},
					{
						PartitionID: 4566,
						Batch:       []byte("batch3"),
					},
				},
			},
			{
				TopicID: 45545,
				PartitionProduceRequests: []PartitionProduceRequest{
					{
						PartitionID: 34342,
						Batch:       []byte("batch4"),
					},
				},
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 DirectProduceRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}
