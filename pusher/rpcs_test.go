package pusher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

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
