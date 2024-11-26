package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserializeMembershipData(t *testing.T) {
	data := MembershipData{
		ClusterListenAddress: "some-address:1234",
		KafkaListenerAddress: "other-address:5678",
		Location:             "az-12345",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = data.Serialize(buff)
	var data2 MembershipData
	off := data2.Deserialize(buff, 3)
	require.Equal(t, data, data2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeDirectWriteRequest(t *testing.T) {
	req := DirectWriteRequest{
		WriterKey:   "g.mygoup123123",
		WriterEpoch: 123,
		KVs: []KV{
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
