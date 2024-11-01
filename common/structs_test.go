package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserializeMembershipData(t *testing.T) {
	data := MembershipData{
		ClusterListenAddress: "some-address:1234",
		KafkaListenerAddress: "other-address:5678",
		AZInfo:               "az-12345",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = data.Serialize(buff)
	var data2 MembershipData
	off := data2.Deserialize(buff, 3)
	require.Equal(t, data, data2)
	require.Equal(t, off, len(buff))
}
