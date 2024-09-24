package kafkagen

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenProto(t *testing.T) {
	err := Generate("../asl/kafka/spec", "../kafkaprotocol")
	require.NoError(t, err)
}
