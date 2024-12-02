package kafkagen

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenProto(t *testing.T) {
	specSet := SpecSet{
		SpecDir:  "../asl/kafka/spec",
		Included: StandardIncluded,
	}
	err := Generate([]SpecSet{specSet}, "../kafkaprotocol")
	require.NoError(t, err)
}
