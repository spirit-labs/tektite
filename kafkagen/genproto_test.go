package kafkagen

import (
	"github.com/spirit-labs/tektite/tekusers/tekusers"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenProto(t *testing.T) {
	standardSpecSet := SpecSet{
		SpecDir:  "../asl/kafka/spec",
		Included: StandardIncluded,
	}
	customSpecSet := SpecSet{
		SpecDir:  "../tekusers/tekusers/apispec",
		Included: tekusers.Included,
	}
	err := Generate([]SpecSet{standardSpecSet, customSpecSet}, "../kafkaprotocol")
	require.NoError(t, err)
}
