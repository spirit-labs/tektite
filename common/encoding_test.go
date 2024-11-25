package common

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestAppendValueMetadata(t *testing.T) {
	buff := []byte("some bytes")
	vals := []int64{2343, -34, 0, 345345, 34, 797, -344, 0, 23, math.MaxInt64, math.MaxInt64 - 100, math.MinInt64, math.MinInt64 + 100}
	buff = AppendValueMetadata(buff, vals...)
	vals2 := ReadValueMetadata(buff)
	require.Equal(t, vals, vals2)
}

func TestAppendValueMetadataZeroVals(t *testing.T) {
	buff := []byte("some bytes")
	buff = AppendValueMetadata(buff)
	vals := ReadValueMetadata(buff)
	require.Nil(t, vals)

	buff = AppendValueMetadata(buff, []int64{}...)
	vals = ReadValueMetadata(buff)
	require.Nil(t, vals)
}
