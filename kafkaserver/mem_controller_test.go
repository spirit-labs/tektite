package kafkaserver

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestReserve(t *testing.T) {
	maxSize := 1000000
	allocator := newDefaultMemController(maxSize)
	i := 0
	as := &allocateState{}
	totalAllocates := 0
	for totalAllocates < 2*maxSize {
		size := rand.Intn(10000)
		as.totSize += size
		ii := i
		allocator.Reserve(size, func() {
			// Make sure callback is not called unless totSize > maxSize
			require.Greater(t, as.totSize, maxSize)
			// Make sure callbacks are called in correct order
			require.Equal(t, as.expectedCallback, ii)
			as.expectedCallback++
			as.totSize -= size
		})
		i++
		totalAllocates += size
	}
}

type allocateState struct {
	expectedCallback int
	totSize          int
}
