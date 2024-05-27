package kafkaserver

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestAllocator(t *testing.T) {
	maxSize := 1000000
	allocator := newDefaultAllocator(maxSize)
	i := 0
	as := &allocateState{}
	totalAllocates := 0
	for totalAllocates < 2*maxSize {
		size := rand.Intn(10000)
		as.totSize += size
		ii := i
		buff, err := allocator.Allocate(size, func() {
			// Make sure callback is not called unless totSize > maxSize
			require.Greater(t, as.totSize, maxSize)
			// Make sure callbacks are called in correct order
			require.Equal(t, as.expectedCallback, ii)
			as.expectedCallback++
			as.totSize -= size
		})
		require.NoError(t, err)
		require.Equal(t, size, len(buff))
		i++
		totalAllocates += size
	}
}

type allocateState struct {
	expectedCallback int
	totSize          int
}
