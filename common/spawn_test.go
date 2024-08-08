package common

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestGo(t *testing.T) {
	numRoutines := 10
	var lock sync.Mutex
	blockCount := 0
	var wg1 sync.WaitGroup
	wg1.Add(1)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	for i := 0; i < numRoutines; i++ {
		Go(func() {
			lock.Lock()
			blockCount++
			if blockCount == numRoutines {
				wg1.Done()
			}
			lock.Unlock()
			wg2.Wait()
		})
	}
	wg1.Wait()
	require.Equal(t, numRoutines, int(RunningGRCount()))
	wg2.Done()
}
