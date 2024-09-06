package sequence

import (
	"fmt"
	"github.com/spirit-labs/tektite/lock"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

const sequencesBatchSize = 10
const unavailabilityRetryDelay = 1 * time.Millisecond

const bucketName = "test-bucket"

func TestSingleSequence(t *testing.T) {
	lockMgr := lock.NewInMemLockManager()
	objStore := dev.NewInMemStore(0)
	mgr := NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)
	for i := 0; i < 10*sequencesBatchSize; i++ {
		seq, err := mgr.GetNextID("test_sequence", sequencesBatchSize)
		require.NoError(t, err)
		require.Equal(t, i, seq)
	}

	// Recreate so state gets reloaded
	mgr = NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	for i := 0; i < 10*sequencesBatchSize; i++ {
		seq, err := mgr.GetNextID("test_sequence", sequencesBatchSize)
		require.NoError(t, err)
		require.Equal(t, 10*sequencesBatchSize+i, seq)
	}

}

func TestMultipleSequences(t *testing.T) {
	lockMgr := lock.NewInMemLockManager()
	objStore := dev.NewInMemStore(0)
	mgr := NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	for i := 0; i < 10; i++ {
		sequenceName := fmt.Sprintf("sequence-%d", i)
		for j := 0; j < 10*sequencesBatchSize; j++ {
			seq, err := mgr.GetNextID(sequenceName, sequencesBatchSize)
			require.NoError(t, err)
			require.Equal(t, j, seq)
		}
	}

	mgr = NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	// Reload state
	for i := 0; i < 10; i++ {
		sequenceName := fmt.Sprintf("sequence-%d", i)
		for j := 0; j < 10*sequencesBatchSize; j++ {
			seq, err := mgr.GetNextID(sequenceName, sequencesBatchSize)
			require.NoError(t, err)
			require.Equal(t, 10*sequencesBatchSize+j, seq)
		}
	}

}

func TestSequenceBatchSize(t *testing.T) {
	lockMgr := lock.NewInMemLockManager()
	objStore := dev.NewInMemStore(0)
	mgr := NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	seq, err := mgr.GetNextID("test_sequence", sequencesBatchSize)
	require.NoError(t, err)
	require.Equal(t, 0, seq)

	// Recreate so state gets reloaded
	mgr = NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	seq, err = mgr.GetNextID("test_sequence", sequencesBatchSize)
	require.NoError(t, err)
	require.Equal(t, sequencesBatchSize, seq)
}

func TestConcurrentGets(t *testing.T) {
	lockMgr := lock.NewInMemLockManager()
	objStore := dev.NewInMemStore(0)
	var seqs1 sync.Map
	// Note unavailabilityRetryDelay is set to a low value so the different managers gets coincide more
	mgr1 := NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)
	var seqs2 sync.Map
	mgr2 := NewSequenceManager(objStore, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 1000; i++ {
			seq, err := mgr1.GetNextID("test_sequence", sequencesBatchSize)
			if err != nil {
				panic(err)
			}
			seqs1.Store(seq, struct{}{})
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			seq, err := mgr2.GetNextID("test_sequence", sequencesBatchSize)
			if err != nil {
				panic(err)
			}
			seqs2.Store(seq, struct{}{})
		}
		wg.Done()
	}()

	wg.Wait()
	// Should be no overlap
	overlap := false
	seqs1.Range(func(key, value any) bool {
		_, ok := seqs2.Load(key)
		if ok {
			overlap = true
			return false
		}
		return true
	})
	require.False(t, overlap)

	overlap = false
	seqs2.Range(func(key, value any) bool {
		_, ok := seqs1.Load(key)
		if ok {
			overlap = true
			return false
		}
		return true
	})
	require.False(t, overlap)
}

func TestCloudStoreUnavailable(t *testing.T) {
	lockMgr := lock.NewInMemLockManager()
	store := dev.NewInMemStore(0)
	mgr := NewSequenceManager(store, "sequences_obj", lockMgr, unavailabilityRetryDelay, bucketName)
	for i := 0; i < 10*sequencesBatchSize; i++ {
		seq, err := mgr.GetNextID("test_sequence", sequencesBatchSize)
		require.NoError(t, err)
		require.Equal(t, i, seq)
	}

	store.SetUnavailable(true)
	time.AfterFunc(1*time.Second, func() {
		store.SetUnavailable(false)
	})

	for i := 0; i < 10*sequencesBatchSize; i++ {
		seq, err := mgr.GetNextID("test_sequence", sequencesBatchSize)
		require.NoError(t, err)
		require.Equal(t, 10*sequencesBatchSize+i, seq)
	}

}
