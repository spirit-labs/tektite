package control

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

const (
	stateUpdatorBucketName = "state-updator-bucket"
	stateUpdatorKeyprefix  = "state-updator-keyprefix"
	dataBucketName         = "data-bucket"
	dataKeyprefix          = "data-keyprefix"
)

func TestApplyChanges(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	holder := NewLsmHolder(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.Conf{})
	err := holder.Start()
	require.NoError(t, err)

	testApplyChanges(t, holder, []byte(uuid.New().String()))
}

func TestApplyChangesRestart(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	holder := NewLsmHolder(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.Conf{})
	err := holder.Start()
	require.NoError(t, err)

	tableID := []byte(uuid.New().String())
	testApplyChanges(t, holder, tableID)

	err = holder.Stop()
	require.NoError(t, err)

	// recreate holder
	holder = NewLsmHolder(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.Conf{})
	err = holder.Start()
	require.NoError(t, err)

	// Registration should still be there - as metadata was committed to object store after previous ApplyChanges
	res, err := holder.QueryTablesInRange(nil, nil)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID := res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))
}

func testApplyChanges(t *testing.T, holder *LsmHolder, tableID []byte) {
	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")
	batch := createBatch(1, tableID, keyStart, keyEnd)
	ch := make(chan error, 1)
	err := holder.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	})
	require.NoError(t, err)
	err = <-ch
	require.NoError(t, err)

	res, err := holder.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID := res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))
}

// TestApplyChangesL0Full tests the queueing behaviour of the holder when L0 is full
func TestApplyChangesL0Full(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	opts := lsm.Conf{
		L0MaxTablesBeforeBlocking: 10,
	}
	holder := NewLsmHolder(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		opts)
	err := holder.Start()
	require.NoError(t, err)

	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")

	var tabIDs []sst.SSTableID

	// We should be able to apply L0MaxTablesBeforeBlocking tables
	for i := 0; i < opts.L0MaxTablesBeforeBlocking; i++ {
		tableID := []byte(uuid.New().String())
		tabIDs = append(tabIDs, tableID)
		batch := createBatch(0, tableID, keyStart, keyEnd)
		ch := make(chan error, 1)
		err := holder.ApplyLsmChanges(batch, func(err error) error {
			ch <- err
			return nil
		})
		require.NoError(t, err)
		err = <-ch
		require.NoError(t, err)
	}

	// The next ones should queue (i.e. completion not called) until we free some space
	numToQueue := 5
	completionsCalled := make([]*atomic.Bool, numToQueue)
	var chans []chan error
	for i := 0; i < numToQueue; i++ {
		cc := new(atomic.Bool)
		completionsCalled[i] = cc
		tableID := []byte(uuid.New().String())
		batch := createBatch(0, tableID, keyStart, keyEnd)
		ch := make(chan error, 1)
		chans = append(chans, ch)
		err = holder.ApplyLsmChanges(batch, func(err error) error {
			cc.Store(true)
			ch <- err
			return nil
		})
		require.NoError(t, err)
	}

	// They should be queued
	time.Sleep(10 * time.Millisecond)
	for _, cc := range completionsCalled {
		require.False(t, cc.Load())
	}

	// Now free some space in L0 (two tables)
	deregBatch := lsm.RegistrationBatch{DeRegistrations: []lsm.RegistrationEntry{
		{
			Level:    0,
			TableID:  tabIDs[0],
			KeyStart: keyStart,
			KeyEnd:   keyEnd,
		},
		{
			Level:    0,
			TableID:  tabIDs[1],
			KeyStart: keyStart,
			KeyEnd:   keyEnd,
		},
	}}
	ch := make(chan error, 1)
	err = holder.ApplyLsmChanges(deregBatch, func(err error) error {
		ch <- err
		return nil
	})
	require.NoError(t, err)
	err = <-ch
	require.NoError(t, err)

	// The first two queued completions should be called
	testutils.WaitUntil(t, func() (bool, error) {
		// Wait for 0, 1 to be called
		return completionsCalled[0].Load() && completionsCalled[1].Load(), nil
	})
	// The others not
	require.Equal(t, false, completionsCalled[2].Load())
	require.Equal(t, false, completionsCalled[3].Load())
	require.Equal(t, false, completionsCalled[4].Load())

	// Now free the rest
	deregBatch = lsm.RegistrationBatch{DeRegistrations: []lsm.RegistrationEntry{
		{
			Level:    0,
			TableID:  tabIDs[2],
			KeyStart: keyStart,
			KeyEnd:   keyEnd,
		},
		{
			Level:    0,
			TableID:  tabIDs[3],
			KeyStart: keyStart,
			KeyEnd:   keyEnd,
		},
		{
			Level:    0,
			TableID:  tabIDs[4],
			KeyStart: keyStart,
			KeyEnd:   keyEnd,
		},
	}}
	ch = make(chan error, 1)
	err = holder.ApplyLsmChanges(deregBatch, func(err error) error {
		ch <- err
		return nil
	})
	require.NoError(t, err)
	err = <-ch
	require.NoError(t, err)

	// All should now be called (async)
	testutils.WaitUntil(t, func() (bool, error) {
		// Wait for 2, 3, 4 to be called
		return completionsCalled[2].Load() && completionsCalled[3].Load() && completionsCalled[4].Load(), nil
	})

	for _, ch := range chans {
		err := <-ch
		require.NoError(t, err)
	}
}

func createBatch(level int, tableID []byte, keyStart []byte, keyEnd []byte) lsm.RegistrationBatch {
	regEntry := lsm.RegistrationEntry{
		Level:      level,
		TableID:    tableID,
		MinVersion: 123,
		MaxVersion: 1235,
		KeyStart:   keyStart,
		KeyEnd:     keyEnd,
		AddedTime:  uint64(time.Now().UnixMilli()),
		NumEntries: 1234,
		TableSize:  12345567,
	}
	return lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
}
