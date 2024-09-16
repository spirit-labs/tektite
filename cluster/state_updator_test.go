package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStateUpdator(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		objStores[i] = objStore
	}
	runTime := 3 * time.Second
	applyLoadAndVerifyStateUpdator(t, runTime, numMembers, 10*time.Millisecond, StateUpdatorOpts{}, objStores)
}

func TestStateUpdatorAutoUpdate(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	updateInterval := 100 * time.Millisecond
	opts := StateUpdatorOpts{
		AutoUpdate:         true,
		AutoUpdateInterval: updateInterval,
	}
	member := NewStateUpdator("bucket1", "prefix1", objStore, opts)
	member.Start()
	defer member.Stop()

	runTime := 2 * time.Second
	time.Sleep(runTime)

	expectedSequenceMin := (runTime / updateInterval) - 1
	require.GreaterOrEqual(t, member.NextSequence(), int(expectedSequenceMin))
}

func updateWithBytesFunc(f func(s stateMachineState) (stateMachineState, error)) func(buff []byte) ([]byte, error) {
	fb := func(buff []byte) ([]byte, error) {
		var s stateMachineState
		if buff != nil {
			if err := json.Unmarshal(buff, &s); err != nil {
				return nil, err
			}
		}
		s, err := f(s)
		if err != nil {
			return nil, err
		}
		return json.Marshal(s)
	}
	return fb
}

func TestStateUpdatorJoinMember(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)

	numInitialMembers := 4
	members := make([]*StateUpdator, numInitialMembers)
	for i := 0; i < numInitialMembers; i++ {
		member := NewStateUpdator("bucket1", "prefix1", objStore, StateUpdatorOpts{})
		member.Start()
		defer member.Stop()
		members[i] = member
	}

	// do some updates on current members
	numUpdates := 100
	expectedSeq := 1
	for i := 0; i < numUpdates; i++ {
		for _, member := range members {
			_, err := member.update(updateWithBytesFunc(func(s stateMachineState) (stateMachineState, error) {
				s.Updates = append(s.Updates, fmt.Sprintf("update-%d", s.Val))
				s.Val++
				return s, nil
			}))
			require.NoError(t, err)
			require.Equal(t, expectedSeq, member.NextSequence())
			expectedSeq++
		}
	}

	// Now add a new member
	member := NewStateUpdator("bucket1", "prefix1", objStore, StateUpdatorOpts{})
	member.Start()
	defer member.Stop()

	_, err := member.update(updateWithBytesFunc(func(s stateMachineState) (stateMachineState, error) {
		s.Updates = append(s.Updates, fmt.Sprintf("update-%d", s.Val))
		s.Val++
		return s, nil
	}))
	require.NoError(t, err)
	// Should catch up and get correct sequence
	require.Equal(t, expectedSeq, member.NextSequence())
	expectedSeq++

}

func TestStateUpdatorLatestState(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)

	opts := StateUpdatorOpts{
		LatestStateBucketName: "latest-bucket",
	}

	prefix := "prefix1"

	numMembers := 4
	members := make([]*StateUpdator, numMembers)
	for i := 0; i < numMembers; i++ {
		member := NewStateUpdator("bucket1", prefix, objStore, opts)
		member.Start()
		defer member.Stop()
		members[i] = member

		require.NotEmpty(t, member.memberID)
	}

	numUpdates := 10
	expectedSeq := 1
	for i := 0; i < numUpdates; i++ {
		for _, member := range members {
			state, err := member.update(updateWithBytesFunc(func(s stateMachineState) (stateMachineState, error) {
				s.Updates = append(s.Updates, fmt.Sprintf("update-%d", s.Val))
				s.Val++
				return s, nil
			}))
			require.NoError(t, err)
			require.Equal(t, expectedSeq, member.NextSequence())
			expectedSeq++

			latestStateKey := fmt.Sprintf("%s-latest-state-%s", prefix, member.memberID)

			// Make sure latest state has been stored
			latestState, err := objStore.Get(context.Background(), opts.LatestStateBucketName, latestStateKey)
			require.NoError(t, err)
			require.NotNil(t, latestState)

			var smState stateMachineState
			err = json.Unmarshal(latestState, &smState)
			require.NoError(t, err)

			var expectedSmState stateMachineState
			err = json.Unmarshal(state, &expectedSmState)
			require.NoError(t, err)

			require.Equal(t, expectedSmState, smState)
		}
	}
}

// TestUnavailabilityRetry - verifies that the unavailability retry logic is exercised, but with low unavailability delays
// such that a re-initialisation is not triggered
func TestUnavailabilityRetry(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		uav := &unavailableObjStoreProxy{
			objStore: objStore,
		}
		uav.start(0, 100*time.Millisecond)
		objStores[i] = uav
	}
	defer func() {
		for _, objStore := range objStores {
			objStore.(*unavailableObjStoreProxy).stop()
		}
	}()
	opts := StateUpdatorOpts{
		AutoUpdate:                false,
		AvailabilityRetryInterval: 10 * time.Millisecond,
		MaxTimeBeforeReinitialise: 30 * time.Second,
	}
	runTime := 3 * time.Second
	members := applyLoadAndVerifyStateUpdator(t, runTime, numMembers, 10*time.Millisecond, opts, objStores)
	// Make sure they are initialised
	for _, member := range members {
		require.Equal(t, 0, int(atomic.LoadInt64(&member.reinitCount)))
		require.Greater(t, int(atomic.LoadInt64(&member.retryCount)), 1)
	}
}

// TestUnavailabilityTriggerReinitialise - tests that re-initialisation logic gets triggered when state machine is unable
// to update for a longer time
func TestUnavailabilityTriggerReinitialise(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		uav := &unavailableObjStoreProxy{
			objStore: objStore,
		}
		uav.start(500*time.Millisecond, 1*time.Second)
		objStores[i] = uav
	}
	defer func() {
		for _, objStore := range objStores {
			objStore.(*unavailableObjStoreProxy).stop()
		}
	}()
	opts := StateUpdatorOpts{
		AutoUpdate:                false,
		AvailabilityRetryInterval: 50 * time.Millisecond,
		MaxTimeBeforeReinitialise: 500 * time.Millisecond,
	}
	runTime := 3 * time.Second
	members := applyLoadAndVerifyStateUpdator(t, runTime, numMembers, 10*time.Millisecond, opts, objStores)
	// Make sure where initialised
	for _, member := range members {
		require.Greater(t, int(atomic.LoadInt64(&member.reinitCount)), 1)
	}
}

type stateMachineState struct {
	Val     int
	Updates []string
}

func applyLoadAndVerifyStateUpdator(t *testing.T, runTime time.Duration, numMembers int, updateDelay time.Duration,
	opts StateUpdatorOpts, objStores []objstore.Client) []*StateUpdator {

	var members []*StateUpdator
	for i := 0; i < numMembers; i++ {
		member := NewStateUpdator("bucket1", "prefix1", objStores[i], opts)
		members = append(members, member)
		members[i] = member
		member.Start()
		defer member.Stop()
	}

	endTime := arista.NanoTime() + uint64(runTime.Nanoseconds())
	var numUpdates int64

	stopChans := make([]chan error, numMembers)
	for i := 0; i < len(members); i++ {
		ch := make(chan error, 1)
		stopChans[i] = ch
		member := members[i]
		go func() {
			for arista.NanoTime() < endTime {
				_, err := member.Update(updateWithBytesFunc(func(s stateMachineState) (stateMachineState, error) {
					s.Updates = append(s.Updates, fmt.Sprintf("update-%d", s.Val))
					s.Val++
					return s, nil
				}))
				if err != nil {
					ch <- err
					return
				}
				atomic.AddInt64(&numUpdates, 1)
				time.Sleep(updateDelay)
			}
			ch <- nil
		}()
	}

	for _, ch := range stopChans {
		err := <-ch
		require.NoError(t, err)
	}

	// Do a final no-op update on each member to make sure it loads final state
	for _, member := range members {
		_, err := member.Update(updateWithBytesFunc(func(state stateMachineState) (stateMachineState, error) {
			return state, nil
		}))
		require.NoError(t, err)
	}

	totUpdates := int(atomic.LoadInt64(&numUpdates))
	require.Greater(t, totUpdates, 0)
	for _, member := range members {
		// State machine updates should be applied serially with multiple concurrent updates
		finalState, err := member.GetState()
		var s stateMachineState
		err = json.Unmarshal(finalState, &s)
		require.NoError(t, err)
		require.NoError(t, err)
		require.Equal(t, totUpdates, s.Val)
		require.Equal(t, totUpdates, len(s.Updates))
		for i, update := range s.Updates {
			require.Equal(t, fmt.Sprintf("update-%d", i), update)
		}
	}
	return members
}

type unavailableObjStoreProxy struct {
	unavailMinTime time.Duration
	unavailMaxTime time.Duration
	unavailable    atomic.Bool
	objStore       objstore.Client
	stopping       atomic.Bool
	stopWg         sync.WaitGroup
}

func (u *unavailableObjStoreProxy) start(unavailMinTime time.Duration, unavailMaxTime time.Duration) {
	u.unavailMinTime = unavailMinTime
	u.unavailMaxTime = unavailMaxTime
	u.stopWg.Add(1)
	go u.runLoop()
}

func (u *unavailableObjStoreProxy) stop() {
	u.stopping.Store(true)
	u.stopWg.Wait()
}

func (u *unavailableObjStoreProxy) runLoop() {
	for !u.stopping.Load() {
		u.delayRandom()
		u.unavailable.Store(true)
		u.delayRandom()
		u.unavailable.Store(false)
	}
	u.stopWg.Done()
}

func (u *unavailableObjStoreProxy) delayRandom() {
	unavailDur := u.unavailMinTime + time.Duration(rand.Intn(int(u.unavailMaxTime-u.unavailMinTime)))
	time.Sleep(unavailDur)
}

func (u *unavailableObjStoreProxy) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	if u.unavailable.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Get(ctx, bucket, key)
}

func (u *unavailableObjStoreProxy) Put(ctx context.Context, bucket string, key string, value []byte) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Put(ctx, bucket, key, value)
}

func (u *unavailableObjStoreProxy) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error) {
	if u.unavailable.Load() {
		return false, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.PutIfNotExists(ctx, bucket, key, value)
}

func (u *unavailableObjStoreProxy) Delete(ctx context.Context, bucket string, key string) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Delete(ctx, bucket, key)
}

func (u *unavailableObjStoreProxy) DeleteAll(ctx context.Context, bucket string, keys []string) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.DeleteAll(ctx, bucket, keys)
}

func (u *unavailableObjStoreProxy) ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	if u.unavailable.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.ListObjectsWithPrefix(ctx, bucket, prefix, maxKeys)
}

func (u *unavailableObjStoreProxy) Start() error {
	return u.objStore.Start()
}

func (u *unavailableObjStoreProxy) Stop() error {
	return u.objStore.Stop()
}
