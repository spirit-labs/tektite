package cluster

import (
	"context"
	"encoding/json"
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

func TestStateUpdater(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	numMembers := 1
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		objStores[i] = objStore
	}
	runTime := 3 * time.Second
	applyLoadAndVerifyStateUpdater(t, runTime, numMembers, 10*time.Millisecond, StateUpdaterOpts{}, objStores)
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

// TestUnavailabilityRetry - verifies that the unavailability retry logic is exercised
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
	opts := StateUpdaterOpts{
		AvailabilityRetryInterval: 10 * time.Millisecond,
	}
	runTime := 3 * time.Second
	members := applyLoadAndVerifyStateUpdater(t, runTime, numMembers, 10*time.Millisecond, opts, objStores)
	// Make sure they are initialised
	for _, member := range members {
		require.Greater(t, int(atomic.LoadInt64(&member.retryCount)), 1)
	}
}

type stateMachineState struct {
	Val int
}

func applyLoadAndVerifyStateUpdater(t *testing.T, runTime time.Duration, numMembers int, updateDelay time.Duration,
	opts StateUpdaterOpts, objStores []objstore.Client) []*StateUpdater {

	var members []*StateUpdater
	for i := 0; i < numMembers; i++ {
		member := NewStateUpdater("bucket1", "prefix1", objStores[i], opts)
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

	// Do a final fetch on each member to make sure it loads final state
	for _, member := range members {
		_, err := member.FetchLatestState()
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

func (u *unavailableObjStoreProxy) GetObjectInfo(ctx context.Context, bucket string, key string) (objstore.ObjectInfo, bool, error) {
	if u.unavailable.Load() {
		return objstore.ObjectInfo{}, false, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.GetObjectInfo(ctx, bucket, key)
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

func (u *unavailableObjStoreProxy) PutIfMatchingEtag(ctx context.Context, bucket string, key string, value []byte, etag string) (bool, string, error) {
	if u.unavailable.Load() {
		return false, "", common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.PutIfMatchingEtag(ctx, bucket, key, value, etag)
}

func (u *unavailableObjStoreProxy) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, string, error) {
	if u.unavailable.Load() {
		return false, "", common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
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
