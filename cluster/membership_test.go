package cluster

import (
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinSequential(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()
	numMembers := 10
	var becomeLeaderCalledCount atomic.Int64
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberIndex := i
		membership := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond, 5*time.Second, func() {
			if memberIndex != 0 {
				panic("becomeLeader callback called for wrong member")
			}
			becomeLeaderCalledCount.Add(1)
		})
		membership.Start()
		memberships = append(memberships, membership)
		waitForMembers(t, memberships...)
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, i+1, state.Epoch)
	}
	require.Equal(t, 1, int(becomeLeaderCalledCount.Load()))
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		log.Infof("epoch is %d", state.Epoch)
		require.Equal(t, numMembers, state.Epoch)
		require.Equal(t, numMembers, len(state.Members))
		// should be in order of joining
		for i, membership2 := range memberships {
			require.Equal(t, membership2.address, state.Members[i].Address)
		}
	}
}

func TestJoinParallel(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()
	numMembers := 10
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond, 5*time.Second, func() {
		})
		memberShip.Start()
		// Randomise join time a little
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		memberships = append(memberships, memberShip)
	}
	// Wait for all members to join
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		log.Infof("epoch is %d", state.Epoch)
		require.Equal(t, numMembers, state.Epoch)
	}
}

func TestNonLeadersEvicted(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()
	numMembers := 5
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond, 1*time.Second, func() {
		})
		memberShip.Start()
		memberships = append(memberships, memberShip)
	}
	waitForMembers(t, memberships...)
	leaderAddress := memberships[0].address
	for i := 0; i < numMembers-1; i++ {
		// choose a member at random - but not the leader
		index := 1 + rand.Intn(len(memberships)-1)
		// stop it
		memberships[index].Stop()
		memberships = append(memberships[:index], memberships[index+1:]...)
		// wait to be evicted
		waitForMembers(t, memberships...)
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			require.Equal(t, numMembers+i+1, state.Epoch)
		}
	}
	finalState, err := memberships[0].GetState()
	require.NoError(t, err)
	require.Equal(t, 1, len(finalState.Members))
	require.Equal(t, leaderAddress, finalState.Members[0].Address)
}

func TestLeaderEvicted(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()
	numMembers := 5
	leaderCallbackCalledCounts := make([]int64, numMembers)
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberIndex := i
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond, 1*time.Second, func() {
			newVal := atomic.AddInt64(&leaderCallbackCalledCounts[memberIndex], 1)
			if newVal != 1 {
				panic("leader callback called too many times")
			}
		})
		memberShip.Start()
		memberships = append(memberships, memberShip)
		waitForMembers(t, memberships...)
	}
	waitForMembers(t, memberships...)
	require.Equal(t, 1, int(atomic.LoadInt64(&leaderCallbackCalledCounts[0])))
	for i := 1; i < numMembers; i++ {
		require.Equal(t, 0, int(atomic.LoadInt64(&leaderCallbackCalledCounts[i])))
	}
	for i := 0; i < numMembers-1; i++ {
		expectedNewLeader := memberships[1].address
		// stop current leader
		memberships[0].Stop()
		memberships = memberships[1:]
		// wait for leader to be evicted
		waitForMembers(t, memberships...)
		require.Equal(t, expectedNewLeader, memberships[0].address)
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			require.Equal(t, numMembers+i+1, state.Epoch)
		}
	}
	for i := 0; i < numMembers; i++ {
		require.Equal(t, 1, int(atomic.LoadInt64(&leaderCallbackCalledCounts[i])))
	}
}

func waitForMembers(t *testing.T, memberships ...*Membership) {
	allAddresses := map[string]struct{}{}
	for _, membership := range memberships {
		allAddresses[membership.address] = struct{}{}
	}
	start := time.Now()
	for {
		ok := true
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			if len(state.Members) == len(memberships) {
				for _, member := range state.Members {
					_, exists := allAddresses[member.Address]
					require.True(t, exists)
				}
			} else {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().Sub(start) > 5*time.Second {
			require.Fail(t, "timed out waiting for memberships")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
