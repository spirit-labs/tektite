package cluster

import (
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
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
	memberStateChanges := make([]stateChanges, numMembers)
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		membership := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond,
			5*time.Second, memberStateChanges[i].stateChanged)
		membership.Start()
		memberships = append(memberships, membership)
		waitForMembers(t, memberships...)
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, i+1, state.Epoch)
	}
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
	for i := 0; i < numMembers; i++ {
		changes := memberStateChanges[i].getStates()
		require.Equal(t, numMembers-i, len(changes))
		for j, membership := range changes {
			var expectedAddresses []string
			for k := 0; k < i+j+1; k++ {
				expectedAddresses = append(expectedAddresses, fmt.Sprintf("address-%d", k))
			}
			require.Equal(t, i+j+1, membership.Epoch)
			require.Equal(t, j+i+1, len(membership.Members))
			for k := 0; k < len(membership.Members); k++ {
				require.Equal(t, expectedAddresses[k], membership.Members[k].Address)
			}
		}
	}
}

type stateChanges struct {
	lock   sync.Mutex
	states []MembershipState
}

func (s *stateChanges) stateChanged(state MembershipState) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.states = append(s.states, state)
}

func (s *stateChanges) getStates() []MembershipState {
	s.lock.Lock()
	defer s.lock.Unlock()
	statesCopy := make([]MembershipState, len(s.states))
	copy(statesCopy, s.states)
	return statesCopy
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
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond,
			5*time.Second, func(state MembershipState) {
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
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond,
			1*time.Second, func(state MembershipState) {
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
	leaderCallbackCalledCounts := make([]atomic.Bool, numMembers)
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberIndex := i
		memberShip := NewMembership("bucket1", "prefix1", address, objStore, 100*time.Millisecond,
			1*time.Second, func(state MembershipState) {
				if state.Members[0].Address == address {
					// member became leader
					leaderCallbackCalledCounts[memberIndex].Store(true)
				}
			})
		memberShip.Start()
		memberships = append(memberships, memberShip)
		waitForMembers(t, memberships...)
	}

	waitForMembers(t, memberships...)
	require.True(t, leaderCallbackCalledCounts[0].Load())
	for i := 1; i < numMembers; i++ {
		require.False(t, leaderCallbackCalledCounts[i].Load())
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
		require.True(t, leaderCallbackCalledCounts[i].Load())
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
