package cluster

import (
	"fmt"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestJoinSequential(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			err := membership.Stop()
			require.NoError(t, err)
		}
	}()
	numMembers := 10
	var receivedStates []*membershipReceivedStates
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		receivedState := &membershipReceivedStates{}
		receivedStates = append(receivedStates, receivedState)
		membership := NewMembership(createConfig(), address, objStore, receivedState.membershipChanged)
		err := membership.Start()
		require.NoError(t, err)
		memberships = append(memberships, membership)
		waitForMembers(t, memberships...)
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, i+1, state.ClusterVersion)
		require.Equal(t, 1, state.LeaderVersion)
	}
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, numMembers, state.ClusterVersion)
		require.Equal(t, numMembers, len(state.Members))
		// should be in order of joining
		for i, membership2 := range memberships {
			require.Equal(t, membership2.address, state.Members[i].Address)
		}
	}
	for i, receivedState := range receivedStates {
		memShips := receivedState.getMemberships()
		require.Equal(t, numMembers-i, len(memShips))
		for j, membership := range memShips {
			require.Equal(t, i+j+1, len(membership.Members))
			for k := i + j; k < len(membership.Members); k++ {
				require.Equal(t, memberships[k].address, membership.Members[k].Address)
			}
		}
	}
}

type membershipReceivedStates struct {
	lock     sync.Mutex
	received []MembershipState
}

func (m *membershipReceivedStates) membershipChanged(membership MembershipState) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.received = append(m.received, membership)
	return nil
}

func (m *membershipReceivedStates) getMemberships() []MembershipState {
	m.lock.Lock()
	defer m.lock.Unlock()
	copied := make([]MembershipState, len(m.received))
	copy(copied, m.received)
	return copied
}

func createConfig() MembershipConf {
	cfg := NewMembershipConf()
	cfg.UpdateInterval = 100 * time.Millisecond
	cfg.EvictionDuration = 5 * time.Second
	return cfg
}

func TestJoinParallel(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			err := membership.Stop()
			require.NoError(t, err)
		}
	}()
	numMembers := 10
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberShip := NewMembership(createConfig(), address, objStore, func(state MembershipState) error {
			return nil
		})
		err := memberShip.Start()
		require.NoError(t, err)
		// Randomize join time a little
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		memberships = append(memberships, memberShip)
	}
	// Wait for all members to join
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, numMembers, state.ClusterVersion)
		require.Equal(t, 1, state.LeaderVersion)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			err := membership.Stop()
			require.NoError(t, err)
		}
	}()
	numMembers := 5
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		cfg := createConfig()
		cfg.EvictionDuration = 1 * time.Second
		memberShip := NewMembership(cfg, address, objStore, func(state MembershipState) error {
			return nil
		})
		err := memberShip.Start()
		require.NoError(t, err)
		memberships = append(memberships, memberShip)
	}
	waitForMembers(t, memberships...)
	for i := 0; i < numMembers-1; i++ {
		index := rand.Intn(len(memberships))
		stoppedAddress := memberships[index].address
		// stop it
		err := memberships[index].Stop()
		require.NoError(t, err)
		memberships = append(memberships[:index], memberships[index+1:]...)
		// wait to be evicted
		waitForMembers(t, memberships...)
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			require.Equal(t, numMembers+i+1, state.ClusterVersion)
			for _, member := range state.Members {
				require.NotEqual(t, stoppedAddress, member.Address)
			}
		}
	}
	finalState, err := memberships[0].GetState()
	require.NoError(t, err)
	require.Equal(t, 1, len(finalState.Members))
}

func TestLeaderVersionChangedOnEviction(t *testing.T) {
	t.Parallel()
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			err := membership.Stop()
			require.NoError(t, err)
		}
	}()
	numMembers := 5
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		cfg := createConfig()
		cfg.EvictionDuration = 1 * time.Second
		memberShip := NewMembership(cfg, address, objStore, func(state MembershipState) error {
			return nil
		})
		err := memberShip.Start()
		require.NoError(t, err)
		memberships = append(memberships, memberShip)
		waitForMembers(t, memberships...)
	}

	// evict 0 - should cause leader version to increment
	err := memberships[0].Stop()
	require.NoError(t, err)
	memberships = memberships[1:]
	// wait to be evicted
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 1 - not leader so shouldn't cause increment
	err = memberships[1].Stop()
	require.NoError(t, err)
	memberships = append(memberships[0:1], memberships[2:]...)
	// wait to be evicted
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 2 - not leader so shouldn't cause increment
	err = memberships[2].Stop()
	require.NoError(t, err)
	memberships = memberships[:2]
	// wait to be evicted
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 0 - leader
	err = memberships[0].Stop()
	require.NoError(t, err)
	memberships = memberships[1:]
	// wait to be evicted
	waitForMembers(t, memberships...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 3, state.LeaderVersion)
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
