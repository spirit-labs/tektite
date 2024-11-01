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
		data := []byte(fmt.Sprintf("data-%d", i))
		receivedState := &membershipReceivedStates{}
		receivedStates = append(receivedStates, receivedState)
		membership := NewMembership(createConfig(), data, objStore, receivedState.membershipChanged)
		err := membership.Start()
		require.NoError(t, err)
		memberships = append(memberships, membership)
		waitForMembers2(t, receivedStates...)
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
			require.Equal(t, membership2.id, state.Members[i].ID)
		}
	}
	ids := map[int32]struct{}{}
	for i, receivedState := range receivedStates {

		// check id is unique
		id := receivedState.getThisID()
		_, exists := ids[id]
		require.False(t, exists)
		ids[id] = struct{}{}

		memShips := receivedState.getMemberships()
		require.Equal(t, numMembers-i, len(memShips))
		for j, membership := range memShips {
			require.Equal(t, i+j+1, len(membership.Members))
			for k := i + j; k < len(membership.Members); k++ {
				require.Equal(t, memberships[k].id, membership.Members[k].ID)
				require.Equal(t, memberships[k].data, membership.Members[k].Data)
			}
		}
	}
}

type membershipReceivedStates struct {
	lock           sync.Mutex
	receivedThisID int32
	received       []MembershipState
}

func (m *membershipReceivedStates) membershipChanged(thisID int32, membership MembershipState) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.received = append(m.received, membership)
	if m.receivedThisID != 0 && m.receivedThisID != thisID {
		panic("received wrong id")
	}
	m.receivedThisID = thisID
	return nil
}

func (m *membershipReceivedStates) getMemberships() []MembershipState {
	m.lock.Lock()
	defer m.lock.Unlock()
	copied := make([]MembershipState, len(m.received))
	copy(copied, m.received)
	return copied
}

func (m *membershipReceivedStates) getThisID() int32 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.receivedThisID
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
	var receiverStates []*membershipReceivedStates
	for i := 0; i < numMembers; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		receivedState := &membershipReceivedStates{}
		receiverStates = append(receiverStates, receivedState)
		memberShip := NewMembership(createConfig(), data, objStore, receivedState.membershipChanged)
		err := memberShip.Start()
		require.NoError(t, err)
		memberships = append(memberships, memberShip)
	}
	// Wait for all members to join
	waitForMembers2(t, receiverStates...)
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
	var receiverStates []*membershipReceivedStates
	for i := 0; i < numMembers; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		cfg := createConfig()
		cfg.EvictionDuration = 1 * time.Second
		receiverState := &membershipReceivedStates{}
		receiverStates = append(receiverStates, receiverState)
		memberShip := NewMembership(cfg, data, objStore, receiverState.membershipChanged)
		err := memberShip.Start()
		require.NoError(t, err)
		memberships = append(memberships, memberShip)
	}
	waitForMembers2(t, receiverStates...)
	for i := 0; i < numMembers-1; i++ {
		index := rand.Intn(len(memberships))
		stoppedAddress := memberships[index].id
		// stop it
		err := memberships[index].Stop()
		require.NoError(t, err)
		memberships, receiverStates = removeMembership(index, memberships, receiverStates)
		// wait to be evicted
		waitForMembers2(t, receiverStates...)
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			require.Equal(t, numMembers+i+1, state.ClusterVersion)
			for _, member := range state.Members {
				require.NotEqual(t, stoppedAddress, member.ID)
			}
		}
	}
	finalState, err := memberships[0].GetState()
	require.NoError(t, err)
	require.Equal(t, 1, len(finalState.Members))
}

func removeMembership(index int, memberships []*Membership, receiverStates []*membershipReceivedStates) ([]*Membership, []*membershipReceivedStates) {
	var newMemberships []*Membership
	newMemberships = append(newMemberships, memberships[:index]...)
	newMemberships = append(newMemberships, memberships[index+1:]...)
	memberships = newMemberships

	var newReceiverStates []*membershipReceivedStates
	newReceiverStates = append(newReceiverStates, receiverStates[:index]...)
	newReceiverStates = append(newReceiverStates, receiverStates[index+1:]...)
	receiverStates = newReceiverStates

	return newMemberships, newReceiverStates
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
	var receiverStates []*membershipReceivedStates
	for i := 0; i < numMembers; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		cfg := createConfig()
		cfg.EvictionDuration = 1 * time.Second
		receivedState := &membershipReceivedStates{}
		receiverStates = append(receiverStates, receivedState)
		memberShip := NewMembership(cfg, data, objStore, receivedState.membershipChanged)
		err := memberShip.Start()
		require.NoError(t, err)
		memberships = append(memberships, memberShip)
		waitForMembers2(t, receiverStates...)
	}

	// evict 0 - should cause leader version to increment
	err := memberships[0].Stop()
	require.NoError(t, err)
	memberships, receiverStates = removeMembership(0, memberships, receiverStates)
	// wait to be evicted
	waitForMembers2(t, receiverStates...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 1 - not leader so shouldn't cause increment
	err = memberships[1].Stop()
	require.NoError(t, err)
	memberships, receiverStates = removeMembership(1, memberships, receiverStates)
	// wait to be evicted
	waitForMembers2(t, receiverStates...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 2 - not leader so shouldn't cause increment
	err = memberships[2].Stop()
	require.NoError(t, err)
	memberships, receiverStates = removeMembership(2, memberships, receiverStates)
	// wait to be evicted
	waitForMembers2(t, receiverStates...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 2, state.LeaderVersion)
	}

	// evict 0 - leader
	err = memberships[0].Stop()
	require.NoError(t, err)
	memberships, receiverStates = removeMembership(0, memberships, receiverStates)
	// wait to be evicted
	waitForMembers2(t, receiverStates...)
	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, 3, state.LeaderVersion)
	}
}

func waitForMembers2(t *testing.T, receivedStates ...*membershipReceivedStates) {
	start := time.Now()
	for {
		ok := true
		for _, receivedState := range receivedStates {
			memberStates := receivedState.getMemberships()
			if len(memberStates) == 0 || len(memberStates[len(memberStates)-1].Members) != len(receivedStates) {
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
