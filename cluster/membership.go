package cluster

import (
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

type Membership struct {
	updateInterval       time.Duration
	evictionInterval     time.Duration
	updateTimer          *time.Timer
	lock                 sync.Mutex
	started              bool
	stateMachine         *StateMachine[MembershipState]
	address              string
	leader               bool
	becomeLeaderCallback func()
}

func NewMembership(bucket string, keyPrefix string, address string, objStoreClient objstore.Client, updateInterval time.Duration,
	evictionInterval time.Duration, becomeLeaderCallback func()) *Membership {
	return &Membership{
		address:              address,
		stateMachine:         NewStateMachine[MembershipState](bucket, keyPrefix, objStoreClient, StateMachineOpts{}),
		updateInterval:       updateInterval,
		evictionInterval:     evictionInterval,
		becomeLeaderCallback: becomeLeaderCallback,
	}
}

func (m *Membership) Start() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return
	}
	m.stateMachine.Start()
	m.scheduleTimer()
	m.started = true
}

func (m *Membership) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return
	}
	m.started = false
	m.updateTimer.Stop()
	m.stateMachine.Stop()
}

func (m *Membership) scheduleTimer() {
	m.updateTimer = time.AfterFunc(m.updateInterval, m.updateOnTimer)
}

func (m *Membership) updateOnTimer() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return
	}
	if err := m.update(); err != nil {
		log.Errorf("failed to update membership: %v", err)
	}
	m.scheduleTimer()
}

func (m *Membership) update() error {
	newState, err := m.stateMachine.Update(m.updateState)
	if err != nil {
		return err
	}
	if newState.Members[0].Address == m.address {
		if !m.leader {
			m.becomeLeaderCallback()
		}
		m.leader = true
	}
	return nil
}

func (m *Membership) updateState(memberShipState MembershipState) (MembershipState, error) {
	now := time.Now().UnixMilli()
	found := false
	var newMembers []MembershipEntry
	changed := false
	for _, member := range memberShipState.Members {
		if member.Address == m.address {
			// When we update we preserve position in the slice
			member.UpdateTime = now
			found = true
		} else {
			if now-member.UpdateTime >= m.evictionInterval.Milliseconds() {
				// member evicted
				changed = true
				continue
			}
		}
		newMembers = append(newMembers, member)
	}
	if !found {
		// Add the new member on the end
		newMembers = append(newMembers, MembershipEntry{
			Address:    m.address,
			UpdateTime: now,
		})
		changed = true
	}
	memberShipState.Members = newMembers
	if changed {
		// NewmMember joined or member(s) where evicted, so we change the epoch
		memberShipState.Epoch++
	}
	return memberShipState, nil
}

type MembershipState struct {
	Epoch   int               // Epoch changes every time member joins or leaves the group
	Members []MembershipEntry // The members of the group, we define the first member to be the leader
}

type MembershipEntry struct {
	Address    string // Address of the member
	UpdateTime int64  // Time the member last updated itself, in Unix millis
}

func (m *Membership) GetState() (MembershipState, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return MembershipState{}, errors.New("not started")
	}
	return m.stateMachine.GetState()
}
