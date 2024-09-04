package cluster

import (
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

type Membership struct {
	updateInterval            time.Duration
	evictionInterval          time.Duration
	updateTimer               *time.Timer
	lock                      sync.RWMutex
	started                   bool
	stateMachine              *StateMachine[MembershipState]
	address                   string
	currentState              MembershipState
	membershipChangedCallback func(state MembershipState)
	sentFirstUpdate           bool
	valid                     bool
}

func NewMembership(bucket string, keyPrefix string, address string, objStoreClient objstore.Client, updateInterval time.Duration,
	evictionInterval time.Duration, membershipChangedCallback func(state MembershipState)) *Membership {
	return &Membership{
		address:                   address,
		stateMachine:              NewStateMachine[MembershipState](bucket, keyPrefix, objStoreClient, StateMachineOpts{}),
		updateInterval:            updateInterval,
		evictionInterval:          evictionInterval,
		membershipChangedCallback: membershipChangedCallback,
		currentState:              MembershipState{Epoch: -1},
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

func (m *Membership) SetValid(valid bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.valid = valid
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
	same := true
	if len(newState.Members) == len(m.currentState.Members) {
		for i, member := range m.currentState.Members {
			if member.Address != m.currentState.Members[i].Address {
				same = false
				break
			}
		}
	} else {
		same = false
	}
	m.currentState = newState
	if !same || !m.sentFirstUpdate {
		m.membershipChangedCallback(newState)
	}
	m.sentFirstUpdate = true
	return nil
}

// membershipState is immutable
func (m *Membership) updateState(memberShipState MembershipState) (MembershipState, error) {
	if !m.valid && len(memberShipState.Members) >= 0 {
		// We only update our entry if we are valid, or we're the first member and will become leader
		return memberShipState, nil
	}
	now := time.Now().UnixMilli()
	found := false
	var newState MembershipState
	var newMembers []MembershipEntry
	leaderChanged := len(memberShipState.Members) == 0
	for i, member := range memberShipState.Members {
		if member.Address == m.address {
			// When we update we preserve position in the slice
			member.UpdateTime = now
			found = true
		} else {
			if now-member.UpdateTime >= m.evictionInterval.Milliseconds() {
				// member evicted
				if i == 0 {
					leaderChanged = true
				}
				continue
			}
		}
		newMembers = append(newMembers, member)
	}
	if !found {
		newMembers = append(newMembers, MembershipEntry{
			Address:    m.address,
			UpdateTime: now,
		})
	}
	newState.Members = newMembers
	newState.Epoch = memberShipState.Epoch
	if leaderChanged {
		// Leader changed we increment the epoch
		newState.Epoch++
	}
	return newState, nil
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
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return MembershipState{}, errors.New("not started")
	}
	return m.currentState, nil
}

func (m *Membership) IsLeader(address string) (bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return false, errors.New("not started")
	}
	state, err := m.stateMachine.GetState()
	if err != nil {
		return false, err
	}
	if len(state.Members) > 0 && state.Members[0].Address == address {
		return true, nil
	}
	return false, nil
}
