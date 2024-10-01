package cluster

import (
	"encoding/json"
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

type Membership struct {
	updateInterval    time.Duration
	evicationDuration time.Duration
	updateTimer       *time.Timer
	lock                 sync.Mutex
	started              bool
	stateUpdator         *StateUpdater
	address              string
	leader               bool
	currentState MembershipState
	stateChangedCallback func(state MembershipState) error
}

type MembershipConf struct {
	BucketName string
	KeyPrefix string
	UpdateInterval time.Duration
	EvictionDuration time.Duration
}

func NewMembershipConf() MembershipConf {
	return MembershipConf{
		BucketName: "tektite-membership",
		KeyPrefix: "tektite-membership",
		UpdateInterval: 5 * time.Second,
		EvictionDuration: 20 * time.Second,
	}
}

func (m *MembershipConf) Validate() error {
	return nil
}

func NewMembership(cfg MembershipConf, address string, objStoreClient objstore.Client, stateChangedCallback func(state MembershipState) error) *Membership {
	return &Membership{
		address:              address,
		stateUpdator:         NewStateUpdator(cfg.BucketName, cfg.KeyPrefix, objStoreClient, StateUpdatorOpts{}),
		updateInterval:       cfg.UpdateInterval,
		evicationDuration:    cfg.EvictionDuration,
		stateChangedCallback: stateChangedCallback,
	}
}

func (m *Membership) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	m.stateUpdator.Start()
	m.scheduleTimer()
	m.started = true
	return nil
}

func (m *Membership) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	m.updateTimer.Stop()
	m.stateUpdator.Stop()
	return nil
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
	buff, err := m.stateUpdator.Update(m.updateState)
	if err != nil {
		return err
	}
	var newState MembershipState
	err = json.Unmarshal(buff, &newState)
	if err != nil {
		return err
	}
	if membershipChanged(m.currentState.Members, newState.Members) {
		if err := m.stateChangedCallback(newState); err != nil {
			log.Errorf("failed to call membership state changed callback: %v", err)
		}
	}
	m.currentState = newState
	return nil
}

func membershipChanged(oldMembers []MembershipEntry, newMembers []MembershipEntry) bool {
	if len(oldMembers) != len(newMembers) {
		return true
	}
	for i, oldMember := range oldMembers {
		if oldMember.Address != newMembers[i].Address {
			return true
		}
	}
	return false
}

func (m *Membership) updateState(buff []byte) ([]byte, error) {
	var memberShipState MembershipState
	if buff != nil {
		err := json.Unmarshal(buff, &memberShipState)
		if err != nil {
			return nil, err
		}
	}
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
			if now-member.UpdateTime >= m.evicationDuration.Milliseconds() {
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
		memberShipState.ClusterVersion++
	}
	return json.Marshal(&memberShipState)
}

type MembershipState struct {
	ClusterVersion int               // ClusterVersion changes every time member joins or leaves the group
	Members        []MembershipEntry // The members of the group, we define the first member to be the leader
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
	buff, err := m.stateUpdator.GetState()
	if err != nil {
		return MembershipState{}, err
	}
	if buff == nil {
		return MembershipState{}, nil
	}
	memberShipState := MembershipState{}
	err = json.Unmarshal(buff, &memberShipState)
	if err != nil {
		return MembershipState{}, err
	}
	return memberShipState, nil
}
