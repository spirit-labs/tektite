package cluster

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"math"
	"sync"
	"time"
)

type Membership struct {
	updateInterval       time.Duration
	evictionDuration     time.Duration
	updateTimer          *time.Timer
	lock                 sync.Mutex
	started              bool
	stateUpdater         *StateUpdater
	id                   int32
	candidateID          int32
	data                 []byte
	leader               bool
	currentState         MembershipState
	stateChangedCallback func(thisMemberID int32, state MembershipState) error
}

type MembershipConf struct {
	BucketName       string
	KeyPrefix        string
	UpdateInterval   time.Duration
	EvictionInterval time.Duration
}

func NewMembershipConf() MembershipConf {
	return MembershipConf{
		BucketName:       "tektite-membership",
		KeyPrefix:        "tektite-membership",
		UpdateInterval:   5 * time.Second,
		EvictionInterval: 20 * time.Second,
	}
}

func (m *MembershipConf) Validate() error {
	return nil
}

func NewMembership(cfg MembershipConf, data []byte, objStoreClient objstore.Client,
	stateChangedCallback func(thisMemberID int32, state MembershipState) error) *Membership {
	return &Membership{
		id:                   -1,
		data:                 data,
		stateUpdater:         NewStateUpdater(cfg.BucketName, cfg.KeyPrefix, objStoreClient, StateUpdaterOpts{}),
		updateInterval:       cfg.UpdateInterval,
		evictionDuration:     cfg.EvictionInterval,
		stateChangedCallback: stateChangedCallback,
	}
}

func (m *Membership) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	m.stateUpdater.Start()
	m.scheduleTimer()
	m.started = true
	return nil
}

func (m *Membership) Stop() error {
	m.stateUpdater.SetStopping() // Needs to be done outside lock
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	m.updateTimer.Stop()
	m.stateUpdater.Stop()
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
	buff, err := m.stateUpdater.Update(m.updateState)
	if err != nil {
		return err
	}
	// Update succeeded so set member id if not already set
	if m.id == -1 && m.candidateID != -1 {
		m.id = m.candidateID
		m.candidateID = -1
	}
	var newState MembershipState
	err = json.Unmarshal(buff, &newState)
	if err != nil {
		return err
	}
	if membershipChanged(m.currentState.Members, newState.Members) {
		if m.id == -1 {
			panic("member id not generated")
		}
		if err := m.stateChangedCallback(m.id, newState); err != nil {
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
		if oldMember.ID != newMembers[i].ID {
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
	leaderChanged := false
	for i, member := range memberShipState.Members {
		if member.ID == m.id {
			// When we update we preserve position in the slice
			member.UpdateTime = now
			found = true
		} else {
			if now-member.UpdateTime >= m.evictionDuration.Milliseconds() {
				// member evicted
				log.Infof("attempting to evict cluster member %d from membership", member.ID)
				changed = true
				if i == 0 {
					// leader evicted
					leaderChanged = true
				}
				continue
			}
		}
		newMembers = append(newMembers, member)
	}
	if !found {
		// Generate an id
		m.candidateID = m.generateMemberID(&memberShipState)
		// Add ourself on the end
		newMembers = append(newMembers, MembershipEntry{
			ID:         m.candidateID,
			Data:       m.data,
			UpdateTime: now,
		})
		changed = true
		if len(newMembers) == 1 {
			// First member
			leaderChanged = true
		}
	}
	memberShipState.Members = newMembers
	if changed {
		// New member joined or member(s) where evicted, so we increment the cluster version
		memberShipState.ClusterVersion++
	}
	if leaderChanged {
		// leader changed, so we increment the leader version
		memberShipState.LeaderVersion++
	}
	return json.Marshal(&memberShipState)
}

func (m *Membership) generateMemberID(memberShipState *MembershipState) int32 {
	// we take the last 32 bits of the cluster version - it will wrap around if cluster version
	// gets bigger than max int32 - this is ok
	startID := int32(memberShipState.ClusterVersion)
	candidateID := startID
	for {
		// Sanity check that no members have this id already, note, this is *very* unlikely is it would require at least
		// one of the members to remain as other members were added/removed over 2 billion times. If it does exist
		// we try the next one
		for _, member := range memberShipState.Members {
			if member.ID == candidateID {
				if candidateID == math.MaxInt32 {
					// wrap to zero
					candidateID = 0
				} else {
					// try the next one
					candidateID++
				}
				if candidateID == startID {
					panic("cannot generate member ID")
				}
				continue
			}
		}
		break
	}
	return candidateID
}

type MembershipState struct {
	// LeaderVersion increments every time the member at position zero in the Members changes
	// This member can be considered the leader and hosts the controller
	LeaderVersion int
	// ClusterVersion increments every time any member joins or leaves the group
	ClusterVersion int
	// The members of the group, we define the first member to be the leader
	Members []MembershipEntry
}

type MembershipEntry struct {
	ID         int32  // Unique ID of the member in the cluster - 32 bits for compatibility with kafka node id
	Data       []byte // Arbitrary data added by the member
	UpdateTime int64  // Time the member last updated itself, in Unix millis
}

func (m *Membership) GetState() (MembershipState, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return MembershipState{}, errors.New("membership not started")
	}
	buff, err := m.stateUpdater.GetState()
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

func ChooseMemberAddressForHash(partHash []byte, members []MembershipEntry) (string, bool) {
	if len(members) == 0 {
		return "", false
	}
	memberID := common.CalcMemberForHash(partHash, len(members))
	data := members[memberID].Data
	var memberData common.MembershipData
	memberData.Deserialize(data, 0)
	return memberData.ClusterListenAddress, true
}
