package agent

import (
	"github.com/spirit-labs/tektite/cluster"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"time"
)

type InMemClusterMemberships struct {
	lock              sync.Mutex
	currentMembership cluster.MembershipState
	listeners         []listenerHolder
	updatesChan       chan updateInfo
	idSeq             int32
}

type updateInfo struct {
	thisMemberID int32
	membership   cluster.MembershipState
	listeners    []listenerHolder
}

func NewInMemClusterMemberships() *InMemClusterMemberships {
	return &InMemClusterMemberships{
		updatesChan: make(chan updateInfo, 10),
	}
}

func (i *InMemClusterMemberships) Start() {
	i.lock.Lock()
	defer i.lock.Unlock()
	go i.deliverUpdatesLoop()
}

func (i *InMemClusterMemberships) Stop() {
	i.lock.Lock()
	defer i.lock.Unlock()
	close(i.updatesChan)
}

func (i *InMemClusterMemberships) deliverUpdatesLoop() {
	for update := range i.updatesChan {
		for _, holder := range update.listeners {
			if err := holder.listener(holder.memberID, update.membership); err != nil {
				log.Errorf("Failed to send membership update: %v", err)
			}
		}
	}
}

func (i *InMemClusterMemberships) NewMembership(data []byte, listener MembershipListener) ClusterMembership {
	return &InMemMembership{
		memberships: i,
		data:        data,
		listener:    listener,
		id:          -1,
	}
}

func (i *InMemClusterMemberships) addMember(data []byte, listener MembershipListener) int32 {
	i.lock.Lock()
	defer i.lock.Unlock()
	id := i.idSeq
	i.currentMembership.Members = append(i.currentMembership.Members, cluster.MembershipEntry{
		ID:         id,
		Data:       data,
		UpdateTime: time.Now().UnixMilli(),
	})
	i.idSeq++
	i.listeners = append(i.listeners, listenerHolder{
		memberID: id,
		listener: listener,
	})
	i.currentMembership.ClusterVersion++
	if len(i.currentMembership.Members) == 1 {
		i.currentMembership.LeaderVersion++
	}
	i.sendUpdate()
	return id
}

type listenerHolder struct {
	memberID int32
	listener MembershipListener
}

func (i *InMemClusterMemberships) removeMember(id int32) {
	i.lock.Lock()
	defer i.lock.Unlock()
	var newMembers []cluster.MembershipEntry
	var newListeners []listenerHolder
	for j, member := range i.currentMembership.Members {
		if member.ID != id {
			newMembers = append(newMembers, member)
			newListeners = append(newListeners, i.listeners[j])
		} else if j == 0 {
			i.currentMembership.LeaderVersion++
		}
	}
	i.currentMembership.Members = newMembers
	i.listeners = newListeners
	i.currentMembership.ClusterVersion++
	i.sendUpdate()
}

func (i *InMemClusterMemberships) sendUpdate() {
	listenersCopy := make([]listenerHolder, len(i.listeners))
	copy(listenersCopy, i.listeners)
	i.updatesChan <- updateInfo{
		membership: i.currentMembership,
		listeners:  listenersCopy,
	}
}

type InMemMembership struct {
	memberships *InMemClusterMemberships
	id          int32
	data        []byte
	listener    MembershipListener
}

func (i *InMemMembership) Start() error {
	i.id = i.memberships.addMember(i.data, i.listener)
	return nil
}

func (i *InMemMembership) Stop() error {
	i.memberships.removeMember(i.id)
	return nil
}
