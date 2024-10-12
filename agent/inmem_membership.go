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
	listeners         []MembershipListener
	updatesChan       chan updateInfo
}

type updateInfo struct {
	membership cluster.MembershipState
	listeners  []MembershipListener
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
		for _, listener := range update.listeners {
			if err := listener(update.membership); err != nil {
				log.Errorf("Failed to send membership update: %v", err)
			}
		}
	}
}

func (i *InMemClusterMemberships) NewMembership(id string, data []byte, listener MembershipListener) ClusterMembership {
	return &InMemMembership{
		memberships: i,
		id:          id,
		data:        data,
		listener:    listener,
	}
}

func (i *InMemClusterMemberships) addMember(id string, data []byte, listener MembershipListener) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.currentMembership.Members = append(i.currentMembership.Members, cluster.MembershipEntry{
		ID:         id,
		Data:       data,
		UpdateTime: time.Now().UnixMilli(),
	})
	i.listeners = append(i.listeners, listener)
	i.currentMembership.ClusterVersion++
	if len(i.currentMembership.Members) == 1 {
		i.currentMembership.LeaderVersion++
	}
	i.sendUpdate()
}

func (i *InMemClusterMemberships) removeMember(id string) {
	i.lock.Lock()
	defer i.lock.Unlock()
	var newMembers []cluster.MembershipEntry
	var newListeners []MembershipListener
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
	listenersCopy := make([]MembershipListener, len(i.listeners))
	copy(listenersCopy, i.listeners)
	i.updatesChan <- updateInfo{
		membership: i.currentMembership,
		listeners:  listenersCopy,
	}
}

type InMemMembership struct {
	memberships *InMemClusterMemberships
	id          string
	data        []byte
	listener    MembershipListener
}

func (i *InMemMembership) Start() error {
	i.memberships.addMember(i.id, i.data, i.listener)
	return nil
}

func (i *InMemMembership) Stop() error {
	i.memberships.removeMember(i.id)
	return nil
}
