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
		log.Infof("delivering update")
		for _, listener := range update.listeners {
			if err := listener(update.membership); err != nil {
				log.Errorf("Failed to send membership update: %v", err)
			}
		}
	}
}

func (i *InMemClusterMemberships) NewMembership(address string, listener MembershipListener) ClusterMembership {
	return &InMemMembership{
		memberships: i,
		address:     address,
		listener:    listener,
	}
}

func (i *InMemClusterMemberships) addMember(address string, listener MembershipListener) {
	i.lock.Lock()
	defer i.lock.Unlock()
	log.Infof("adding member %s", address)
	i.currentMembership.Members = append(i.currentMembership.Members, cluster.MembershipEntry{
		Address:    address,
		UpdateTime: time.Now().UnixMilli(),
	})
	i.listeners = append(i.listeners, listener)
	i.currentMembership.ClusterVersion++
	i.sendUpdate()
}

func (i *InMemClusterMemberships) removeMember(address string) {
	i.lock.Lock()
	defer i.lock.Unlock()
	var newMembers []cluster.MembershipEntry
	var newListeners []MembershipListener
	for j, member := range i.currentMembership.Members {
		if member.Address != address {
			newMembers = append(newMembers, member)
			newListeners = append(newListeners, i.listeners[j])
		}
	}
	i.currentMembership.Members = newMembers
	i.listeners = newListeners
	i.currentMembership.ClusterVersion++
	i.sendUpdate()
}

func (i *InMemClusterMemberships) sendUpdate() {
	listenersCopy := make([]MembershipListener, len(i.listeners))
	log.Infof("sending update")
	copy(listenersCopy, i.listeners)
	i.updatesChan <- updateInfo{
		membership: i.currentMembership,
		listeners:  listenersCopy,
	}
}

type InMemMembership struct {
	memberships *InMemClusterMemberships
	address     string
	listener    MembershipListener
}

func (i *InMemMembership) Start() error {
	i.memberships.addMember(i.address, i.listener)
	return nil
}

func (i *InMemMembership) Stop() error {
	i.memberships.removeMember(i.address)
	return nil
}
