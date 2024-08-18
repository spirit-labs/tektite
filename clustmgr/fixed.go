package clustmgr

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync"
)

// FixedStateManager is used in integration tests so we can test using a cluster with a fixed number of nodes and
// groups, so we can start-up quickly and don't need a dependency on etcd, which can slow tests down
type FixedStateManager struct {
	startStopLock sync.Mutex
	handlers      []ClusterStateHandler
	state         *ClusterState
	lock          sync.Mutex
	csChan        chan csHolder
	closeWg       sync.WaitGroup
	frozen        bool
	started       bool
}

type csHolder struct {
	cs       ClusterState
	handlers []ClusterStateHandler
}

func NewFixedStateManager(numGroups int, numNodes int) *FixedStateManager {
	var groupStates [][]GroupNode
	for i := 0; i < numGroups; i++ {
		leader := i % numNodes
		var groupNodes []GroupNode
		for j := 0; j < numNodes; j++ {
			groupNodes = append(groupNodes, GroupNode{
				NodeID:        j,
				Leader:        j == leader,
				Valid:         false,
				JoinedVersion: 0,
			})
		}
		groupStates = append(groupStates, groupNodes)
	}
	cs := &ClusterState{
		Version:     1,
		GroupStates: groupStates,
	}
	return &FixedStateManager{
		state:  cs,
		csChan: make(chan csHolder, 100),
	}
}

func (l *FixedStateManager) Start() error {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	if l.started {
		return nil
	}
	l.closeWg.Add(1)
	go l.handleLoop()
	l.started = true
	return nil
}

func (l *FixedStateManager) Stop() error {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	if !l.started {
		return nil
	}
	close(l.csChan)
	l.closeWg.Wait()
	l.started = false
	return nil
}

func (l *FixedStateManager) Halt() error {
	return l.Stop()
}

func (l *FixedStateManager) Freeze() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.frozen = true
}

func (l *FixedStateManager) SetClusterStateHandler(handler ClusterStateHandler) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.handlers = append(l.handlers, handler)
	// Only send to this handler
	l.csChan <- csHolder{
		cs:       *l.state,
		handlers: []ClusterStateHandler{handler},
	}
}

func (l *FixedStateManager) handleLoop() {
	defer l.closeWg.Done()
	for holder := range l.csChan {
		l.deliverClusterState(holder)
	}
}

func (l *FixedStateManager) deliverClusterState(holder csHolder) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.frozen {
		return
	}
	for _, handler := range holder.handlers {
		if err := handler(holder.cs); err != nil {
			log.Errorf("failed to handle cluster state: %v", err)
		}
	}
}

func (l *FixedStateManager) MarkGroupAsValid(nodeID int, groupID int, _ int) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.state.GroupStates[groupID][nodeID].Valid = true
	l.state.Version++
	l.csChan <- csHolder{
		cs:       *l.state,
		handlers: l.handlers,
	}
	return true, nil
}

func (l *FixedStateManager) PrepareForShutdown() {
}
