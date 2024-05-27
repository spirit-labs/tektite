package clustmgr

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
)

func NewLocalStateManager(numGroups int) *LocalStateManager {
	var groupStates [][]GroupNode
	for i := 0; i < numGroups; i++ {
		groupStates = append(groupStates, []GroupNode{{
			NodeID:        0,
			Leader:        true,
			Valid:         false,
			JoinedVersion: 0,
		}})
	}
	cs := &ClusterState{
		Version:     1,
		GroupStates: groupStates,
	}
	return &LocalStateManager{
		state:  cs,
		csChan: make(chan ClusterState, 100),
	}
}

type LocalStateManager struct {
	handler ClusterStateHandler
	state   *ClusterState
	lock    sync.Mutex
	csChan  chan ClusterState
	closeWg sync.WaitGroup
	frozen  bool
	stopped atomic.Bool
}

func (l *LocalStateManager) Start() error {
	l.closeWg.Add(1)
	go l.handleLoop()
	return nil
}

func (l *LocalStateManager) Stop() error {
	if l.stopped.Load() {
		return nil
	}
	close(l.csChan)
	l.closeWg.Wait()
	l.stopped.Store(true)
	return nil
}

func (l *LocalStateManager) Halt() error {
	return l.Stop()
}

func (l *LocalStateManager) Freeze() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.frozen = true
}

func (l *LocalStateManager) SetClusterStateHandler(handler ClusterStateHandler) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.handler = handler
	l.sendClusterState()
}

func (l *LocalStateManager) sendClusterState() {
	// Make a copy
	cs := *l.state
	l.csChan <- cs
}

func (l *LocalStateManager) handleLoop() {
	defer l.closeWg.Done()
	for cs := range l.csChan {
		l.deliverClusterState(cs)
	}
}

func (l *LocalStateManager) deliverClusterState(cs ClusterState) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.handler != nil && !l.frozen {
		if err := l.handler(cs); err != nil {
			log.Errorf("failed to handle cluster state: %v", err)
		}
	}
}

func (l *LocalStateManager) MarkGroupAsValid(_ int, groupID int, _ int) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.state.GroupStates[groupID][0].Valid = true
	l.state.Version++
	l.sendClusterState()
	return true, nil
}

func (l *LocalStateManager) PrepareForShutdown() {
}
