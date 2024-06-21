// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proc

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"reflect"
	"sync"
	"time"
)

const failureRetryInterval = 500 * time.Millisecond

type failureHandler struct {
	procMgr                *ProcessorManager
	lastFailedClusterState *clustmgr.ClusterState
	failureCh              chan int
	stopWg                 sync.WaitGroup
	liveProcessorCount     int
	failingClusterVersion  int
	failureFlushedVersion  int
	lock                   sync.Mutex
	stopped                bool
	state                  failureState
}

type failureState int

const (
	failureStateIdle               = failureState(1)
	failureStateNotifyingFailure   = failureState(2)
	failureStateGettingLastFlushed = failureState(3)
	failureStateNotifyingComplete  = failureState(4)
	failureStateAwaitingComplete   = failureState(5)
)

func newFailureHandler(procMgr *ProcessorManager) *failureHandler {
	fh := &failureHandler{
		procMgr:               procMgr,
		failureCh:             make(chan int, 10),
		failingClusterVersion: -1,
		state:                 failureStateIdle,
	}
	return fh
}

func (f *failureHandler) start() {
	f.stopWg.Add(1)
	go f.runLoop()
}

func (f *failureHandler) stop() {
	close(f.failureCh)
	f.stopWg.Wait()
	f.lock.Lock()
	defer f.lock.Unlock()
	f.stopped = true
}

func (f *failureHandler) runLoop() {
	defer func() {
		f.stopWg.Done()
	}()
	for {
		select {
		case clusterVersion, ok := <-f.failureCh:
			if !ok {
				return
			}
			if err := f.handleClusterVersion(clusterVersion); err != nil {
				log.Errorf("failed to handle failure: %v", err)
			}
		case <-time.After(failureRetryInterval):
			if err := f.execFailureStage(); err != nil {
				log.Errorf("failed to process failure: %v", err)
			}
		}
	}
}

func (f *failureHandler) handleClusterVersion(clusterVersion int) error {
	if clusterVersion < 0 {
		panic(fmt.Sprintf("invalid cluster version %d", clusterVersion))
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.state != failureStateIdle {
		// failure already running - cancel failure
		f.cancelFailure()
	}
	return f.initiateFailure(clusterVersion)
}

func (f *failureHandler) execFailureStage() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	var err error
	switch f.state {
	case failureStateIdle:
		return nil
	case failureStateNotifyingFailure:
		err = f.sendFailureDetected()
	case failureStateGettingLastFlushed:
		err = f.getLastFailureFlushedVersion()
	case failureStateNotifyingComplete:
		err = f.sendFailureComplete()
	case failureStateAwaitingComplete:
		err = f.sendIsComplete()
	default:
		panic("unknown failure state")
	}
	if err != nil {
		var terr errors.TektiteError
		if errors.As(err, &terr) {
			if terr.Code == errors.FailureCancelled {
				// failure cancelled as version manager notified us we sent wrong cluster version
				f.cancelFailure()
				return nil
			} else if terr.Code == errors.Unavailable {
				// temp unavailability - we will retry after delay
				return nil
			}
		}
	}
	return err
}

func (f *failureHandler) initiateFailure(clusterVersion int) error {

	if clusterVersion == f.failingClusterVersion {
		panic(fmt.Sprintf("already failing at cluster version %d", clusterVersion))
	}

	f.failingClusterVersion = clusterVersion

	log.Debugf("node %d failureHandler version %d 1", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Stop flush removing any entries from replication queues
	f.procMgr.disableReplBatchFlush(true) // don't lock as called from processor handler - lock already held

	// Note, we do not disable table flushing on the store - if we did then the memtable queue could become full,
	// blocking writes from processors and stopping versions from completing which could cause the failure process to
	// hang

	// Stop replicators accepting any new traffic and wait for in-progress replications to complete.
	f.procMgr.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if f.procMgr.isLevelManagerProcessor(proc.id) {
			// level manager - ignore
			// if we acquiesce the level manager here too then registering of dead version range won't work
			return true
		}
		if proc.IsLeader() {
			repli := proc.GetReplicator()
			if repli != nil {
				repli.Acquiesce()
			}
		}
		return true
	})

	log.Debugf("node %d failureHandler version %d 2", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Tell Stream manager to stop ingest while we are recovering
	if err := f.procMgr.ingestNotifier.StopIngest(); err != nil {
		return err
	}

	log.Debugf("node %d failureHandler version %d 3", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Tell vmgr failure is detected - once all processors have told it they have detected failure at the same
	// cluster version it will wait for all in-progress versions to complete then register dead versions with the
	// level manager
	f.liveProcessorCount = f.procMgr.liveProcessorCount()

	f.state = failureStateNotifyingFailure

	return f.sendFailureDetected()
}

func (f *failureHandler) sendFailureDetected() error {
	err := f.procMgr.vMgrClient.FailureDetected(f.liveProcessorCount, f.failingClusterVersion)
	log.Debugf("node: %d failureHandler sending failureDetected, clusterVersion %d - returned %v", f.procMgr.cfg.NodeID,
		f.failingClusterVersion, err)
	if err == nil {
		f.state = failureStateGettingLastFlushed
	}
	return err
}

func (f *failureHandler) getLastFailureFlushedVersion() error {
	log.Debugf("node: %d failureHandler calling GetLastFailureFlushedVersion, clusterVersion %d", f.procMgr.cfg.NodeID,
		f.failingClusterVersion)
	lastFlushedVersion, err := f.procMgr.vMgrClient.GetLastFailureFlushedVersion(f.failingClusterVersion)
	if err != nil {
		return err
	}
	// -2 return represents still waiting for versions to complete for required cluster version
	if lastFlushedVersion == -2 || err != nil {
		log.Debugf("node: %d calling GetLastFailureFlushedVersion for clusterVersion %d returned last flushed %d err %v",
			f.procMgr.cfg.NodeID, f.failingClusterVersion, lastFlushedVersion, err)
		// not all processors have called in to version manager or version manager is unavailable - we will retry
		return nil
	}
	// we got a valid version
	f.failureFlushedVersion = lastFlushedVersion

	// Now we can acquiesce level manager processor
	if err := f.procMgr.AcquiesceLevelManagerProcessor(); err != nil {
		return err
	}

	log.Debugf("node %d failureHandler version %d 5", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Reset the last processed checkpoints - otherwise if a version flush occurs replication queues could be
	// truncated losing batches that we need to replay
	// We also clear processor write caches
	f.procMgr.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if proc.IsLeader() {
			repli := proc.GetReplicator()
			if repli != nil {
				repli.ResetProcessedSequences()
			}
		}
		return true
	})

	log.Debugf("node %d failureHandler version %d 6", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Ingest has stopped and there is no processing in progress - we can now clear the local store.
	// Note: it is critical that local store clear is done on all nodes *before* processing is enabled on any nodes
	// that is why we wait for failure to be complete on all nodes before proceeding to the final stage. Otherwise,
	// processing could be re-enabled on one node and messages forwarded to another node which hadn't cleared local
	// store yet and dead state could be loaded from the local store (e.g. for an aggregation).
	f.procMgr.store.ClearUnflushedData()

	f.state = failureStateNotifyingComplete

	return f.sendFailureComplete()
}

func (f *failureHandler) sendFailureComplete() error {
	err := f.procMgr.vMgrClient.FailureComplete(f.liveProcessorCount, f.failingClusterVersion)
	if err != nil {
		return err
	}
	f.state = failureStateAwaitingComplete
	return f.sendIsComplete()
}

func (f *failureHandler) sendIsComplete() error {
	complete, err := f.procMgr.vMgrClient.IsFailureComplete(f.failingClusterVersion)
	if err != nil {
		return err
	}
	if !complete {
		return nil
	}

	log.Debugf("node %d failureHandler rolling back to last flushed %d", f.procMgr.cfg.NodeID, f.failureFlushedVersion)

	log.Debugf("node %d failureHandler version %d 9", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Tell replicators to replay their queues
	f.procMgr.processors.Range(func(key, value any) bool {
		proc := value.(*processor)
		if proc.IsLeader() {
			repli := proc.GetReplicator()
			if repli != nil {
				if err = repli.MaybeReprocessQueue(f.failureFlushedVersion); err != nil {
					return false
				}
				repli.Resume()
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	log.Debugf("node %d failureHandler version %d 10", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Re-enable repl batch flushing
	f.procMgr.disableReplBatchFlush(false)

	log.Debugf("node %d failureHandler version %d 11", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	// Tell stream manager that kafka_in operators can now reset to offset as
	// of LastFlushedVersion and restart. Producer endpoints can reset their dedup to LastFlushedVersion.
	if err := f.procMgr.ingestNotifier.StartIngest(f.failureFlushedVersion); err != nil {
		return err
	}

	log.Debugf("node %d failureHandler version %d 12", f.procMgr.cfg.NodeID, f.failingClusterVersion)

	f.state = failureStateIdle
	f.failingClusterVersion = -1
	f.liveProcessorCount = -1
	f.failureFlushedVersion = -1
	return nil
}

func (f *failureHandler) cancelFailure() {
	f.failingClusterVersion = -1
	f.procMgr.disableReplBatchFlush(false)
	f.state = failureStateIdle
}

func (f *failureHandler) maybeRunFailure(cs *clustmgr.ClusterState) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.stopped {
		return nil
	}

	if len(cs.GroupStates) < len(f.procMgr.lastClusterState.GroupStates) {
		// Groups lost
		log.Warnf("groups lost from cluster state")
		return nil
	}

	nodesAdded := len(cs.GroupStates) > 0 && len(f.procMgr.lastClusterState.GroupStates) > 0 &&
		len(cs.GroupStates[0]) > len(f.procMgr.lastClusterState.GroupStates[0])
	if nodesAdded {
		return nil
	}
	nodeFailures := f.hasNodeFailure(cs)
	if nodeFailures || f.failingClusterVersion != -1 || reflect.DeepEqual(f.lastFailedClusterState, cs) {
		// If any node failures, or we're already failing, or this cluster state is same as last cluster state failure
		// was attempted with then we start failure.
		// The same cluster state, with a different version can arrive, and we must make sure we reprocess failure with
		// the higher cluster version.
		f.lastFailedClusterState = cs
		f.failureCh <- cs.Version
	}
	return nil
}

func (f *failureHandler) hasNodeFailure(cs *clustmgr.ClusterState) bool {
	nodeFailure := false
	for i, groupState := range cs.GroupStates {
		// if the leader node changed then a node failure must have occurred (we don't support re-balancing yet)
		// but it's not sufficient to just check if the leader node changed as the node could have been quickly bounced
		// in which case the leader node wouldn't have changed, so we need to check if the joined version changed in that
		// case
		currLeaderJoined := -1
		currLeaderNode := -1
		for _, groupNode := range groupState {
			if groupNode.Leader {
				currLeaderJoined = groupNode.JoinedVersion
				currLeaderNode = groupNode.NodeID
			}
		}
		if currLeaderJoined == -1 {
			panic("no leader")
		}
		prevState := f.procMgr.lastClusterState.GroupStates[i]
		prevLeaderJoined := -1
		prevLeaderNode := -1
		for _, groupNode := range prevState {
			if groupNode.Leader {
				prevLeaderJoined = groupNode.JoinedVersion
				prevLeaderNode = groupNode.NodeID
			}
		}
		if prevLeaderJoined == -1 {
			panic("no prev leader")
		}
		if currLeaderNode != prevLeaderNode || currLeaderJoined != prevLeaderJoined {
			nodeFailure = true
			break
		}
	}
	return nodeFailure
}
