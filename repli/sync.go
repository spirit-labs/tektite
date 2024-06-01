package repli

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/debug"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"sync/atomic"
)

const syncBatchesMaxChunkSize = 1

func (r *replicator) maybeSyncInvalidReplicas() {
	r.checkInProcessorLoop()

	if !r.initialised {
		log.Debugf("replica %d is not initialised so maybeSyncInvalidReplicas doing nothing", r.id)
		// can only sync if initialised
		return
	}

	groupState, ok := r.manager.GetGroupState(r.id)
	if !ok {
		return
	}
	r.checkSyncsRequired(groupState.GroupNodes)

	if r.syncsRequired() {
		if r.replicationsInProgress == 0 {
			log.Debugf("syncing nodes for replicator %d", r.id)
			r.syncNodes()
		} else {
			log.Debugf("nodes to sync but not syncing as replications in progress- for replicator %d", r.id)
			// We set the needsSync flag, so we don't call into this method after every replication from
			// replicator.appendAndProcessBatch as this has some performance overhead
			r.needsSync = true
		}
	}
}

func (r *replicator) CheckSync() {
	r.processor.SubmitAction(func() error {
		for nid, entry := range r.invalidReplicas {
			log.Debugf("replicator %d for node %d is in state %d", r.id, nid, entry.state)
		}
		r.maybeSyncInvalidReplicas()
		return nil
	})
}

func (r *replicator) syncsRequired() bool {
	for _, entry := range r.invalidReplicas {
		if entry.state == invalidReplicaStateRequiresSync {
			return true
		}
	}
	return false
}

func (r *replicator) syncsInProgressOrRequested() bool {
	for _, entry := range r.invalidReplicas {
		if entry.state != invalidReplicaStateSyncCompleted {
			return true
		}
	}
	return false
}

func (r *replicator) checkSyncsRequired(groupNodes []clustmgr.GroupNode) {
	r.checkInProcessorLoop()

	if len(r.invalidReplicas) > 0 {
		for _, groupNode := range groupNodes {
			// Check whether any currently invalid nodes are now valid - this means the sync completed ok and the
			// replica was marked as valid
			if groupNode.Valid {
				delete(r.invalidReplicas, groupNode.NodeID)
			}
		}
		// Check whether any invalid nodes syncing or waiting to sync have now disappeared from the cluster,
		// if so, we remove them from the invalid set, otherwise the processor would remain in "sync in progress"
		// state until that replica was started, and we might already have enough replicas.
		nids := make(map[int]struct{}, len(groupNodes))
		for _, gn := range groupNodes {
			nids[gn.NodeID] = struct{}{}
		}
		for invNid := range r.invalidReplicas {
			_, ok := nids[invNid]
			if !ok {
				delete(r.invalidReplicas, invNid)
			}
		}
	}

	for _, groupNode := range groupNodes {
		if groupNode.NodeID == *r.cfg.NodeID {
			// Ignore current node
			continue
		}
		if !groupNode.Valid {
			invalidEntry, ok := r.invalidReplicas[groupNode.NodeID]
			//log.Debugf("processor %d repl %d is not valid this joined version %d gn joined version %d", r.id,
			//	groupNode.NodeID, invalidEntry.joinedVersion, groupNode.JoinedVersion)
			if !ok || invalidEntry.joinedVersion != groupNode.JoinedVersion {
				// We check the joined version - this will change if the replica stops then rejoins the group
				// the catches the case where we never see a transition from invalid->valid->invalid and makes
				// sure the replica gets resynced in this case
				r.invalidReplicas[groupNode.NodeID] = &invalidReplicaEntry{
					joinedVersion: groupNode.JoinedVersion,
					state:         invalidReplicaStateRequiresSync,
				}
			}
		}
	}
}

func (r *replicator) syncNodes() {
	r.checkInProcessorLoop()
	if r.processor.IsStopped() {
		log.Warnf("processor %d not syncing as stopped", r.id)
		return
	}
	for nid := range r.invalidReplicas {
		theNid := nid
		entry := r.invalidReplicas[nid]
		joinedVersion := entry.joinedVersion
		if entry.state == invalidReplicaStateRequiresSync {
			atomic.AddInt64(&r.syncingCount, 1)
			entry.state = invalidReplicaStateSyncing
			r.sendSyncState(theNid, joinedVersion, func(err error) {
				// Needs to be run on the event loop for correct happens-before semantics!
				r.processor.SubmitAction(func() error {
					atomic.AddInt64(&r.syncingCount, -1)
					if err != nil {
						log.Debugf("node %d processor %d sync failed %v will retry", r.cfg.NodeID, r.id, err)
						// One of the syncs failed - set it to require sync again, so it will be retried -
						// this will be called once for each failed sync (not for each batch)
						entry.state = invalidReplicaStateRequiresSync
						// Trigger another sync
						r.maybeSyncInvalidReplicas()
						return nil
					}
					entry.state = invalidReplicaStateSyncCompleted
					log.Debugf("sync completed for replicator %d", r.id)
					return nil
				})
			})
		}
	}
	r.needsSync = false
}

func (r *replicator) isSyncing() bool {
	return atomic.LoadInt64(&r.syncingCount) != 0
}

func (r *replicator) sendSyncState(nodeID int, joinedClusterVersion int, completionFunc func(error)) {
	if !r.processor.IsLeader() {
		panic(fmt.Sprintf("node %d processor %d not a leader in sendReplicatedBatches", r.cfg.NodeID, r.id))
	}

	if len(r.replicatedBatches) > 0 {
		log.Debugf("processor %d node %d sending replicated sync state to node %d num batches %d first seq %d last seq %d",
			r.id, r.cfg.NodeID, nodeID, len(r.replicatedBatches), r.replicatedBatches[0].ReplSeq, r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq)

		// check contiguous sanity check
		for i := 1; i < len(r.replicatedBatches); i++ {
			if r.replicatedBatches[i].ReplSeq != r.replicatedBatches[i-1].ReplSeq+1 {
				panic("replicated batches not in sequence")
			}
		}
	} else {
		log.Debugf("processor %d node %d sending replicated sync state to node %d num batches %d",
			r.id, r.cfg.NodeID, nodeID, len(r.replicatedBatches))
	}
	r.checkInProcessorLoop()

	if r.replicationsInProgress != 0 {
		panic("sync with replications in progress")
	}

	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if len(r.replicatedBatches) > 0 {
			lastSeq := r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq
			if lastSeq != r.lastCommittedSeq {
				panic(fmt.Sprintf("processor %d invalid uncommitted batches at start of sync last seq %d last committed %d repls in progress %d lastReceivedCommitted %d",
					r.id, lastSeq, r.lastCommittedSeq, r.replicationsInProgress, r.lastReceivedCommittedSeq))
			}
		}
	}

	// Start batch - empty marker batch
	log.Debugf("processor %d sending sync start to node %d", r.id, nodeID)

	startBatch := &proc.ProcessBatch{
		EvBatchBytes: []byte{},
	}

	r.sendSyncBatchToNode(r.id, nodeID, r.leaderClusterVersion, joinedClusterVersion, replicationTypeSyncStart,
		0, startBatch, func(err error) {
			if err != nil {
				completionFunc(err)
				return
			}
			log.Debugf("processor %d sync start sent to node %d ok", r.id, nodeID)

			r.processor.SubmitAction(func() error {
				// The start batch succeeded
				r.sendSyncBatches(nodeID, joinedClusterVersion, completionFunc)
				return nil
			})
		})
}

func (r *replicator) sendSyncBatches(nodeID int, joinedClusterVersion int, completionFunc func(error)) {
	// Now we send the actual batches
	log.Debugf("processor %d sendSyncBatches to node %d", r.id, nodeID)
	lrb := len(r.replicatedBatches)
	r.sendSyncChunk(nodeID, joinedClusterVersion, 0, lrb, func(err error) {
		if err != nil {
			completionFunc(err)
			return
		}
		r.processor.SubmitAction(func() error {
			log.Debugf("processor %d sent sync batches ok. now sending end batch", r.id)
			r.sendSyncEndBatch(nodeID, joinedClusterVersion, completionFunc)
			return nil
		})
	})
}

func (r *replicator) sendSyncEndBatch(nodeID int, joinedClusterVersion int, completionFunc func(error)) {
	r.checkInProcessorLoop()
	// now we send the end batch - special marker batch
	if len(r.replicatedBatches) > 0 && r.lastCommittedSeq != r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq {
		panic(fmt.Sprintf("processor %d wrong last committed %d last seq %d", r.id, r.lastCommittedSeq,
			r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq))
	}
	// We send the current batch sequence in the end batch - this is used to make sure the replica has the correct
	// batch sequence in the case there was no state to sync
	log.Debugf("processor %d sending sync end to node %d last committed is %d", r.id, nodeID, r.lastCommittedSeq)
	endBatch := &proc.ProcessBatch{EvBatchBytes: []byte{}}

	endBatch.ReplSeq = r.lastCommittedSeq
	r.sendSyncBatchToNode(r.id, nodeID, r.leaderClusterVersion, joinedClusterVersion, replicationTypeSyncEnd,
		1+len(r.replicatedBatches), endBatch, completionFunc)
}

func (r *replicator) sendSyncChunk(nodeID int, joinedClusterVersion int, start int, expectedLrb int, completionFunc func(error)) {
	r.checkInProcessorLoop()
	lrb := len(r.replicatedBatches)
	if lrb != expectedLrb {
		panic("replication queue has changed while sync in progress")
	}

	log.Debugf("processor %d sendSyncChunk to node %d start %d lrb %d", r.id, nodeID, start, lrb)
	if start == expectedLrb {
		// we're done
		completionFunc(nil)
		return
	}
	// We send these in chunks of syncBatchesMaxChunkSize to avoid overwhelming the connection, which can cause
	// deadlock with two nodes syncing with each other at same time
	end := start + syncBatchesMaxChunkSize
	if end > lrb {
		end = lrb
	}
	cf := common.NewCountDownFuture(end-start, func(err error) {
		if err != nil {
			completionFunc(err)
			return
		}
		r.processor.SubmitAction(func() error {
			// Send the next chunk
			r.sendSyncChunk(nodeID, joinedClusterVersion, end, expectedLrb, completionFunc)
			return nil
		})
	})
	seq := start + 1
	for i := start; i < end; i++ {
		replBatch := r.replicatedBatches[i]
		log.Debugf("processor %d sending sync batch to node %d with repl seq %d", r.id, nodeID, seq)
		r.sendSyncBatchToNode(r.id, nodeID, r.leaderClusterVersion, joinedClusterVersion, replicationTypeSync,
			seq, replBatch, cf.CountDown)
		seq++
	}
}

func (r *replicator) receiveReplicatedSync(batch *proc.ProcessBatch, clusterVersion int, joinedClusterVersion int, replicationType int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if joinedClusterVersion != r.joinedClusterVersion {
		r.replicaSyncing = false
		return errors.NewTektiteErrorf(errors.Unavailable, "invalid joined cluster version")
	}
	if r.processor.IsLeader() {
		r.replicaSyncing = false
		log.Warnf("node %d processor %d batch %d rejecting replicate sync as replica promoted to leader",
			r.cfg.NodeID, r.id, batch.ReplSeq)
		return errors.NewTektiteErrorf(errors.Unavailable, "replica has been promoted to leader")
	}
	if r.IsValid() {
		log.Warnf("node %d processor %d batch %d replica already synced", r.cfg.NodeID, r.id, batch.ReplSeq)
		r.replicaSyncing = false
		return errors.NewTektiteErrorf(errors.Unavailable, "replica already synced")
	}
	if r.leaderClusterVersion == -1 {
		// Set cluster version
		r.leaderClusterVersion = clusterVersion
	}

	// Sync batches
	if replicationType == replicationTypeSyncStart {
		// reset - if a previous sync failed because of leader failure, another leader may try to sync
		// in this case we need to reset the replicated state
		if clusterVersion < r.leaderClusterVersion {
			return errors.NewTektiteErrorf(errors.Unavailable, "sync start with old cluster version")
		}
		r.checkNotSyncing()
		r.replicatedBatches = nil
		r.replicaSyncing = true
		r.lastReceivedCommittedSeq = -1
		r.leaderClusterVersion = clusterVersion
		if batch.ReplSeq != 0 {
			panic("invalid sync start seq")
		}
		log.Debugf("replicator %d received sync start", r.id)
		return nil
	}
	if r.leaderClusterVersion != -1 && clusterVersion != r.leaderClusterVersion {
		log.Warnf("node %d processor %d batch %d rejecting replicate sync as cluster version incorrect %d %d ",
			r.cfg.NodeID, r.id,
			batch.ReplSeq, clusterVersion, r.leaderClusterVersion)
		r.replicaSyncing = false
		return errors.NewTektiteErrorf(errors.Unavailable, "replication with incorrect cluster version")
	}
	if !r.replicaSyncing {
		// Sync batches from a sync which already returned an error
		return errors.NewTektiteErrorf(errors.Unavailable, "not syncing")
	}
	if replicationType == replicationTypeSync {
		if len(r.replicatedBatches) > 0 {
			lbs := r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq
			if batch.ReplSeq != lbs+1 {
				// We can get batches out of sequence, e.g. if batch sent, then received but sender gets error and retries, or
				// if sent on different connections and earlier batch "overtakes" later one. In this case we return an
				// error, and it will be resent
				return errors.NewTektiteErrorf(errors.Unavailable, "processor %d sync batch out of sequence %d last batch %d", r.id,
					batch.ReplSeq, lbs)
			}
		}
		r.checkNotSyncing()
		r.replicatedBatches = append(r.replicatedBatches, batch)
		log.Debugf("replicator %d received sync batch with seq %d", r.id, batch.ReplSeq)
		return nil
	}
	if replicationType != replicationTypeSyncEnd {
		panic(fmt.Sprintf("invalid replication type %d", replicationType))
	}
	// The end batch contains the last committed sequence - we need to send this even if there are no batches
	// in the replication queue so the replicas has the correct value
	// also we send the last completed version here
	r.lastReceivedCommittedSeq = batch.ReplSeq
	if len(r.replicatedBatches) > 0 {
		if r.lastReceivedCommittedSeq != r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq {
			// sanity
			panic("inconsistent committed batches at sync end")
		}
	}

	if len(r.replicatedBatches) > 0 {
		if r.lastReceivedCommittedSeq != r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq {
			panic("wrong lastReceivedCommittedSeq")
		}
	}

	ok, err := r.markReplicaAsValid()
	if err != nil {
		// Pass this back to the leader, it will pass it back to the sender who will retry if it's
		// an unavailable error
		if common.IsUnavailableError(err) {
			return err
		}
		log.Errorf("node %d processor %d failed to mark replica as valid", r.cfg.NodeID, r.id)
		return nil
	}
	if !ok {
		// Implies the joined version was wrong.
		// An old replica can sometimes attempt to mark as valid after a new replica has started
		// in this case an error will occur
		log.Warnf("node %d processor %d failed to mark replica as valid, incorrect joined version", r.cfg.NodeID,
			r.id)
		return nil
	}
	log.Debugf("node %d processor %d synced ok", r.cfg.NodeID, r.id)

	r.valid.Set(true)
	r.replicaSyncing = false

	return nil
}
