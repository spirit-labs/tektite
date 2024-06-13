package repli

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/debug"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/remoting"
	"sync"
	"sync/atomic"
)

type replicator struct {
	lock                       sync.Mutex
	id                         int
	cfg                        *conf.Config
	manager                    proc.Manager
	remotingClient             *remoting.Client
	processor                  proc.Processor
	initialised                bool // true when replication queue has been processed - can process syncs
	initialisedCallback        func() error
	initialisedCallbackLock    sync.Mutex
	paused                     bool
	acquiescing                bool
	requiresReprocess          bool
	lastProcessedSequence      int
	acquiesceCh                chan struct{}
	replicatedBatches          []*proc.ProcessBatch
	valid                      atomic.Bool
	invalidReplicas            map[int]*invalidReplicaEntry
	batchSequence              int
	joinedClusterVersion       int
	leaderClusterVersion       int // cluster version of the leader, when it became leader
	replicationFailing         bool
	replicationsInProgress     int
	lastCommittedSeq           int
	lastReceivedCommittedSeq   int
	lastProcessedSeq           int
	lastProcessedSeqCheckpoint int
	lastFlushedSeq             int
	replicaSyncing             bool
	needsSync                  bool

	// These are used for sequence sanity checks
	lastReplicatedBatchSeq           int
	lastReplicatedCommitSeq          int
	lastReplicationResponseSeq       int
	lastReplicationCommitResponseSeq int

	syncingCount int64
}

var _ proc.ReplicatorFactory = NewReplicator
var _ proc.Replicator = &replicator{}

func NewReplicator(id int, cfg *conf.Config, processor proc.Processor, manager proc.Manager, joinedClusterVersion int) proc.Replicator {
	return &replicator{
		id:              id,
		cfg:             cfg,
		manager:         manager,
		remotingClient:  remoting.NewClient(cfg.ClusterTlsConfig),
		processor:       processor,
		invalidReplicas: map[int]*invalidReplicaEntry{},

		joinedClusterVersion: joinedClusterVersion,

		leaderClusterVersion:       0,
		lastProcessedSeq:           -1,
		lastProcessedSeqCheckpoint: -1,
		lastFlushedSeq:             -1,

		lastCommittedSeq:         -1,
		lastReceivedCommittedSeq: -1,
		//lastReceivedSyncSeq:              -1,
		lastReplicatedBatchSeq:           -1,
		lastReplicatedCommitSeq:          -1,
		lastReplicationResponseSeq:       -1,
		lastReplicationCommitResponseSeq: -1,
	}
}

type invalidReplicaEntry struct {
	joinedVersion int
	state         invalidReplicaState
}

type invalidReplicaState int

const (
	invalidReplicaStateRequiresSync  = 1
	invalidReplicaStateSyncing       = 2
	invalidReplicaStateSyncCompleted = 3
)

type replicationType int

const (
	replicationTypeBatch     = 1
	replicationTypeCommit    = 2
	replicationTypeSyncStart = 3
	replicationTypeSync      = 4
	replicationTypeSyncEnd   = 5
)

func (r *replicator) SetLeader(sufficientReplicas bool, clusterVersion int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	log.Debugf("%s: setting leader for replicator %d, sufficientReplicas:?%t clusterVersion:%d", r.cfg.LogScope, r.id, sufficientReplicas, clusterVersion)
	if clusterVersion != -1 {
		// A new leader
		r.leaderClusterVersion = clusterVersion
		r.paused = !sufficientReplicas
	} else {
		// Existing leader
		r.processor.SubmitAction(func() error {
			// We need to call maybeSyncInvalidReplicas here as number of replicas my have changed or valid state
			// changed, and we may be able to sync.
			if r.initialised {
				r.maybeSyncInvalidReplicas()
			}
			r.paused = !sufficientReplicas
			return nil
		})
	}
}

func (r *replicator) Stop() {
	r.remotingClient.Stop()
}

func (r *replicator) checkInProcessorLoop() {
	r.processor.CheckInProcessorLoop()
}

func (r *replicator) IsValid() bool {
	return r.valid.Load()
}

func (r *replicator) markReplicaAsValid() (bool, error) {
	return r.manager.MarkGroupAsValid(r.cfg.NodeID, r.id, r.joinedClusterVersion)
}

func (r *replicator) ReplicateBatch(processBatch *proc.ProcessBatch, completionFunc func(err error)) {
	ok := r.processor.SubmitAction(func() error {
		r.replicateBatch(processBatch, completionFunc)
		return nil
	})
	if !ok {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "unable to submit action on processor. stopped: %t leader: %t", r.processor.IsStopped(), r.processor.IsLeader()))
	}
}

func (r *replicator) replicateBatch(processBatch *proc.ProcessBatch, completionFunc func(err error)) {

	r.checkInProcessorLoop()

	if r.acquiescing {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "cluster is acquiescing"))
		// Note, we must let levelManager batches through as store flush is called during shutdown and this needs to apply
		// changes on levelManager
		return
	}

	if !r.initialised {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "replicator is not initialised"))
		return
	}

	if r.paused {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "replicator is paused"))
		return
	}

	if r.replicationFailing {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "replication failing"))
		return
	}

	if r.syncsInProgressOrRequested() {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "sync in progress"))
		return
	}

	r.performReplication(processBatch, completionFunc)
}

func (r *replicator) performReplication(processBatch *proc.ProcessBatch, completionFunc func(err error)) {
	r.checkInProcessorLoop()

	groupState, ok := r.manager.GetGroupState(r.id)
	if !ok {
		// This can occur if cluster not ready - client will retry, and it should resolve
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "no processor available"))
		return
	}

	processBatch.ReplSeq = r.batchSequence
	r.batchSequence++

	// Replication is a two phase process
	// First we replicate the batch, and if that is successful we replicate again the sequence of the latest batch
	// that has been successfully replicated to all replicas. We call this the last committed batch.
	// This two phase protocol ensures that on recovery, a new leader can be sure that all batches up to the
	// committed batch exist on all replicas. This is essential for our reliability guarantees.
	r.doReplicateBatch(processBatch, groupState, completionFunc)
}

func (r *replicator) doReplicateBatch(batch *proc.ProcessBatch, groupState clustmgr.GroupState, completionFunc func(err error)) {
	log.Debugf("node %d processor %d replicating batch %d", r.cfg.NodeID, r.id, batch.ReplSeq)

	batchSeq := batch.ReplSeq
	r.incReplicationsInProgress()

	if //goland:noinspection ALL
	debug.SanityChecks {
		if r.lastReplicatedBatchSeq != -1 && batchSeq > r.lastReplicatedBatchSeq+1 {
			panic(fmt.Sprintf("node %d processor %d replicated batch not in sequence last %d this %d",
				r.cfg.NodeID, r.id,
				r.lastReplicatedBatchSeq, batchSeq))
		}
		r.lastReplicatedBatchSeq = batchSeq
	}
	r.sendReplication(r.id, groupState.GroupNodes, r.leaderClusterVersion, batch, func(err error) {
		r.processor.SubmitAction(func() error {
			if !r.checkReplicationError(err, completionFunc) {
				return nil
			}
			if debug.SanityChecks {
				if r.lastReplicationResponseSeq != -1 && batchSeq != r.lastReplicationResponseSeq+1 {
					panic("replication response out of sequence")
				}
				r.lastReplicationResponseSeq = batchSeq
			}
			r.doReplicateCommit(batch, groupState, completionFunc)
			return nil
		})
	})
}

func (r *replicator) doReplicateCommit(batch *proc.ProcessBatch, groupState clustmgr.GroupState, completionFunc func(err error)) {
	log.Debugf("node %d processor %d replicated commit %d", r.cfg.NodeID, r.id, batch.ReplSeq)
	// Now we replicate the commit
	batchSeq := batch.ReplSeq
	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if r.lastReplicatedCommitSeq != -1 && batchSeq > r.lastReplicatedCommitSeq+1 {
			panic(fmt.Sprintf("node %d processor %d replicated commit batch not in sequence last %d this %d",
				r.cfg.NodeID, r.id, r.lastReplicatedCommitSeq, batchSeq))
		}
		r.lastReplicatedCommitSeq = batchSeq
	}
	r.sendCommit(r.id, groupState.GroupNodes, r.leaderClusterVersion, batchSeq, func(err error) {
		r.processor.SubmitAction(func() error {
			if !r.checkReplicationError(err, completionFunc) {
				return nil
			}
			if debug.SanityChecks {
				if r.lastReplicationCommitResponseSeq != -1 && batchSeq != r.lastReplicationCommitResponseSeq+1 {
					panic("replication response out of sequence")
				}
				r.lastReplicationCommitResponseSeq = batchSeq
				if len(r.replicatedBatches) > 0 &&
					r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq != batchSeq-1 {
					panic(fmt.Sprintf("node %d processor %d commit responses out of sequence this %d last %d",
						r.cfg.NodeID, r.id, batchSeq, r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq))
				}
			}
			r.appendAndProcessBatch(batch, completionFunc)
			return nil
		})
	})
}

func (r *replicator) checkNotSyncing() {
	// sanity check
	syncCount := atomic.LoadInt64(&r.syncingCount)
	if syncCount != 0 {
		for nid, entry := range r.invalidReplicas {
			log.Errorf("replicator %d for node %d is in state %d", r.id, nid, entry.state)
		}
		panic(fmt.Sprintf("cannot change replicated batches - replicator is syncing - sync count %d replications in progress %d", syncCount, r.replicationsInProgress))
	}
}

func (r *replicator) appendAndProcessBatch(batch *proc.ProcessBatch, completionFunc func(err error)) {
	// The commit is successful, now we can add the batch to the replicated batches.

	r.checkNotSyncing()

	r.replicatedBatches = append(r.replicatedBatches, batch)
	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if r.lastCommittedSeq != -1 && batch.ReplSeq != r.lastCommittedSeq+1 {
			panic(fmt.Sprintf("processor %d last committed came in out of sequece this %d last %d", r.id,
				batch.ReplSeq, r.lastCommittedSeq))
		}
	}
	r.lastCommittedSeq = batch.ReplSeq

	// Now we can finally process the batch
	r.processor.ProcessBatch(batch, func(err error) {
		if err == nil {
			// We keep track of the last processed batch sequence - this is used when flushing batches from the
			// replication queue
			r.lastProcessedSeq = batch.ReplSeq
		}
		r.decReplicationsInProgress()
		completionFunc(err)
		if r.acquiescing && r.acquiesceCh != nil && r.replicationsInProgress == 0 {
			r.acquiesceCh <- struct{}{}
			r.acquiesceCh = nil
		}
		if r.needsSync && r.replicationsInProgress == 0 {
			r.maybeSyncInvalidReplicas()
		}
	})
}

func (r *replicator) MarkProcessedSequenceCheckpoint() {
	r.processor.SubmitAction(func() error {
		r.lastProcessedSeqCheckpoint = r.lastProcessedSeq
		return nil
	})
}

func (r *replicator) ResetProcessedSequences() {
	ch := make(chan struct{}, 1)
	r.processor.SubmitAction(func() error {
		r.lastProcessedSeq = -1
		r.lastProcessedSeqCheckpoint = -1
		// And we clear the processor write cache
		r.processor.WriteCache().Clear()
		ch <- struct{}{}
		return nil
	})
	<-ch
}

func (r *replicator) FlushBatchesFromCheckpoint() {
	r.processor.SubmitAction(func() error {
		r.flushBatchesFromCheckpoint()
		return nil
	})
}

func (r *replicator) checkReplicationError(err error, completionFunc func(error)) bool {
	// When replication fails, we pause any new replications and wait until no more are in progress, then
	// we reset batch sequence to the last committed seq + 1.
	if err != nil {
		log.Debugf("node %d processor %d replication failed %v", r.cfg.NodeID, r.id, err)
	}
	if r.replicationFailing {
		r.decReplicationsInProgress()
		// We are already failing
		if r.replicationsInProgress == 0 {
			// No more in progress - reset the batch sequence
			r.batchSequence = r.lastCommittedSeq + 1
			r.lastReplicationResponseSeq = r.lastCommittedSeq
			r.replicationFailing = false
			log.Debugf("node %d processor %d replication failed %v -no more replications reset batch sequence to %d",
				r.cfg.NodeID, r.id, err, r.batchSequence)
		}
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "replication failing"))
		return false
	}
	if err != nil {
		r.decReplicationsInProgress()
		if r.replicationsInProgress > 0 {
			// There are further replications in progress
			r.replicationFailing = true
		} else {
			// No replications in progress, we can just reset the batch sequence now
			r.batchSequence = r.lastCommittedSeq + 1
			r.lastReplicationResponseSeq = r.lastCommittedSeq
			log.Debugf("node %d processor %d replication failed %v -no replications to wait for reset batch sequence to %d",
				r.cfg.NodeID, r.id, err, r.batchSequence)
		}
		completionFunc(err)
		return false
	}
	return true
}

func (r *replicator) incReplicationsInProgress() {
	r.checkInProcessorLoop()
	r.replicationsInProgress++
}

func (r *replicator) decReplicationsInProgress() {
	r.checkInProcessorLoop()
	r.replicationsInProgress--
	if r.replicationsInProgress < 0 {
		panic("replicationsInProgress < 0")
	}
}

func (r *replicator) flushBatchesFromCheckpoint() {
	r.checkInProcessorLoop()
	if r.lastProcessedSeqCheckpoint == -1 {
		return
	}
	if r.lastProcessedSeqCheckpoint == r.lastFlushedSeq {
		return
	}
	toFlush := r.lastProcessedSeqCheckpoint
	r.sendFlush(r.id, r.leaderClusterVersion, toFlush, func(err error) {
		if err != nil {
			// This is OK, can happen when nodes are unavailable. Replicating flushes is best-effort.
			// Subsequent flushes will remove previous sequences.
			log.Debugf("processor %d on node %d failed to replicate flush seq: %d err: %v",
				r.id, r.cfg.NodeID, toFlush, err)
		}
		r.processor.SubmitAction(func() error {
			log.Debugf("node %d processor %d seq %d flushBatchesFromCheckpoint", r.cfg.NodeID, r.id, toFlush)
			if r.syncsInProgressOrRequested() {
				// If we're syncing then we cannot remove flushed batches as the receiver might then get an inconsistent
				// state
				return nil
			}
			r.removeFlushedBatches(toFlush)
			r.lastFlushedSeq = toFlush
			return nil
		})
	})
}

func (r *replicator) receiveReplicatedBatch(batch *proc.ProcessBatch, clusterVersion int, joinedClusterVersion int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := r.checkErrors(clusterVersion, joinedClusterVersion); err != nil {
		return err
	}
	log.Debugf("processor %d received replicated batch %d node %d version %d",
		r.id, batch.ReplSeq, r.cfg.NodeID, batch.Version)
	return r.checkAndAppendReplicatedBatch(batch)
}

func (r *replicator) receiveReplicatedCommit(lastSuccessfulReplicationSeq int, clusterVersion int,
	joinedClusterVersion int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := r.checkErrors(clusterVersion, joinedClusterVersion); err != nil {
		return err
	}
	lrb := len(r.replicatedBatches)
	if lrb == 0 || lastSuccessfulReplicationSeq > r.replicatedBatches[lrb-1].ReplSeq {
		// Ignore - this can happen if failure occurred with commits in transit, then batches were resent
		return nil
	}
	r.lastReceivedCommittedSeq = lastSuccessfulReplicationSeq
	log.Debugf("processor %d received replicated commit %d node %d", r.id, lastSuccessfulReplicationSeq, r.cfg.NodeID)
	return nil
}

func (r *replicator) checkErrors(clusterVersion int, joinedClusterVersion int) error {
	if !r.IsValid() {
		return errors.NewTektiteErrorf(errors.Unavailable, "requires sync")
	}
	if clusterVersion < r.leaderClusterVersion {
		// This prevents the replica picking up a replication from an old leader
		return errors.NewTektiteErrorf(errors.Unavailable, "replication with older cluster version")
	}
	if joinedClusterVersion != r.joinedClusterVersion {
		// This prevents an old replica picking up a replication from the leader
		return errors.NewTektiteErrorf(errors.Unavailable, "invalid joined cluster version")
	}
	if r.processor.IsLeader() {
		return errors.NewTektiteErrorf(errors.Unavailable, "replica has been promoted to leader")
	}
	return nil
}

func (r *replicator) receiveFlushMessage(batchSeq int, clusterVersion int, joinedClusterVersion int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := r.checkErrors(clusterVersion, joinedClusterVersion); err != nil {
		return err
	}
	r.removeFlushedBatches(batchSeq)
	return nil
}

func (r *replicator) removeFlushedBatches(batchSeq int) {
	// We remove all batches with seq <= flushed sequence
	log.Debugf("node %d processor %d truncating batches up to and including seq %d size %d",
		r.cfg.NodeID, r.id, batchSeq, len(r.replicatedBatches))

	i := 0
	for i < len(r.replicatedBatches) && r.replicatedBatches[i].ReplSeq <= batchSeq {
		if log.DebugEnabled {
			log.Debugf("node %d processor %d batch %d flushed from replication queue", r.cfg.NodeID, r.id,
				r.replicatedBatches[i].ReplSeq)
		}
		// Sanity check
		if //goland:noinspection ALL
		debug.SanityChecks {
			if !r.processor.IsLeader() && r.replicatedBatches[i].ReplSeq > r.lastReceivedCommittedSeq {
				panic(fmt.Sprintf("processor %d flushed uncommitted batch seq %d last received committed %d batch seq num %d",
					r.id, batchSeq, r.lastReceivedCommittedSeq, r.replicatedBatches[i].ReplSeq))
			}
		}
		i++
	}
	if i == 0 {
		// Received a flush for a batch we don't have - this can happen when a new replica joins the cluster, and
		// initially may receive some flushes for batches that were replicated to other replica(s) just before it joined
		return
	}
	r.checkNotSyncing()
	r.replicatedBatches = r.replicatedBatches[i:]
	log.Debugf("node %d processor %d after removeFlushedBatches repl queue has size %d", r.cfg.NodeID, r.id, len(r.replicatedBatches))
}

func (r *replicator) checkAndAppendReplicatedBatch(batch *proc.ProcessBatch) error {
	lrb := len(r.replicatedBatches)
	overWritten := false
	if lrb > 0 {
		// Gaps can occur if a queued replication fails then a subsequent one succeeds - we ignore this - correct ones
		// will arrive after retry
		lastSequence := r.replicatedBatches[lrb-1].ReplSeq
		if lastSequence < batch.ReplSeq-1 {
			// Ignore
			return nil
		}
		// When replicating again after a failure, batch sequence is rewound, so a replica might see the same
		// replicated batches again, in which case we need to overwrite them.
		// This can also occur when a new leader starts replicating - it might have last committed sequence less
		// than the last committed replicated sequence on this replica, so we truncate to the leader's value. We are not losing
		// any data here as we do send ack back to the sender until all replicas successfully complete phase 2 of
		// replication
		i := lrb - 1
		for r.replicatedBatches[i].ReplSeq >= batch.ReplSeq {
			log.Debugf("node %d processor %d batch %d overwriting replicated batch", r.cfg.NodeID, r.id,
				r.replicatedBatches[i].ReplSeq)
			i--
			if i < 0 {
				break
			}
			if i < 0 {
				panic(fmt.Sprintf("node id %d processor %d discontinuity in batch sequence",
					r.cfg.NodeID, r.id))
			}
		}
		if i != lrb-1 {
			r.checkNotSyncing()
			r.replicatedBatches = r.replicatedBatches[:i+1]
			overWritten = true
		}
	}
	// final sanity check - must be contiguous
	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if len(r.replicatedBatches) > 0 {
			lbs := r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq
			if lbs != batch.ReplSeq-1 {
				panic("replicated batches out of sequence")
			}
		}
	}
	if overWritten {
		// If we overwrote the batches and last committed was >= first overwritten batch then we set last committed to
		// the current batch sequence - 1 as that batch is not committed yet
		// This can occur after a new leader fails over with last committed to one value, but one of the replicas has
		// last committed to that value + 1, then the new leader will resend a batch with a sequence number already
		// got, we need to reset the last received committed
		if r.lastReceivedCommittedSeq >= batch.ReplSeq {
			r.lastReceivedCommittedSeq = batch.ReplSeq - 1
		}
	}
	// More sanity
	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if len(r.replicatedBatches) > 0 {
			lbs := r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq
			if r.lastReceivedCommittedSeq > lbs {
				panic(fmt.Sprintf("processor %d invalid last received committed %d last batch %d", r.id,
					r.lastReceivedCommittedSeq, lbs))
			}
		}
	}
	r.checkNotSyncing()
	r.replicatedBatches = append(r.replicatedBatches, batch)
	return nil
}

func (r *replicator) GetLastCommitted(clusterVersion int, joinedVersion int) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if joinedVersion != r.joinedClusterVersion {
		// check that the message is for the correct replica
		return 0, errors.NewTektiteErrorf(errors.Unavailable, "incorrect joined version %d on GetLastCommitted expected %d",
			r.leaderClusterVersion, joinedVersion)
	}
	// We set the cluster version of the new leader here - this is checked on replication to prevent any replications
	// from previous leader being accepted
	r.leaderClusterVersion = clusterVersion
	log.Debugf("node %d processor %d getting last committed %d", r.cfg.NodeID, r.id, r.lastReceivedCommittedSeq)
	return r.lastReceivedCommittedSeq, nil
}

func (r *replicator) SetLastCommittedSequence(lastCommitted int, joinedVersion int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if joinedVersion != r.joinedClusterVersion {
		// check that the message is for the correct replica
		return errors.NewTektiteErrorf(errors.Unavailable, "incorrect joined version on SetLastCommittedSequence %d expected %d",
			r.leaderClusterVersion, joinedVersion)
	}
	log.Debugf("node %d processor %d setting last received committed to %d was %d", r.cfg.NodeID, r.id, lastCommitted, r.lastReceivedCommittedSeq)
	if //goland:noinspection GoBoolExpressions
	debug.SanityChecks {
		if len(r.replicatedBatches) > 0 &&
			lastCommitted > r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq {
			panic(fmt.Sprintf("processor %d set last committed %d but last batch is %d prev last received committed %d",
				r.id, lastCommitted,
				r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq, r.lastReceivedCommittedSeq))
		}
	}
	r.lastReceivedCommittedSeq = lastCommitted
	r.removeUncommittedBatches()
	return nil
}

func (r *replicator) removeUncommittedBatches() {
	if len(r.replicatedBatches) > 0 {
		// Remove any uncommitted batches - these are batches that we cannot guarantee have been replicated to all nodes
		// of the cluster by the previous leader, we cannot keep these as if there is failure of this leader before a
		// flush the data could be lost as it may not be on any other replica. It won't have been acked to the sender so
		// it's ok to drop it.
		i := len(r.replicatedBatches) - 1
		for r.replicatedBatches[i].ReplSeq > r.lastReceivedCommittedSeq {
			if log.DebugEnabled {
				log.Debugf("node %d processor %d batch %d removed in reprocessing as not committed - lastReceivedCommittedSeq %d",
					r.cfg.NodeID, r.id, r.replicatedBatches[i].ReplSeq, r.lastReceivedCommittedSeq)
			}
			i--
			if i < 0 {
				break
			}
		}
		r.checkNotSyncing()
		r.replicatedBatches = r.replicatedBatches[:i+1]
		log.Debugf("node %d processor %d after removeUncommittedBatches repl queue has size %d", r.cfg.NodeID, r.id, len(r.replicatedBatches))

		if //goland:noinspection GoBoolExpressions
		debug.SanityChecks {
			if len(r.replicatedBatches) > 0 && r.lastReceivedCommittedSeq != r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq {
				// sanity
				panic(fmt.Sprintf("processor %d inconsistent committed batches last received committed %d, last seq %d",
					r.id, r.lastReceivedCommittedSeq, r.replicatedBatches[len(r.replicatedBatches)-1].ReplSeq))
			}
		}
	}
}

func (r *replicator) GetReplicatedBatchCount() int {
	if r.processor.IsLeader() {
		ch := make(chan int, 1)
		r.processor.SubmitAction(func() error {
			ch <- len(r.replicatedBatches)
			return nil
		})
		return <-ch
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	return len(r.replicatedBatches)
}

func (r *replicator) SetJoinedClusterVersion(joinedVersion int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.joinedClusterVersion = joinedVersion
}

func (r *replicator) Acquiesce() {
	ch := make(chan bool, 1)
	shutdownCh := make(chan struct{}, 1)
	// Must be executed on event loop to ensure no replications get accepted after this
	r.processor.SubmitAction(func() error {
		if r.replicationsInProgress == 0 {
			ch <- true
		} else {
			r.acquiescing = true
			r.acquiesceCh = shutdownCh
			ch <- false
		}
		return nil
	})
	complete := <-ch
	if !complete {
		// Need to wait for in progress replications to complete
		<-shutdownCh
	}
}

func (r *replicator) TruncateProcessedBatches(version int) error {
	seq, err := r.processor.LoadLastProcessedReplBatchSeq(version)
	log.Debugf("processor %d truncating to sequence %d", r.processor.ID(), seq)
	if err != nil {
		return err
	}
	if seq != -1 {
		r.processor.SubmitAction(func() error {
			r.removeFlushedBatches(int(seq))
			return nil
		})
	}
	return nil
}

func (r *replicator) Resume() {
	r.processor.SubmitAction(func() error {
		r.acquiescing = false
		return nil
	})
}
