package repli

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"math"
	"sync"
	"time"
)

const minLastCommittedRetryDelay = 100 * time.Millisecond
const reprocessRetryDelay = 100 * time.Millisecond

func (r *replicator) SetInitialisedCallback(callback func() error) {
	r.initialisedCallbackLock.Lock()
	defer r.initialisedCallbackLock.Unlock()
	r.initialisedCallback = callback
}

func (r *replicator) InitialiseLeader(forceReprocess bool) error {
	// Need replicator lock here for correct happens-before semantics
	r.lock.Lock()
	defer r.lock.Unlock()

	r.checkInProcessorLoop()

	if r.initialised {
		return nil
	}

	for {
		err := r.findAndSetMinLastCommittedSequence()
		if err == nil {
			break
		}
		err = remoting.MaybeConvertError(err)
		if !common.IsUnavailableError(err) {
			return err
		}
		log.Warnf("node %d processor %d failed to get last min committed %v", r.cfg.NodeID, r.id, err)
		if r.processor.IsStopped() {
			return nil
		}
		time.Sleep(minLastCommittedRetryDelay)
	}

	isLevelMgr := r.cfg.LevelManagerEnabled && r.processor.ID() == r.cfg.ProcessorCount
	if isLevelMgr {
		log.Debugf("level manager processor has %d batches to replay", len(r.replicatedBatches))
	}

	if len(r.replicatedBatches) > 0 {

		r.removeUncommittedBatches()

		// Check the queue integrity and calculate the batch sequence number
		lastSeq := -1
		for i, batch := range r.replicatedBatches {
			if lastSeq != -1 && batch.ReplSeq != lastSeq+1 {
				panic(fmt.Sprintf("processor %d replication queue batch sequence out of seq %d last %d index %d",
					r.id, batch.ReplSeq, lastSeq, i))
			}
			lastSeq = batch.ReplSeq
		}

		if r.requiresReprocess || forceReprocess {
			// forceReprocess = true for level manager processor, otherwise r.requiresReprocess is set from failureHandler
			seq := r.lastProcessedSeq
			if forceReprocess {
				// for level manager we always reprocess all the queue
				seq = -1
			}
			if err := r.reprocessQueue(seq); err != nil {
				return err
			}
			r.requiresReprocess = false
			r.lastProcessedSequence = -1
		} else {
			log.Debugf("not reprocessing because requires reprocess: %t", r.requiresReprocess)
		}
	}

	r.lastCommittedSeq = r.lastReceivedCommittedSeq
	log.Debugf("replicator %d initialised, setting lastCommittedSeq to %d", r.id, r.lastReceivedCommittedSeq)
	r.batchSequence = r.lastReceivedCommittedSeq + 1
	r.lastReceivedCommittedSeq = -1
	r.initialised = true

	r.maybeSyncInvalidReplicas()

	r.initialisedCallbackLock.Lock()
	defer r.initialisedCallbackLock.Unlock()
	if r.initialisedCallback != nil {
		for {
			if err := r.initialisedCallback(); err != nil {
				if !common.IsUnavailableError(err) {
					return err
				}
				time.Sleep(reprocessRetryDelay)
			} else {
				break
			}
		}
	}

	log.Debugf("node %d processor %d fully initialised set last committed to %d batch sequence to %d last received committed to %d",
		r.cfg.NodeID, r.id, r.lastCommittedSeq, r.batchSequence, r.lastReceivedCommittedSeq)

	return nil
}

func (r *replicator) IsInitialised() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.initialised
}

func (r *replicator) MaybeReprocessQueue(lastFlushedVersion int) error {
	if r.isLevelManagerProcessor() {
		// level manager queue is always reprocessed on InitialiseLeader
		return nil
	}
	// for level manager processor, we always reprocess all batches as the level manager does its own dedup
	// detection
	seq, err := r.processor.LoadLastProcessedReplBatchSeq(lastFlushedVersion)
	if err != nil {
		log.Errorf("failed to load last processed repl seq: %v", err)
		return err
	}
	lastProcessedSeq := int(seq)

	log.Infof("in maybe reprocess", lastProcessedSeq)

	r.processor.SubmitAction(func() error {
		if r.initialised {
			// Already initialised so reprocess queue now
			return r.reprocessQueue(lastProcessedSeq)
		}
		// Newly failed over processor might not be initialised yet, so we flag it to reprocess when it does get initialised
		r.requiresReprocess = true
		r.lastProcessedSequence = lastProcessedSeq
		return nil
	})
	return nil
}

func (r *replicator) isLevelManagerProcessor() bool {
	return r.cfg.LevelManagerEnabled && r.id == r.cfg.ProcessorCount
}

func (r *replicator) reprocessQueue(lastProcessedSequence int) error {
	r.checkInProcessorLoop()

	for _, batch := range r.replicatedBatches {
		if batch.ReplSeq <= lastProcessedSequence {
			// The batch is from a version <= last flushed version - we ignore it
			continue
		}
		// We ignore barriers - we will soon skip to the next version and that version will break through
		// any in-progress barriers. We do replicate barriers though, it's easier to reason about batch sequence
		// that way
		if !batch.Barrier {
			log.Debugf("node %d processor %d batch %d version %d, forwarding proc %d - reprocessing batch",
				r.cfg.NodeID, r.id, batch.ReplSeq, batch.Version, batch.ForwardingProcessorID)
			for {
				ch := make(chan error, 1)
				r.processor.ReprocessBatch(batch, func(err error) {
					ch <- err
				})
				if err := <-ch; err != nil {
					if !common.IsUnavailableError(err) {
						log.Warnf("node %d processor %d reprocessing of batch completed with error %v", r.cfg.NodeID,
							r.id, err)
						return err
					}
					// retry - we can get unavailable here if reprocessing level manager batches and level manager hasn't
					// loaded yet
					time.Sleep(reprocessRetryDelay)
					log.Debugf("node %d processor %d, unable to reprocess batch got err %v will retry",
						r.cfg.NodeID, r.id, err)
				} else {
					log.Debugf("node %d processor %d seq %d reprocessed batch ok", r.cfg.NodeID, r.id, batch.ReplSeq)
					break
				}
			}
		}
	}
	return nil
}

func (r *replicator) findAndSetMinLastCommittedSequence() error {
	groupState, ok := r.manager.GetGroupState(r.id)
	if !ok {
		return errors.New("no processor in map")
	}

	type validReplica struct {
		nid           int
		joinedVersion int
	}

	var replicas []validReplica
	for _, gn := range groupState.GroupNodes {
		if gn.Valid && !gn.Leader {
			if gn.NodeID == r.cfg.NodeID {
				// Another node has become leader before this one has finished initialising as leader
				return errors.Errorf("failed to promote replica as leader ont node %d - it is not leader any more", r.cfg.NodeID)
			}
			replicas = append(replicas, validReplica{
				nid:           gn.NodeID,
				joinedVersion: gn.JoinedVersion,
			})
		}
	}
	if len(replicas) == 0 {
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(replicas))
	var theErr error
	var resultLock sync.Mutex
	lowestValue := math.MaxInt64
	for _, vr := range replicas {
		serverAddress := r.cfg.ClusterAddresses[vr.nid]
		msg := &clustermsgs.LastCommittedRequest{
			ProcessorId:    uint64(r.id),
			ClusterVersion: uint64(r.leaderClusterVersion),
			JoinedVersion:  uint64(vr.joinedVersion),
		}
		r.remotingClient.SendRPCAsync(func(r remoting.ClusterMessage, err error) {
			resultLock.Lock()
			defer resultLock.Unlock()
			if err != nil {
				theErr = err
			} else {
				resp := r.(*clustermsgs.LastCommittedResponse)
				if int(resp.LastCommitted) < lowestValue {
					lowestValue = int(resp.LastCommitted)
				}
			}
			wg.Done()
		}, msg, serverAddress)
	}
	wg.Wait()
	resultLock.Lock()
	if theErr != nil {
		resultLock.Unlock()
		return theErr
	}

	if r.lastReceivedCommittedSeq < lowestValue {
		lowestValue = r.lastReceivedCommittedSeq
	}

	r.lastReceivedCommittedSeq = lowestValue
	resultLock.Unlock()
	theErr = nil
	wg = sync.WaitGroup{}
	wg.Add(len(replicas))
	for _, vr := range replicas {
		serverAddress := r.cfg.ClusterAddresses[vr.nid]
		msg := &clustermsgs.SetLastCommittedMessage{
			ProcessorId:   uint64(r.id),
			LastCommitted: int64(lowestValue),
			JoinedVersion: uint64(vr.joinedVersion),
		}
		r.remotingClient.SendRPCAsync(func(r remoting.ClusterMessage, err error) {
			resultLock.Lock()
			defer resultLock.Unlock()
			theErr = err
			wg.Done()
		}, msg, serverAddress)
	}
	wg.Wait()
	resultLock.Lock()
	defer resultLock.Unlock()
	return theErr
}
