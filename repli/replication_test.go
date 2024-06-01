package repli

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestReplicationSimple(t *testing.T) {

	procMgrs, batchHandlers, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	// Deploy a single processor/replica

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	numBatches := 1000
	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(numBatches, func(err error) {
		ch <- err
	})

	// leader is on node 1
	procMgr := procMgrs[1]
	processor, ok := procMgr.GetProcessor(0)
	require.True(t, ok)

	var sentBatches []*proc.ProcessBatch

	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i, 0)
		sentBatches = append(sentBatches, pb)
		repli := processor.GetReplicator()

		if i == 0 {
			for {
				// Send the first batch with retry as this might fail as replicas still syncing
				ch := make(chan error, 1)
				repli.ReplicateBatch(pb, func(err error) {
					if err == nil {
						ch <- nil
						return
					}
					if common.IsUnavailableError(err) {
						ch <- err
						return
					}
					panic(err)
				})
				err := <-ch
				if err == nil {
					break
				}
				time.Sleep(1 * time.Millisecond)
			}
			cf.CountDown(nil)
		} else {
			repli.ReplicateBatch(pb, cf.CountDown)
		}
	}

	err := <-ch
	require.NoError(t, err)

	handler := batchHandlers[1]

	require.Equal(t, len(sentBatches), len(handler.getReceivedBatches()))
	require.Equal(t, sentBatches, handler.getReceivedBatches())
	for i, batch := range handler.getReceivedBatches() {
		id := batch.EvBatch.GetIntColumn(0).Get(0)
		require.Equal(t, i, int(id))
	}

	replLeader := processor.GetReplicator().(*replicator)

	procRepl1, ok := procMgrs[0].GetProcessor(0)
	require.True(t, ok)
	procRepl2, ok := procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl1 := procRepl1.GetReplicator().(*replicator)
	repl2 := procRepl2.GetReplicator().(*replicator)

	// Make sure leader and replica have all the same batches
	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))

	checkReplicatedBatches(t, replLeader, 0, numBatches, schema)
	checkReplicatedBatches(t, repl1, 0, numBatches, schema)
	checkReplicatedBatches(t, repl2, 0, numBatches, schema)
	checkLastCommitted(t, replLeader, repl1, repl2)
}

func TestInsufficientReplicas(t *testing.T) {

	procMgrs, _, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	// Deploy a single processor on one node

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	processor, ok := procMgrs[1].GetProcessor(0)
	require.True(t, ok)

	pb := createProcessBatch(0, 0)
	repli := processor.GetReplicator()

	err := replicateBatchSync(pb, repli)
	require.Error(t, err)
	// Insufficient replicas, so replicator should be paused
	require.Equal(t, "replicator is paused", err.Error())

	// Deploy a replica
	cs = clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 2},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)
	// Should now sync and the replication should complete
	replicateBatchWithRetry(t, pb, repli)

	mgs := stateMgrs[0].getMarkedGroups()
	require.Equal(t, 1, len(mgs))
	require.Equal(t, 0, mgs[0].nodeID)
	require.Equal(t, 0, mgs[0].groupID)
	require.Equal(t, 2, mgs[0].joinedVersion)

	// Now remove a replica
	cs = clustmgr.ClusterState{
		Version: 15,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)
	// Should fail to replicate
	err = replicateBatchSync(pb, repli)
	require.Error(t, err)
	// Insufficient replicas again, so replicator should be paused
	require.Equal(t, "replicator is paused", err.Error())
}

func TestSyncBatches(t *testing.T) {

	procMgrs, _, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	// Deploy a processor with 2 replicas

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	processor, ok := procMgrs[1].GetProcessor(0)
	require.True(t, ok)
	repli := processor.GetReplicator()

	// Replicate a bunch of batches
	numBatches := 1000

	var sentBatches []*proc.ProcessBatch
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i, 0)
		sentBatches = append(sentBatches, pb)
		replicateBatchWithRetry(t, pb, repli)
	}

	mgs := stateMgrs[0].getMarkedGroups()
	require.Equal(t, 1, len(mgs))
	require.Equal(t, 0, mgs[0].nodeID)
	require.Equal(t, 0, mgs[0].groupID)
	require.Equal(t, 1, mgs[0].joinedVersion)

	replLeader := repli.(*replicator)

	procRepl0, ok := procMgrs[0].GetProcessor(0)
	require.True(t, ok)
	repl0 := procRepl0.GetReplicator().(*replicator)

	// Make sure leader and replica have all the same batches
	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))
	checkReplicatedBatches(t, replLeader, 0, numBatches, schema)
	checkReplicatedBatches(t, repl0, 0, numBatches, schema)
	checkLastCommitted(t, replLeader, repl0)

	// Now add another replica - it should sync the state from the leader
	cs = clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 2},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	// replicate another load of batches
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i+numBatches, 0)
		sentBatches = append(sentBatches, pb)
		replicateBatchWithRetry(t, pb, repli)
	}

	mgs = stateMgrs[2].getMarkedGroups()
	require.Equal(t, 1, len(mgs))
	require.Equal(t, 2, mgs[0].nodeID)
	require.Equal(t, 0, mgs[0].groupID)
	require.Equal(t, 2, mgs[0].joinedVersion)

	// All the replicas should have all the batches, including the newly synced one
	procRepl2, ok := procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl2 := procRepl2.GetReplicator().(*replicator)

	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))
	checkReplicatedBatches(t, replLeader, 0, numBatches*2, schema)
	checkReplicatedBatches(t, repl0, 0, numBatches*2, schema)
	checkReplicatedBatches(t, repl2, 0, numBatches*2, schema)
	checkLastCommitted(t, replLeader, repl0, repl2)
}

func TestBounceReplica(t *testing.T) {

	procMgrs, _, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	processor, ok := procMgrs[1].GetProcessor(0)
	require.True(t, ok)
	repli := processor.GetReplicator()

	// Replicate a bunch of batches
	numBatches := 1000

	var sentBatches []*proc.ProcessBatch
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i, 0)
		sentBatches = append(sentBatches, pb)
		replicateBatchWithRetry(t, pb, repli)
	}

	mgs := stateMgrs[0].getMarkedGroups()
	require.Equal(t, 1, len(mgs))
	require.Equal(t, 0, mgs[0].nodeID)
	require.Equal(t, 0, mgs[0].groupID)
	require.Equal(t, 1, mgs[0].joinedVersion)
	mgs = stateMgrs[2].getMarkedGroups()
	require.Equal(t, 1, len(mgs))
	require.Equal(t, 2, mgs[0].nodeID)
	require.Equal(t, 0, mgs[0].groupID)
	require.Equal(t, 1, mgs[0].joinedVersion)

	replLeader := repli.(*replicator)

	procRepl0, ok := procMgrs[0].GetProcessor(0)
	require.True(t, ok)
	repl0 := procRepl0.GetReplicator().(*replicator)

	procRepl2, ok := procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl2 := procRepl2.GetReplicator().(*replicator)

	// Make sure leader and replica have all the same batches
	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))
	checkReplicatedBatches(t, replLeader, 0, numBatches, schema)
	checkReplicatedBatches(t, repl0, 0, numBatches, schema)
	checkReplicatedBatches(t, repl2, 0, numBatches, schema)
	checkLastCommitted(t, replLeader, repl0, repl2)
	// delete replica on node 2
	cs = clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
			},
		},
	}
	err := stateMgrs[2].sendClusterState(cs)
	require.NoError(t, err)
	_, ok = procMgrs[2].GetProcessor(0)
	require.False(t, ok)
	// And recreate it
	cs = clustmgr.ClusterState{
		Version: 15,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 2},
			},
		},
	}
	err = stateMgrs[2].sendClusterState(cs)
	require.NoError(t, err)

	// At this point, leader cluster state still thinks node 2 replica is valid, but its just been deployed and
	// is internally invalid

	pb := createProcessBatch(1000, 0)
	err = replicateBatchSync(pb, repli)
	require.Error(t, err)
	require.Equal(t, "requires sync", err.Error())

	// Now a little time later a new cluster state will be deployed on all nodes with the joined version incremented
	// for the group on node 2, this should cause the replica to be synced as the leader will recognise the joined
	// version has changed
	cs = clustmgr.ClusterState{
		Version: 16,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 2},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	// replicate another load of batches
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i+numBatches, 0)
		sentBatches = append(sentBatches, pb)
		replicateBatchWithRetry(t, pb, repli)
	}

	// make sure 2 has synced
	mgs = stateMgrs[2].getMarkedGroups()
	require.Equal(t, 2, len(mgs))
	require.Equal(t, 2, mgs[1].nodeID)
	require.Equal(t, 0, mgs[1].groupID)
	require.Equal(t, 2, mgs[1].joinedVersion)

	// All the replicas should have all the batches, including the newly synced one

	procRepl2, ok = procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl2 = procRepl2.GetReplicator().(*replicator)

	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))
	checkReplicatedBatches(t, replLeader, 0, numBatches*2, schema)
	checkReplicatedBatches(t, repl0, 0, numBatches*2, schema)
	checkReplicatedBatches(t, repl2, 0, numBatches*2, schema)
	checkLastCommitted(t, replLeader, repl0, repl2)
}

func TestLeaderFailureReplicaHasLowerLastCommitted(t *testing.T) {
	testLeaderFailure(t, 998, 997)
}

func TestLeaderFailureLeaderHasLowerLastCommitted(t *testing.T) {
	testLeaderFailure(t, 997, 998)
}

func testLeaderFailure(t *testing.T, leaderLastCommitted int, replicaLastCommitted int) {

	procMgrs, handlers, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	cs := clustmgr.ClusterState{
		Version: 13,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 1},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	processor, ok := procMgrs[1].GetProcessor(0)
	require.True(t, ok)
	repli := processor.GetReplicator()

	// Replicate a bunch of batches
	numBatches := 1000

	var sentBatches []*proc.ProcessBatch
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i, 0)
		sentBatches = append(sentBatches, pb)
		replicateBatchWithRetry(t, pb, repli)
	}

	replLeader := repli.(*replicator)
	procRepl0, ok := procMgrs[0].GetProcessor(0)
	require.True(t, ok)
	repl0 := procRepl0.GetReplicator().(*replicator)

	procRepl2, ok := procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl2 := procRepl2.GetReplicator().(*replicator)

	// Make sure leader and replica have all the same batches
	require.Equal(t, len(sentBatches), len(replLeader.replicatedBatches))
	checkReplicatedBatches(t, replLeader, 0, numBatches, schema)
	checkReplicatedBatches(t, repl0, 0, numBatches, schema)
	checkReplicatedBatches(t, repl2, 0, numBatches, schema)
	checkLastCommitted(t, replLeader, repl0, repl2)

	// Change the last committed slightly on nodes 0 and 2
	repl0.lastReceivedCommittedSeq = leaderLastCommitted // 0 will be the new leader
	repl2.lastReceivedCommittedSeq = replicaLastCommitted

	// Now fail leader on node 1, and promote 0 to leader
	cs = clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}
	err := stateMgrs[0].sendClusterState(cs)
	require.NoError(t, err)
	err = stateMgrs[2].sendClusterState(cs)
	require.NoError(t, err)

	// new leader initialisation doesn't occur until next version
	// barriers are sent - the processor needs version before batches can be reprocessed
	pb := createProcessBatch(1000, 0)
	err = replicateBatchSync(pb, repl0)
	require.Error(t, err)
	require.Equal(t, "replicator is not initialised", err.Error())

	// So we increment version on manager which will cause barriers to be injected
	procMgrs[0].HandleVersionBroadcast(101, 0, 0)

	// leader initialisation should occur and last committed should be set to the minimum
	minLastCommitted := leaderLastCommitted
	if replicaLastCommitted < minLastCommitted {
		minLastCommitted = replicaLastCommitted
	}

	// Wait until batches are received
	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		reprocessed := handlers[0].getReceivedBatches()
		return minLastCommitted+1 == len(reprocessed), nil
	}, 5*time.Second, 1*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	repl0.lock.Lock()
	require.Equal(t, minLastCommitted, repl0.lastCommittedSeq)
	repl0.lock.Unlock()
	//replicated batches should be trimmed to 997 on leader
	checkReplicatedBatches(t, repl0, 0, minLastCommitted+1, schema)

	repl2.lock.Lock()
	require.Equal(t, minLastCommitted, repl2.lastReceivedCommittedSeq)
	repl2.lock.Unlock()
	//replicated batches should be trimmed to 997 on replica
	checkReplicatedBatches(t, repl2, 0, minLastCommitted+1, schema)

	// batches should be reprocessed
	reprocessed := handlers[0].getReceivedBatches()
	require.Equal(t, minLastCommitted+1, len(reprocessed))
	for i, batch := range reprocessed {
		id := batch.EvBatch.GetIntColumn(0).Get(0)
		require.Equal(t, i, int(id))
	}
}

func TestFlushBatches(t *testing.T) {

	procMgrs, _, stateMgrs, tearDown := setupCluster(t)
	defer tearDown(t)

	// Deploy a processor with 2 replicas

	cs := clustmgr.ClusterState{
		Version: 14,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: false, JoinedVersion: 2},
			},
		},
	}
	deployClusterState(t, cs, stateMgrs)

	processor, ok := procMgrs[1].GetProcessor(0)
	require.True(t, ok)
	repli := processor.GetReplicator()

	numBatches := 1000
	for i := 0; i < numBatches; i++ {
		pb := createProcessBatch(i, 0)
		replicateBatchWithRetry(t, pb, repli)
	}

	replLeader := repli.(*replicator)

	lastProcessed, lastProcessedCheckpoint, lastFlushed, batches := getFlushVars(replLeader)
	require.Equal(t, numBatches-1, lastProcessed)
	require.Equal(t, -1, lastProcessedCheckpoint)
	require.Equal(t, -1, lastFlushed)
	require.Equal(t, numBatches, len(batches))

	replLeader.MarkProcessedSequenceCheckpoint()
	lastProcessed, lastProcessedCheckpoint, lastFlushed, _ = getFlushVars(replLeader)
	require.Equal(t, numBatches-1, lastProcessed)
	require.Equal(t, numBatches-1, lastProcessedCheckpoint)
	require.Equal(t, -1, lastFlushed)

	// Now we're going to fudge the checkpoint so not all batches get flushed
	checkpoint := rand.Intn(numBatches)
	ch := make(chan struct{}, 1)
	replLeader.processor.SubmitAction(func() error {
		replLeader.lastProcessedSeqCheckpoint = checkpoint
		ch <- struct{}{}
		return nil
	})
	<-ch

	replLeader.FlushBatchesFromCheckpoint()
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		_, _, lastFlushed, _ = getFlushVars(replLeader)
		return lastFlushed == checkpoint, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	lastProcessed, lastProcessedCheckpoint, lastFlushed, _ = getFlushVars(replLeader)
	require.Equal(t, numBatches-1, lastProcessed)
	require.Equal(t, checkpoint, lastProcessedCheckpoint)
	require.Equal(t, checkpoint, lastFlushed)
	checkReplicatedBatches(t, replLeader, checkpoint+1, numBatches, schema)

	procRepl0, ok := procMgrs[0].GetProcessor(0)
	require.True(t, ok)
	repl0 := procRepl0.GetReplicator().(*replicator)
	procRepl2, ok := procMgrs[2].GetProcessor(0)
	require.True(t, ok)
	repl2 := procRepl2.GetReplicator().(*replicator)

	checkReplicatedBatches(t, repl0, checkpoint+1, numBatches, schema)
	checkReplicatedBatches(t, repl2, checkpoint+1, numBatches, schema)
}

func getFlushVars(repl *replicator) (int, int, int, []*proc.ProcessBatch) {
	type flushVars struct {
		lastProcessed, lastProcessedCheckpoint, lastFlushed int
		batches                                             []*proc.ProcessBatch
	}
	ch := make(chan flushVars, 1)
	repl.processor.SubmitAction(func() error {
		ch <- flushVars{
			lastProcessed:           repl.lastProcessedSeq,
			lastProcessedCheckpoint: repl.lastProcessedSeqCheckpoint,
			lastFlushed:             repl.lastFlushedSeq,
			batches:                 repl.replicatedBatches,
		}
		return nil
	})
	vars := <-ch
	return vars.lastProcessed, vars.lastProcessedCheckpoint, vars.lastFlushed, vars.batches
}

func replicateBatchWithRetry(t *testing.T, pb *proc.ProcessBatch, repli proc.Replicator) {
	start := time.Now()
	for {
		err := replicateBatchSync(pb, repli)
		if err == nil {
			return
		}
		if !common.IsUnavailableError(err) {
			require.NoError(t, err)
			return
		}
		log.Warnf("failed to replicate %v", err)
		time.Sleep(1 * time.Millisecond)
		if time.Now().Sub(start) > 5*time.Second {
			require.Fail(t, "timed out waiting to replicate batch")
		}
	}
}

func replicateBatchSync(pb *proc.ProcessBatch, repli proc.Replicator) error {
	ch := make(chan error, 1)
	repli.ReplicateBatch(pb, func(err error) {
		ch <- err
	})
	return <-ch
}

var schema = evbatch.NewEventSchema([]string{"id"}, []types.ColumnType{types.ColumnTypeInt})

func createProcessBatch(id int, processorID int) *proc.ProcessBatch {
	colBuilders := evbatch.CreateColBuilders(schema.ColumnTypes())
	colBuilders[0].(*evbatch.IntColBuilder).Append(int64(id))
	evBatch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
	return proc.NewProcessBatch(processorID, evBatch, 1000, 0, -1)
}

func deployClusterState(t *testing.T, cs clustmgr.ClusterState, stateMgrs []*testClustStateMgr) {
	for _, stateMgr := range stateMgrs {
		err := stateMgr.sendClusterState(cs)
		require.NoError(t, err)
	}
}

func checkReplicatedBatches(t *testing.T, repl *replicator, startSeq int, endSeq int, schema *evbatch.EventSchema) {
	repl.lock.Lock()
	defer repl.lock.Unlock()
	batches := repl.replicatedBatches
	require.Equal(t, endSeq-startSeq, len(batches))
	for i := startSeq; i < endSeq; i++ {
		batch := batches[i-startSeq]
		require.Equal(t, i, batch.ReplSeq)
		batch.CheckDeserializeEvBatch(schema)
		id := batch.EvBatch.GetIntColumn(0).Get(0)
		require.Equal(t, i, int(id))
	}
}

func checkLastCommitted(t *testing.T, replLeader *replicator, replicas ...*replicator) {
	replLeader.lock.Lock()
	defer replLeader.lock.Unlock()
	lc := replLeader.lastCommittedSeq
	for _, replica := range replicas {
		replica.lock.Lock()
		//goland:noinspection ALL
		defer replica.lock.Unlock()
		require.Equal(t, lc, replica.lastReceivedCommittedSeq)
	}
}

func setupCluster(t *testing.T) ([]proc.Manager, []*testBatchHandler, []*testClustStateMgr, func(t *testing.T)) {
	return setupClusterWithNumNodes(t, 3)
}

func setupClusterWithNumNodes(t *testing.T, numNodes int) ([]proc.Manager, []*testBatchHandler, []*testClustStateMgr, func(t *testing.T)) {

	var remotingAddresses []string
	for i := 0; i < numNodes; i++ {
		remotingAddresses = append(remotingAddresses, fmt.Sprintf("localhost:%d", testutils.PortProvider.GetPort(t)))
	}

	var stores []*store.Store
	var stateMgrs []*testClustStateMgr
	var procMgrs []proc.Manager
	var remotingServers []remoting.Server
	var batchHandlers []*testBatchHandler

	for i := 0; i < numNodes; i++ {

		stateMgr := &testClustStateMgr{}
		stateMgrs = append(stateMgrs, stateMgr)

		st := store.TestStore()
		err := st.Start()
		require.NoError(t, err)
		stores = append(stores, st)

		cfg := &conf.Config{}
		cfg.ApplyDefaults()
		cfg.NodeID = &i
		cfg.ClusterAddresses = remotingAddresses
		cfg.BatchFlushCheckInterval = types.AddressOf(1 * time.Hour) // Effectively turn off periodic flushing in the tests

		remotingServer := remoting.NewServer(remotingAddresses[i], conf.TLSConfig{})
		err = remotingServer.Start()
		require.NoError(t, err)
		remotingServers = append(remotingServers, remotingServer)

		batchHandler := &testBatchHandler{}
		batchHandlers = append(batchHandlers, batchHandler)
		mgr := proc.NewProcessorManagerWithFailure(stateMgr, &testReceiverInfoProvider{}, st, cfg,
			NewReplicator, func(processorID int) proc.BatchHandler {
				return batchHandler
			}, nil, &testIngestNotifier{}, true)
		mgr.SetVersionManagerClient(&testVmgrClient{})
		teeHandler := &remoting.TeeBlockingClusterMessageHandler{}
		mgr.SetClusterMessageHandlers(remotingServer, teeHandler)
		remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageVersionsMessage, teeHandler)
		stateMgr.SetClusterStateHandler(mgr.HandleClusterState)

		SetClusterMessageHandlers(remotingServer, mgr)

		err = mgr.Start()
		require.NoError(t, err)
		procMgrs = append(procMgrs, mgr)
	}
	tearDown := func(t *testing.T) {
		for _, st := range stores {
			err := st.Stop()
			require.NoError(t, err)
		}
		for _, rs := range remotingServers {
			err := rs.Stop()
			require.NoError(t, err)
		}
		for _, mgr := range procMgrs {
			err := mgr.Stop()
			require.NoError(t, err)
		}
	}
	return procMgrs, batchHandlers, stateMgrs, tearDown
}

type testVmgrClient struct {
}

func (t *testVmgrClient) FailureDetected(int, int) error {
	return nil
}

func (t *testVmgrClient) GetLastFailureFlushedVersion(int) (int, error) {
	return -1, nil
}

func (t *testVmgrClient) FailureComplete(int, int) error {
	return nil
}

func (t *testVmgrClient) IsFailureComplete(int) (bool, error) {
	return true, nil
}

func (t *testVmgrClient) VersionFlushed(int, int, int, int) error {
	return nil
}

func (t *testVmgrClient) GetVersions() (int, int, int, error) {
	return 100, 0, 0, nil
}

func (t *testVmgrClient) VersionComplete(int, int, int, bool, func(error)) {
}

func (t *testVmgrClient) Start() error {
	return nil
}

func (t *testVmgrClient) Stop() error {
	return nil
}

type testReceiverInfoProvider struct {
}

func (t testReceiverInfoProvider) GetTerminalReceiverCount() int {
	return 0
}

func (t testReceiverInfoProvider) GetForwardingProcessorCount(int) (int, bool) {
	return 1, true
}

func (t testReceiverInfoProvider) GetInjectableReceivers(int) []int {
	return []int{1000}
}

func (t testReceiverInfoProvider) GetRequiredCompletions() int {
	return 1
}

type testBatchHandler struct {
	lock            sync.Mutex
	receivedBatches []*proc.ProcessBatch
}

func (t *testBatchHandler) HandleProcessBatch(_ proc.Processor, processBatch *proc.ProcessBatch,
	_ bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !processBatch.Barrier {
		t.receivedBatches = append(t.receivedBatches, processBatch)
	}
	return true, mem.NewBatch(), nil, nil
}

func (t *testBatchHandler) getReceivedBatches() []*proc.ProcessBatch {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.receivedBatches
}

type testClustStateMgr struct {
	lock             sync.Mutex
	handler          func(state clustmgr.ClusterState) error
	markedGroupInfos []markedGroupInfo
}

type markedGroupInfo struct {
	nodeID        int
	groupID       int
	joinedVersion int
}

func (t *testClustStateMgr) MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.markedGroupInfos = append(t.markedGroupInfos, markedGroupInfo{
		nodeID:        nodeID,
		groupID:       groupID,
		joinedVersion: joinedVersion,
	})
	return true, nil
}

func (t *testClustStateMgr) getMarkedGroups() []markedGroupInfo {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.markedGroupInfos
}

func (t *testClustStateMgr) sendClusterState(state clustmgr.ClusterState) error {
	return t.handler(state)
}

func (t *testClustStateMgr) SetClusterStateHandler(handler clustmgr.ClusterStateHandler) {
	t.handler = handler
}

func (t *testClustStateMgr) Start() error {
	return nil
}

func (t *testClustStateMgr) Stop() error {
	return nil
}

func (t *testClustStateMgr) Halt() error {
	return nil
}

type testIngestNotifier struct {
}

func (t *testIngestNotifier) StopIngest() error {
	return nil
}

func (t *testIngestNotifier) StartIngest(int) error {
	return nil
}
