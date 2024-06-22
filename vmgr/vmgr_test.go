package vmgr

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/sequence"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompleteVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// Consume initial version sent when vmgr activates
	msg := <-vHandler.ch
	require.Equal(t, int64(0), msg.CurrentVersion)
	require.Equal(t, int64(-1), msg.CompletedVersion)

	versions := 10
	completions := 10
	currVersion := mustGetCurrentVersion(vmgr)
	require.Equal(t, 0, currVersion)
	for i := 0; i < versions; i++ {
		for j := 0; j < completions; j++ {
			err := vmgr.VersionComplete(currVersion, completions, 0, false)
			require.NoError(t, err)
		}
		nextCurrVersion := mustGetCurrentVersion(vmgr)
		require.Equal(t, currVersion+1, nextCurrVersion)

		msg := <-vHandler.ch
		require.Equal(t, currVersion, int(msg.CompletedVersion))
		require.Equal(t, currVersion+1, int(msg.GetCurrentVersion()))

		currVersion = nextCurrVersion
	}
}

func TestIgnoreOlderVersions(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// Consume initial version sent when vmgr activates
	msg := <-vHandler.ch
	require.Equal(t, int64(0), msg.CurrentVersion)
	require.Equal(t, int64(-1), msg.CompletedVersion)

	// complete version 0
	completions := 10
	for j := 0; j < completions; j++ {
		err := vmgr.VersionComplete(0, completions, 0, false)
		require.NoError(t, err)
	}
	msg = <-vHandler.ch
	require.Equal(t, 0, int(msg.CompletedVersion))
	require.Equal(t, 1, int(msg.GetCurrentVersion()))

	// Now try and complete with version that's already completed - should be ignored
	for j := 0; j < completions; j++ {
		err := vmgr.VersionComplete(0, completions, 0, false)
		require.NoError(t, err)
	}

	// Now complete version 1
	for j := 0; j < completions; j++ {
		err := vmgr.VersionComplete(1, completions, 0, false)
		require.NoError(t, err)
	}
	msg = <-vHandler.ch
	require.Equal(t, 1, int(msg.CompletedVersion))
	require.Equal(t, 2, int(msg.GetCurrentVersion()))
}

func TestPeriodicBroadcast(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionCompletedBroadcastInterval = 10 * time.Millisecond
	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	for i := 0; i < 5; i++ {
		msg1 := <-vHandler.ch
		require.Equal(t, -1, int(msg1.CompletedVersion))
		require.Equal(t, 0, int(msg1.GetCurrentVersion()))
	}

	for i := 0; i < 5; i++ {
		err := vmgr.VersionComplete(i, 1, 0, false)
		require.NoError(t, err)

		msg := <-vHandler.ch
		require.Equal(t, i, int(msg.CompletedVersion))
		require.Equal(t, i+1, int(msg.GetCurrentVersion()))
	}

	// And wait for periodic broadcasts
	for i := 0; i < 5; i++ {
		msg := <-vHandler.ch
		require.Equal(t, 4, int(msg.CompletedVersion))
		require.Equal(t, 5, int(msg.GetCurrentVersion()))
	}

}

func TestResumeCurrentVersionAtLastSequence(t *testing.T) {
	seqMgr := sequence.NewInMemSequenceManager()

	lastSeq := 1000
	for i := 0; i < lastSeq; i++ {
		_, err := seqMgr.GetNextID(currVersionSequenceName, 10)
		require.NoError(t, err)
	}

	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionCompletedBroadcastInterval = 10 * time.Millisecond
	vmgr, remotingServer, _ := setupWithSeqMgr(t, seqMgr, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	require.Equal(t, lastSeq, mustGetCurrentVersion(vmgr))
}

func TestDoomInProgressVersions(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionCompletedBroadcastInterval = 10 * time.Millisecond
	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// Complete some versions
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			err := vmgr.VersionComplete(i, 10, 0, false)
			require.NoError(t, err)
		}
		require.Equal(t, i+1, mustGetCurrentVersion(vmgr))
	}
	for msg := range vHandler.ch {
		if msg.CompletedVersion == 4 {
			break
		}
	}

	// partially complete next version
	for j := 0; j < 5; j++ {
		err := vmgr.VersionComplete(5, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 5, mustGetCurrentVersion(vmgr))

	// doom this version and next
	err := vmgr.VersionComplete(6, -1, -1, true)
	require.NoError(t, err)

	// curr version should move to 7
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// Wait for this to be broadcast
	for msg := range vHandler.ch {
		if msg.CurrentVersion == 7 {
			require.Equal(t, 4, int(msg.CompletedVersion))
			break
		}
	}

	// Send the rest of the completions for 5
	for j := 5; j < 10; j++ {
		err := vmgr.VersionComplete(5, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// Consume some periodic broadcasts - they should all be stuck on the previous version
	for i := 0; i < 10; i++ {
		msg := <-vHandler.ch
		require.Equal(t, 4, int(msg.CompletedVersion))
		require.Equal(t, 7, int(msg.CurrentVersion))
	}

	// Complete version 6
	for j := 0; j < 10; j++ {
		err := vmgr.VersionComplete(6, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// Should still be on same version
	for i := 0; i < 10; i++ {
		msg := <-vHandler.ch
		require.Equal(t, 4, int(msg.CompletedVersion))
		require.Equal(t, 7, int(msg.CurrentVersion))
	}

	// Now complete version 7
	for j := 0; j < 10; j++ {
		err := vmgr.VersionComplete(7, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 8, mustGetCurrentVersion(vmgr))

	// Wait until we move to next version
	for msg := range vHandler.ch {
		if msg.CurrentVersion == 8 {
			require.Equal(t, 7, int(msg.CompletedVersion))
			if msg.CompletedVersion == 7 {
				break
			}
		}
	}
}

func TestDoomInProgressDoomAgainLowerVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionCompletedBroadcastInterval = 10 * time.Millisecond
	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// Complete some versions
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			err := vmgr.VersionComplete(i, 10, 0, false)
			require.NoError(t, err)
		}
		require.Equal(t, i+1, mustGetCurrentVersion(vmgr))
	}
	for msg := range vHandler.ch {
		if msg.CompletedVersion == 4 {
			break
		}
	}

	// partially complete next version
	for j := 0; j < 5; j++ {
		err := vmgr.VersionComplete(5, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 5, mustGetCurrentVersion(vmgr))

	// doom this version and next
	err := vmgr.VersionComplete(6, -1, -1, true)
	require.NoError(t, err)

	// curr version should move to 7
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// now try and doom again, but with a lower version
	err = vmgr.VersionComplete(5, -1, -1, true)
	require.NoError(t, err)

	// this should be ignored
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	for msg := range vHandler.ch {
		if msg.CurrentVersion == 7 {
			require.Equal(t, 4, int(msg.CompletedVersion))
			break
		}
	}

	// Send the rest of the completions for 5
	for j := 5; j < 10; j++ {
		err := vmgr.VersionComplete(5, 10, 0, false)
		require.NoError(t, err)
	}
	// Complete 6
	for j := 0; j < 10; j++ {
		err := vmgr.VersionComplete(6, 10, 0, false)
		require.NoError(t, err)
	}

	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// Consume some periodic broadcasts - they should all be stuck on the previous version
	for i := 0; i < 10; i++ {
		msg := <-vHandler.ch
		require.Equal(t, 4, int(msg.CompletedVersion))
		require.Equal(t, 7, int(msg.CurrentVersion))
	}

	// Now complete version 7
	for j := 0; j < 10; j++ {
		err := vmgr.VersionComplete(7, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 8, mustGetCurrentVersion(vmgr))

	// Wait until we move to next version
	for msg := range vHandler.ch {
		if msg.CurrentVersion == 8 {
			require.Equal(t, 7, int(msg.CompletedVersion))
			if msg.CompletedVersion == 7 {
				break
			}
		}
	}
}

func TestDoomInProgressDoomAgainHigherVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionCompletedBroadcastInterval = 10 * time.Millisecond
	vmgr, remotingServer, vHandler := setup(t, cfg)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// Complete some versions
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			err := vmgr.VersionComplete(i, 10, 0, false)
			require.NoError(t, err)
		}
		require.Equal(t, i+1, mustGetCurrentVersion(vmgr))
	}
	for msg := range vHandler.ch {
		if msg.CompletedVersion == 4 {
			break
		}
	}

	// partially complete next version
	for j := 0; j < 5; j++ {
		err := vmgr.VersionComplete(5, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 5, mustGetCurrentVersion(vmgr))

	// doom this version and next

	err := vmgr.VersionComplete(6, -1, -1, true)
	require.NoError(t, err)

	// curr version should move to 7
	require.Equal(t, 7, mustGetCurrentVersion(vmgr))

	// now doom up to higher version
	err = vmgr.VersionComplete(8, -1, -1, true)
	require.NoError(t, err)

	require.Equal(t, 9, mustGetCurrentVersion(vmgr))

	for msg := range vHandler.ch {
		if msg.CurrentVersion == 9 {
			require.Equal(t, 4, int(msg.CompletedVersion))
			break
		}
	}

	// Complete up to and including version 8
	for i := 5; i < 9; i++ {
		for j := 0; j < 10; j++ {
			err := vmgr.VersionComplete(i, 10, 0, false)
			require.NoError(t, err)
		}
	}

	require.Equal(t, 9, mustGetCurrentVersion(vmgr))

	// Consume some periodic broadcasts - they should all be stuck on the previous version
	for msg := range vHandler.ch {
		require.Equal(t, 4, int(msg.CompletedVersion))
		if msg.CurrentVersion == 9 {
			break
		}
	}

	// Now complete version 9
	for j := 0; j < 10; j++ {
		err := vmgr.VersionComplete(9, 10, 0, false)
		require.NoError(t, err)
	}
	require.Equal(t, 10, mustGetCurrentVersion(vmgr))

	// Wait until we move to next version
	for msg := range vHandler.ch {
		if msg.CurrentVersion == 10 {
			require.Equal(t, 9, int(msg.CompletedVersion))
			if msg.CompletedVersion == 9 {
				break
			}
		}
	}
}

//
//func TestErrorWhenNotActivated(t *testing.T) {
//	cfg := &conf.Config{}
//	cfg.ApplyDefaults()
//
//	seqMgr := sequence.NewInMemSequenceManager()
//	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
//	defer func() {
//		stopVmgr(t, vmgr)
//		err := remotingServer.Stop()
//		require.NoError(t, err)
//	}()
//
//	_, _, _, err := vmgr.GetVersions()
//	require.Error(t, err)
//	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))
//
//	err = vmgr.VersionComplete(100, 100)
//	require.Error(t, err)
//	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))
//
//	err = vmgr.DoomInProgressVersions(100)
//	require.Error(t, err)
//	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))
//
//	vmgr.FailureDetected(100, 100, func(i int, err error) {
//		require.Error(t, err)
//		require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))
//	})
//
//	err = vmgr.VersionFlushed(1, 100, 100, 100)
//	require.Error(t, err)
//	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))
//
//	ok, err := vmgr.Shutdown()
//	require.False(t, ok)
//}

func TestStopWhileActivating(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lMgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// this will prevent version manager from activating
	lMgrClient.unavailable.Store(true)

	vmgr.Activate()

	_, _, _, err := vmgr.GetVersions()
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.Unavailable))

	// but we should still be able to stop it
	err = vmgr.Stop()
	require.NoError(t, err)
}

func TestActivateWithClusterState(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	cs := clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err := vmgr.HandleClusterState(cs)
	require.NoError(t, err)

	require.True(t, vmgr.IsActivating())

	waitUntilActive(t, vmgr)
}

func TestDoNotActivateWithClusterState(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	cs := clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err := vmgr.HandleClusterState(cs)
	require.NoError(t, err)

	require.False(t, vmgr.IsActivating())
}

func TestActivateThenDeactivateWithClusterState(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	cs := clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err := vmgr.HandleClusterState(cs)
	require.NoError(t, err)
	require.True(t, vmgr.IsActivating())
	waitUntilActive(t, vmgr)

	cs = clustmgr.ClusterState{
		Version: 2,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err = vmgr.HandleClusterState(cs)
	require.NoError(t, err)

	require.False(t, vmgr.IsActive())
}

func TestActivateThenDeactivateWithClusterStateWhileStillActivating(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	lmgrClient.unavailable.Store(true)

	cs := clustmgr.ClusterState{
		Version: 1,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: true, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: false, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err := vmgr.HandleClusterState(cs)
	require.NoError(t, err)
	require.True(t, vmgr.IsActivating())
	require.False(t, vmgr.IsActive())

	cs = clustmgr.ClusterState{
		Version: 2,
		GroupStates: [][]clustmgr.GroupNode{
			{
				clustmgr.GroupNode{NodeID: 0, Leader: false, Valid: true, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 1, Leader: true, Valid: false, JoinedVersion: 1},
				clustmgr.GroupNode{NodeID: 2, Leader: false, Valid: true, JoinedVersion: 1},
			},
		},
	}

	err = vmgr.HandleClusterState(cs)
	require.NoError(t, err)

	lmgrClient.unavailable.Store(true)

	require.False(t, vmgr.IsActive())
	require.False(t, vmgr.IsActivating())
}

func TestFailure(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	current, _, lastFlushed, err := vmgr.GetVersions()
	require.NoError(t, err)

	require.False(t, vmgr.failureInProgress())

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	require.True(t, vmgr.failureInProgress())

	lfv, err := vmgr.GetLastFailureFlushedVersion(1)
	require.NoError(t, err)

	// version not complete yet
	require.Equal(t, -2, lfv)

	// Should wait until *at least* current + 2 is completed
	completingVersion := current + 2

	err = vmgr.VersionComplete(completingVersion, 1, 0, false)
	require.NoError(t, err)

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lfv, err = vmgr.GetLastFailureFlushedVersion(1)
		if err != nil {
			return false, err
		}
		return lastFlushed == lfv, nil
	}, 2*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	deadRange := lmgrClient.getDeadVersionRange()
	require.NotNil(t, deadRange)

	require.Equal(t, lastFlushed+1, int(deadRange.VersionStart))
	require.Equal(t, current+2, int(deadRange.VersionEnd))

	current2, completed2, flushed2, err := vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, completingVersion, completed2)
	require.Equal(t, completingVersion+1, current2)
	require.Equal(t, lastFlushed, flushed2)

	err = vmgr.FailureComplete(16, 1)
	require.NoError(t, err)

	complete, err := vmgr.IsFailureComplete(1)
	require.NoError(t, err)
	require.False(t, complete)

	err = vmgr.FailureComplete(16, 1)
	require.NoError(t, err)

	complete, err = vmgr.IsFailureComplete(1)
	require.NoError(t, err)
	require.False(t, complete)

	err = vmgr.FailureComplete(16, 1)
	require.NoError(t, err)

	complete, err = vmgr.IsFailureComplete(1)
	require.NoError(t, err)
	require.True(t, complete)
}

func TestFlushDisabledWhileFailureInProgress(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	current, _, _, err := vmgr.GetVersions()
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	versionToFlush := vmgr.getLastVersionToFlush()

	// try and flush this version - it should be ignored as failure in progress
	err = vmgr.VersionFlushed(0, current, 2)
	require.NoError(t, err)
	err = vmgr.VersionFlushed(1, current, 2)
	require.NoError(t, err)
	err = vmgr.VersionFlushed(2, current, 2)
	require.NoError(t, err)

	require.Equal(t, versionToFlush, vmgr.getLastVersionToFlush())
}

func TestFailureDetectionNotCompletedWithNotAllProcessors(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	err := vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	// One too few live processors
	err = vmgr.FailureDetected(15, 1)
	require.NoError(t, err)

	require.False(t, vmgr.failureInProgress())

	require.Nil(t, lmgrClient.getDeadVersionRange())
}

func TestSecondFailureAtHigherClusterVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	err := vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 1)
	require.NoError(t, err)

	// greater cluster version
	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	require.False(t, vmgr.failureInProgress())

	// Now 1 and 2 call back in with matching cluster version

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	require.True(t, vmgr.failureInProgress())
}

func TestFailureWithLowerClusterVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	err := vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	// lower cluster version
	err = vmgr.FailureDetected(16, 1)
	require.Error(t, err)
	var terr errors.TektiteError
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)
}

func TestGetLastFailureFlushedVersionWrongClusterVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	err := vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	_, err = vmgr.GetLastFailureFlushedVersion(1)
	require.Error(t, err)
	var terr errors.TektiteError
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)

	_, err = vmgr.GetLastFailureFlushedVersion(3)
	require.Error(t, err)
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)
}

func TestFailureCompleteWrongClusterVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	current, _, lastFlushed, err := vmgr.GetVersions()
	require.NoError(t, err)

	require.False(t, vmgr.failureInProgress())

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	require.True(t, vmgr.failureInProgress())

	// Should wait until *at least* current + 2 is completed
	completingVersion := current + 2

	err = vmgr.VersionComplete(completingVersion, 1, 0, false)
	require.NoError(t, err)

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lfv, err := vmgr.GetLastFailureFlushedVersion(2)
		if err != nil {
			return false, err
		}
		return lastFlushed == lfv, nil
	}, 2*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	err = vmgr.FailureComplete(16, 1)
	require.Error(t, err)
	var terr errors.TektiteError
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)

	err = vmgr.FailureComplete(16, 3)
	require.Error(t, err)
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)
}

func TestIsFailureCompleteWrongClusterVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	current, _, lastFlushed, err := vmgr.GetVersions()
	require.NoError(t, err)

	require.False(t, vmgr.failureInProgress())

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	err = vmgr.FailureDetected(16, 2)
	require.NoError(t, err)

	require.True(t, vmgr.failureInProgress())

	// Should wait until *at least* current + 2 is completed
	completingVersion := current + 2

	err = vmgr.VersionComplete(completingVersion, 1, 0, false)
	require.NoError(t, err)

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lfv, err := vmgr.GetLastFailureFlushedVersion(2)
		if err != nil {
			return false, err
		}
		return lastFlushed == lfv, nil
	}, 2*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = vmgr.IsFailureComplete(1)
	require.Error(t, err)
	var terr errors.TektiteError
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)

	_, err = vmgr.IsFailureComplete(3)
	require.Error(t, err)
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)

	err = vmgr.FailureComplete(48, 2)
	require.NoError(t, err)

	_, err = vmgr.IsFailureComplete(1)
	require.Error(t, err)
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)

	_, err = vmgr.IsFailureComplete(3)
	require.Error(t, err)
	require.True(t, errors.As(err, &terr))
	require.Equal(t, errors.ErrorCode(errors.FailureCancelled), terr.Code)
}

func TestVersionFlushedAllSameVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionManagerStoreFlushedInterval = 10 * time.Millisecond
	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	curr, completed, _, err := vmgr.GetVersions()

	err = vmgr.VersionComplete(curr, 1, 0, false)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(0, completed, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, completed, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, completed, 1)
	require.NoError(t, err)

	require.Equal(t, completed, vmgr.getLastVersionToFlush())

	// Wait for it to be flushed to level manager
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return int(lmgrClient.getLastFlushedVersion()) == completed, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	_, _, flushed, err := vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, completed, flushed)
}

func TestVersionFlushedTakeMinVersion(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionManagerStoreFlushedInterval = 10 * time.Millisecond
	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// fast-forward versions
	for {
		curr, _, _, err := vmgr.GetVersions()
		require.NoError(t, err)
		if curr == 10 {
			break
		}
		err = vmgr.VersionComplete(curr, 1, 0, false)
		require.NoError(t, err)
	}

	err := vmgr.VersionFlushed(0, 7, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, 8, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 9, 1)
	require.NoError(t, err)

	require.Equal(t, 7, vmgr.getLastVersionToFlush())

	// Wait for it to be flushed to level manager
	waitForFlushedVersion(t, lmgrClient, 7)
	_, _, flushed, err := vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, 7, flushed)

	err = vmgr.VersionFlushed(0, 8, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, 8, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 9, 1)
	require.NoError(t, err)

	require.Equal(t, 8, vmgr.getLastVersionToFlush())

	// Wait for it to be flushed to level manager
	waitForFlushedVersion(t, lmgrClient, 8)
	_, _, flushed, err = vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, 8, flushed)

	err = vmgr.VersionFlushed(0, 9, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, 8, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 9, 1)
	require.NoError(t, err)

	require.Equal(t, 8, vmgr.getLastVersionToFlush())

	// Wait for it to be flushed to level manager
	waitForFlushedVersion(t, lmgrClient, 8)
	_, _, flushed, err = vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, 8, flushed)

	err = vmgr.VersionFlushed(0, 9, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, 9, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 9, 1)
	require.NoError(t, err)

	require.Equal(t, 9, vmgr.getLastVersionToFlush())

	// Wait for it to be flushed to level manager
	waitForFlushedVersion(t, lmgrClient, 9)
	_, _, flushed, err = vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, 9, flushed)
}

func TestVersionFlushedAllProcessorsCallIn(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	cfg.VersionManagerStoreFlushedInterval = 10 * time.Millisecond
	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// fast-forward versions
	for {
		curr, _, _, err := vmgr.GetVersions()
		require.NoError(t, err)
		if curr == 10 {
			break
		}
		err = vmgr.VersionComplete(curr, 1, 0, false)
		require.NoError(t, err)
	}

	err := vmgr.VersionFlushed(0, 7, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, 7, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 7, 1)
	require.NoError(t, err)

	// not all processors called in
	require.Equal(t, -1, vmgr.getLastVersionToFlush())

	err = vmgr.VersionFlushed(0, 7, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, 7, 1)
	require.NoError(t, err)

	// Wait for it to be flushed to level manager
	waitForFlushedVersion(t, lmgrClient, 7)
	_, _, flushed, err := vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, 7, flushed)
}

func TestShutdown(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	// fast-forward a bit
	var completed int
	for {
		curr, compl, _, err := vmgr.GetVersions()
		completed = compl
		require.NoError(t, err)
		if curr == 5 {
			break
		}
		err = vmgr.VersionComplete(curr, 1, 0, false)
		require.NoError(t, err)
	}

	// shutdown will block until last completed version is flushed and stored in level manager
	go func() {
		ok, err := vmgr.Shutdown()
		if err != nil {
			panic(err)
		}
		if !ok {
			panic("shutdown returned false")
		}
	}()

	err := vmgr.VersionFlushed(0, completed, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(1, completed, 1)
	require.NoError(t, err)

	err = vmgr.VersionFlushed(2, completed, 1)
	require.NoError(t, err)

	waitForFlushedVersion(t, lmgrClient, completed)
	require.Equal(t, completed, vmgr.getLastVersionToFlush())
}

func TestLoadLastFlushedVersionOnStartup(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	seqMgr := sequence.NewInMemSequenceManager()
	vmgr, remotingServer, _, lmgrClient := setupWithSeqMgrWithActivate(t, seqMgr, cfg, false)
	defer func() {
		stopVmgr(t, vmgr)
		err := remotingServer.Stop()
		require.NoError(t, err)
	}()

	var lfv int64 = 23

	// must fast-forward version sequence it's greater than last flushed version or activate will fail
	for {
		id, err := seqMgr.GetNextID(currVersionSequenceName, 10)
		require.NoError(t, err)
		if id > int(lfv) {
			break
		}
	}

	lmgrClient.setlastFlushedVersion(lfv)

	vmgr.activate()
	waitUntilActive(t, vmgr)

	curr, lastCompleted, lastFlushed, err := vmgr.GetVersions()
	require.NoError(t, err)
	require.Equal(t, int(lfv), lastFlushed)

	log.Debugf("curr %d lc %d", curr, lastCompleted)
}

func waitForFlushedVersion(t *testing.T, lmgrClient *testLevelMgrClient, version int) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return int(lmgrClient.getLastFlushedVersion()) == version, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func newVersionHandler() *versionHandler {
	return &versionHandler{ch: make(chan *clustermsgs.VersionsMessage, 100)}
}

type versionHandler struct {
	ch chan *clustermsgs.VersionsMessage
}

func (v *versionHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	vmsg := messageHolder.Message.(*clustermsgs.VersionsMessage)
	v.ch <- vmsg
	return nil, nil
}

func setup(t *testing.T, cfg *conf.Config) (*VersionManager, remoting.Server, *versionHandler) {
	seqMgr := sequence.NewInMemSequenceManager()
	return setupWithSeqMgr(t, seqMgr, cfg)
}

func setupWithSeqMgr(t *testing.T, seqMgr sequence.Manager,
	cfg *conf.Config) (*VersionManager, remoting.Server, *versionHandler) {
	vmgr, rServer, vHandler, _ := setupWithSeqMgrWithActivate(t, seqMgr, cfg, true)
	return vmgr, rServer, vHandler
}

func setupWithSeqMgrWithActivate(t *testing.T, seqMgr sequence.Manager, cfg *conf.Config,
	activate bool) (*VersionManager, remoting.Server, *versionHandler, *testLevelMgrClient) {

	serverAddress := "localhost:7888"

	remotingServer := remoting.NewServer(serverAddress, conf.TLSConfig{})
	err := remotingServer.Start()
	require.NoError(t, err)

	lmgrClient := &testLevelMgrClient{lastFlushedVersion: -1}

	vmgr := NewVersionManager(seqMgr, lmgrClient, cfg, "localhost:7888")
	err = vmgr.Start()
	require.NoError(t, err)

	vHandler := newVersionHandler()
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageVersionsMessage, vHandler)

	if activate {
		vmgr.Activate()
		waitUntilActive(t, vmgr)
	}

	return vmgr, remotingServer, vHandler, lmgrClient
}

func waitUntilActive(t *testing.T, vmgr *VersionManager) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return vmgr.IsActive(), nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func mustGetCurrentVersion(vMgr *VersionManager) int {
	ver, _, _, err := vMgr.GetVersions()
	if err != nil {
		panic(err)
	}
	return ver
}

type testLevelMgrClient struct {
	lock               sync.Mutex
	lastFlushedVersion int64
	unavailable        atomic.Bool
	deadVersionRange   *levels.VersionRange
}

func (t *testLevelMgrClient) GetStats() (levels.Stats, error) {
	return levels.Stats{}, nil
}

func (t *testLevelMgrClient) setlastFlushedVersion(version int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.lastFlushedVersion = version
}

func (t *testLevelMgrClient) getLastFlushedVersion() int64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.lastFlushedVersion
}

func (t *testLevelMgrClient) getDeadVersionRange() *levels.VersionRange {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.deadVersionRange
}

func (t *testLevelMgrClient) GetTableIDsForRange([]byte, []byte) (levels.OverlappingTableIDs, []levels.VersionRange, error) {
	panic("not implemented")
}

func (t *testLevelMgrClient) RegisterL0Tables(levels.RegistrationBatch) error {
	panic("not implemented")
}

func (t *testLevelMgrClient) ApplyChanges(levels.RegistrationBatch) error {
	panic("not implemented")
}

func (t *testLevelMgrClient) PollForJob() (*levels.CompactionJob, error) {
	panic("not implemented")
}

func (t *testLevelMgrClient) RegisterDeadVersionRange(versionRange levels.VersionRange, _ string, _ int) error {
	if t.unavailable.Load() {
		return errors.NewTektiteErrorf(errors.Unavailable, "unavailable")
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.deadVersionRange = &versionRange
	return nil
}

func (t *testLevelMgrClient) StoreLastFlushedVersion(version int64) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.lastFlushedVersion = version
	return nil
}

func (t *testLevelMgrClient) LoadLastFlushedVersion() (int64, error) {
	if t.unavailable.Load() {
		return 0, errors.NewTektiteErrorf(errors.Unavailable, "unavailable")
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.lastFlushedVersion, nil
}

func (t *testLevelMgrClient) RegisterSlabRetention(slabID int, retention time.Duration) error {
	panic("not implemented")

}

func (t *testLevelMgrClient) UnregisterSlabRetention(slabID int) error {
	panic("not implemented")

}

func (t *testLevelMgrClient) GetSlabRetention(slabID int) (time.Duration, error) {
	panic("not implemented")
}

func (t *testLevelMgrClient) Start() error {
	panic("not implemented")
}

func (t *testLevelMgrClient) Stop() error {
	panic("not implemented")
}

func stopVmgr(t *testing.T, vmgr *VersionManager) {
	err := vmgr.Stop()
	require.NoError(t, err)
}
