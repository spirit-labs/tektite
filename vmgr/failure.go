package vmgr

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/debug"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sanity"
	"sync/atomic"
	"time"
)

type failureInfo struct {
	failureVersionsCompletedRunning atomic.Bool
	requiredProcessorCount          int
	failureClusterVersion           int
	lastFailureFlushedVersion       int
	versionWaitComplete             bool
	failureProcessorCount           int
	failureComplete                 bool
	deadVersionRange                *levels.VersionRange
}

func (v *VersionManager) FailureDetected(liveProcessorCount int, clusterVersion int) error {
	log.Debugf("VersionManager got FailureDetected lpc %d cv %d 1", liveProcessorCount, clusterVersion)
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		log.Debug("vmgr not active")
		return err
	}
	if v.failureClusterVersion != -1 && clusterVersion != v.failureClusterVersion {
		if clusterVersion > v.failureClusterVersion {
			log.Debug("failure at higher cluster version")
			// Failure at higher cluster version - reset
			v.failureProcessorCount = 0
		} else {
			log.Debug("failure at lower cluster version")
			// Failure at lower cluster version - reject this call
			return common.NewTektiteError(common.FailureCancelled, "failureDetected at lower version")
		}
	}

	v.failureClusterVersion = clusterVersion
	v.failureProcessorCount += liveProcessorCount

	log.Debugf("vmgr failureprocessor count %d required processor count %d", v.failureProcessorCount, v.requiredProcessorCount)

	if v.failureProcessorCount == v.requiredProcessorCount {
		// All processors have called in, so we know there are deployed processors after the last failure
		// Its possible versions can get stuck as processors are deployed, so we unstick them by setting
		// handleFailureVersion to currentVersion + 2 - versions in progress can be currentVersion and currentVersion + 1
		// when that version has completed we know there is no processing in progress.

		// We roll back to the last snapshot version so all data (if any) in the following range (inclusive for both
		// bounds) will be marked for deletion
		v.deadVersionRange = &levels.VersionRange{
			VersionStart: uint64(v.lastFlushedVersion + 1),
			VersionEnd:   uint64(v.currentVersion + 2),
		}
		log.Debugf("versionmanager failure - all processors called in dead version range is %d to %d",
			v.deadVersionRange.VersionStart, v.deadVersionRange.VersionEnd)
		for v.currentVersion < int(v.deadVersionRange.VersionEnd) {
			// fast-forward current version to pas the dead range
			if err := v.setNextCurrentVersion(); err != nil {
				log.Errorf("failed to set next version %v", err)
				return err
			}
		}
		// We changed currentVersion so set completion count to zero
		v.completionCount = 0
		// And we disable flushed version being updated until we are done
		v.disableFlush = true
		v.lastVersionToFlush = -1

		v.broadcastVersionsAsync()
	}
	return nil
}

func (v *VersionManager) GetLastFailureFlushedVersion(clusterVersion int) (int, error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		return 0, err
	}
	log.Debugf("vmgr in GetLastFailureFlushedVersion for clusterVersion %d. lastFailureFlushedVersion %d versionWaitComplete %t",
		clusterVersion, v.lastFailureFlushedVersion, v.versionWaitComplete)
	if clusterVersion != v.failureClusterVersion {
		return 0, common.NewTektiteErrorf(common.FailureCancelled, "wrong clusterVersion")
	}
	if v.versionWaitComplete {
		return v.lastFailureFlushedVersion, nil
	}
	// Note that -1 is a valid lastFailureFlushedVersion (i.e. flush hasn't occurred) so we return -2 to represent
	// hasn't waited for required cluster version
	return -2, nil
}

func (v *VersionManager) FailureComplete(liveProcessorCount int, clusterVersion int) error {
	log.Debugf("VersionManager got FailureComplete lpc %d cv %d 1", liveProcessorCount, clusterVersion)
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		log.Debug("vmgr not active")
		return err
	}
	if clusterVersion != v.failureClusterVersion {
		return common.NewTektiteErrorf(common.FailureCancelled, "wrong clusterVersion")
	}
	if !v.versionWaitComplete {
		return common.NewTektiteErrorf(common.FailureCancelled, "version wait not complete")
	}
	v.failureProcessorCount += liveProcessorCount
	if v.failureProcessorCount == v.requiredProcessorCount {
		v.failureComplete = true
	}
	return nil
}

func (v *VersionManager) IsFailureComplete(clusterVersion int) (bool, error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		log.Debug("vmgr not active")
		return false, err
	}
	if clusterVersion != v.failureClusterVersion {
		return false, common.NewTektiteErrorf(common.FailureCancelled, "wrong clusterVersion")
	}
	return v.failureComplete, nil
}

func (v *VersionManager) failureVersionsCompleted(deadVersionRange levels.VersionRange, failureClusterVersion int) error {
	log.Debug("vmgr handleFailure")
	// All processors have called in, and we have waited for in-progress versions to complete
	// We register the dead version range with the level manager, this causes it to start compaction(s) to remove
	// any data with those versions.
	// This section must be outside lock as it can block retrying
	for {
		err := v.levelMgrClient.RegisterDeadVersionRange(deadVersionRange, v.cfg.ClusterName, failureClusterVersion)
		if err == nil {
			if debug.AggregateChecks {
				sanity.RegisterDeadVersionRange(deadVersionRange)
			}
			break
		}
		if v.isStopped() {
			return nil
		}
		if common.IsUnavailableError(err) {
			log.Warnf("vmgr retrying RegisterDeadVersionRange as level manager is unavailable")
			// The level manager is temporarily unavailable - retry after delay
			time.Sleep(v.cfg.VersionManagerLevelManagerRetryDelay)
			continue
		} else {
			return err
		}
	}

	log.Debugf("vmgr registered RegisterDeadVersionRange ok")

	v.lock.Lock()
	defer v.lock.Unlock()
	if v.failureClusterVersion > failureClusterVersion {
		log.Debug("v.failureClusterVersion > failureClusterVersion")
		// while we were registering another failure came in - we cancel this
		return nil
	}
	// We record the last flushed version (the end range of the dead version range) and the cluster version for this
	v.lastFailureFlushedVersion = int(v.deadVersionRange.VersionStart - 1)
	v.versionWaitComplete = true
	log.Debugf("vmgr handleFailure recording lastFailureFlushedVersion %dd", v.lastFailureFlushedVersion)

	v.failureProcessorCount = 0
	v.deadVersionRange = nil
	v.disableFlush = false

	return nil
}

func (v *VersionManager) failureInProgress() bool {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.deadVersionRange != nil
}
