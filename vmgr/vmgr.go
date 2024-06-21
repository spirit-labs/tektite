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

package vmgr

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/sequence"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const currVersionSequenceName = "seq.vmgr.currVersion"
const VersionManagerProcessorID = 0
const versionSequenceBatchSize = 1000

type VersionManager struct {
	cfg                    *conf.Config
	lock                   sync.Mutex
	remotingClient         *remoting.Client
	levelMgrClient         levels.Client
	serverAddresses        []string
	active                 bool
	shutdown               bool
	stopped                atomic.Bool
	activating             atomic.Bool
	completionCount        int
	currentVersion         int
	currentCommandID       int
	minCompletableVersion  int
	lastCompletedVersion   int
	disableFlush           bool
	lastVersionToFlush     int // This is the last version all processors have called in as flushed
	lastFlushedVersion     int // This is the last version that has been reliably flushed to the level manager
	flushingClusterVersion int
	flushedEntries         map[int]flushedEntry
	seqMgr                 sequence.Manager
	broadcastVersionsTimer *common.TimerHandle
	hasBroadcast           bool
	flushedVersionTimer    *common.TimerHandle
	shutdownFlushVersion   int
	shutdownWg             *sync.WaitGroup
	activateWg             sync.WaitGroup
	stuckVersionTimer      *common.TimerHandle
	failureInfo
}

type flushedEntry struct {
	flushedVersion int
	processorCount int
}

type Client interface {
	// GetVersions returns the versions (current, lastCompleted, lastFlushed), from the version manager.
	// This must block until the version manager is available
	GetVersions() (int, int, int, error)
	// VersionComplete notifies the version manager about a version completion from a processor and terminal receiver
	VersionComplete(version int, requiredCompletions int, commandID int, doom bool, completionFunc func(error))
	FailureDetected(liveProcessorCount int, clusterVersion int) error
	VersionFlushed(nodeID int, version int, liveProcessorCount int, clusterVersion int) error
	GetLastFailureFlushedVersion(clusterVersion int) (int, error)
	FailureComplete(liveProcessorCount int, clusterVersion int) error
	IsFailureComplete(clusterVersion int) (bool, error)
	Start() error
	Stop() error
}

func NewVersionManager(seqMgr sequence.Manager, levelMgrClient levels.Client, cfg *conf.Config,
	serverAddresses ...string) *VersionManager {
	requiredProcessorCount := cfg.ProcessorCount
	if cfg.LevelManagerEnabled {
		requiredProcessorCount++
	}
	vmgr := &VersionManager{
		levelMgrClient:         levelMgrClient,
		cfg:                    cfg,
		serverAddresses:        serverAddresses,
		flushedEntries:         map[int]flushedEntry{},
		seqMgr:                 seqMgr,
		currentVersion:         0,
		lastCompletedVersion:   -1,
		lastVersionToFlush:     -1,
		lastFlushedVersion:     -1,
		flushingClusterVersion: -1,
		shutdownFlushVersion:   -1,
	}
	vmgr.requiredProcessorCount = requiredProcessorCount
	vmgr.lastFailureFlushedVersion = -1
	vmgr.failureClusterVersion = -1
	return vmgr
}

const levelManagerRetryDelay = 1 * time.Second

func (v *VersionManager) Start() error {
	return nil
}

func (v *VersionManager) Stop() error {
	if v.stopped.Load() {
		// Version manager gets stopped explicitly in server Stop() so it can get called more than once - its called
		// again as one of the slice of services that are stopped. So we ignore if already stopped.
		log.Debug("vmgr already stopped")
		return nil
	}

	log.Debugf("node %d vmgr stopping", v.cfg.NodeID)

	// Make sure activating goroutine stops if in progress
	v.activating.Store(false)
	v.stopped.Store(true)
	v.activateWg.Wait()

	v.lock.Lock()
	log.Debugf("node %d vmgr stopping, active? %t", v.cfg.NodeID, v.active)
	v.active = false
	bvt := v.broadcastVersionsTimer
	if bvt != nil {
		bvt.Stop()
	}
	fvt := v.flushedVersionTimer
	if fvt != nil {
		fvt.Stop()
	}
	svt := v.stuckVersionTimer
	if svt != nil {
		svt.Stop()
	}
	if v.remotingClient != nil {
		v.remotingClient.Stop()
	}
	// Must wait outside lock
	v.lock.Unlock()
	if bvt != nil {
		bvt.WaitComplete()
	}
	if fvt != nil {
		fvt.WaitComplete()
	}
	if svt != nil {
		svt.WaitComplete()
	}
	return nil
}

func (v *VersionManager) isStopped() bool {
	return v.stopped.Load()
}

func (v *VersionManager) IsActive() bool {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.active
}

func (v *VersionManager) IsActivating() bool {
	return v.activating.Load()
}

func (v *VersionManager) Activate() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.activate()
}

func (v *VersionManager) activate() {
	log.Debugf("vmgr activating on node %d", v.cfg.NodeID)
	v.activating.Store(true)
	v.activateWg.Add(1)
	// We run activation on a separate goroutine as it needs to contact the level manager which might not be available,
	// and we can't block the cluster message handler
	common.Go(func() {
		defer v.activateWg.Done()
		if err := v.doActivate(); err != nil {
			log.Errorf("failed to activate version manager: %v", err)
		}
		log.Debugf("vmgr now activated on node %d", v.cfg.NodeID)
	})
}

func (v *VersionManager) doActivate() error {
	// Get last flushed version
	var lfv int64
	for {
		var err error
		log.Debugf("node %d vmgr loading last flushed version", v.cfg.NodeID)
		lfv, err = v.levelMgrClient.LoadLastFlushedVersion()
		if err == nil {
			break
		}
		if !v.IsActivating() {
			log.Debugf("node %d breaking out of activation loop", v.cfg.NodeID)
			// break out of loop if activation cancelled - e.g. during stop or deactivation
			break
		}
		log.Debugf("vmgr got error from level manager: %v", err)
		var terr errors.TektiteError
		if errors.As(err, &terr) {
			if terr.Code == errors.Unavailable || terr.Code == errors.LevelManagerNotLeaderNode {
				// The level manager is temporarily unavailable - retry after delay
				time.Sleep(levelManagerRetryDelay)
				continue
			}
		}
		log.Debugf("vmgr not retrying load lastflushed version, error  type: %s", reflect.TypeOf(err).String())
		return err
	}
	v.lock.Lock()
	defer v.lock.Unlock()
	if !v.IsActivating() {
		// was deactivated by cluster state before this was run
		return nil
	}
	if v.active {
		panic("already active")
	}
	if v.flushedVersionTimer != nil {
		panic("already has timer")
	}
	log.Debugf("vmgr loaded last flushed version as %d", lfv)
	v.lastFlushedVersion = int(lfv)
	v.lastVersionToFlush = v.lastFlushedVersion
	v.remotingClient = remoting.NewClient(v.cfg.ClusterTlsConfig)
	err := v.setNextCurrentVersion()
	if err != nil {
		return err
	}
	if v.currentVersion <= v.lastFlushedVersion {
		// Sanity check
		panic(fmt.Sprintf("cannot start version manager, current version %d is <= last flushed version %d",
			v.currentVersion, v.lastFlushedVersion))
	}
	v.active = true
	v.activating.Store(false)
	v.broadcastVersions()
	v.scheduleBroadcast(true)
	v.scheduleLastFlushedVersion(true)
	v.scheduleStuckVersionCheck(true, -1)
	return nil
}

func (v *VersionManager) StopCompletion() {
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		return
	}
	v.minCompletableVersion = math.MaxInt
}

// Shutdown waits for the version to be flushed, then it makes sure the flushedVersion has been written to the level
// manager, before stopping.
func (v *VersionManager) Shutdown() (bool, error) {
	log.Debugf("node %d version manager shutdown", v.cfg.NodeID)
	v.lock.Lock()

	if err := v.checkActive(); err != nil {
		v.lock.Unlock()
		return false, nil
	}

	// We wait for last completed version to be flushed to the version manager
	// By the time this is called during shutdown processing has been paused, and we've already waited for at least
	// currWriteVersion + 2 to complete. There won't be any more data for version > lastCompletedVersion.
	shutdownVersion := v.lastCompletedVersion

	// If the version is not flushed yet, we wait
	if v.lastVersionToFlush < shutdownVersion {
		log.Debugf("node %d vmgr shutdown - waiting for version %d to be flushed, last %d",
			v.cfg.NodeID, shutdownVersion, v.lastVersionToFlush)

		v.shutdownFlushVersion = shutdownVersion
		v.shutdownWg = &sync.WaitGroup{}
		v.shutdownWg.Add(1)
		v.flushedVersionTimer.Stop() // We stop this as we're going to explicitly store it when version is flushed
		v.flushedVersionTimer = nil
		wg := v.shutdownWg
		v.lock.Unlock()

		log.Debugf("node %d versionManager waiting for version %d to be flushed to version manager", v.cfg.NodeID,
			shutdownVersion)

		// we wait for version to be flushed outside lock
		wg.Wait()
		v.lock.Lock()
	}

	log.Debugf("node %d versionManager waiting for version %d to be flushed to levelManager", v.cfg.NodeID,
		v.lastVersionToFlush)

	// Now we must store flushed version in level manager, without the lock held
	lfv := v.lastVersionToFlush
	v.lock.Unlock()
	if lfv == -1 {
		panic("no version flushed")
	}

	for {
		var err error
		err = v.levelMgrClient.StoreLastFlushedVersion(int64(lfv))
		if err == nil {
			break
		}
		if v.isStopped() {
			return false, errors.New("version manager failed over during shutdown")
		}
		var terr errors.TektiteError
		if errors.As(err, &terr) {
			if terr.Code == errors.Unavailable || terr.Code == errors.LevelManagerNotLeaderNode {
				// The level manager is temporarily unavailable - retry after delay
				time.Sleep(levelManagerRetryDelay)
				continue
			}
		}
	}
	v.lock.Lock()
	v.lastFlushedVersion = lfv
	v.active = false
	if v.broadcastVersionsTimer != nil {
		v.broadcastVersionsTimer.Stop()
	}
	if v.flushedVersionTimer != nil {
		v.flushedVersionTimer.Stop()
	}
	v.remotingClient.Stop()
	v.shutdown = true
	// Must wait outside lock
	vbcTimer := v.broadcastVersionsTimer
	v.broadcastVersionsTimer = nil
	fvTimer := v.flushedVersionTimer
	v.flushedVersionTimer = nil
	v.lock.Unlock()
	if vbcTimer != nil {
		vbcTimer.WaitComplete()
	}
	if fvTimer != nil {
		fvTimer.WaitComplete()
	}
	return true, nil
}

func (v *VersionManager) setNextCurrentVersion() error {
	currVersion, err := v.seqMgr.GetNextID(currVersionSequenceName, versionSequenceBatchSize)
	if err != nil {
		return err
	}
	v.currentVersion = currVersion
	return nil
}

func (v *VersionManager) HandleClusterState(cs clustmgr.ClusterState) error {
	v.lock.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			v.lock.Unlock()
		}
	}()
	log.Debugf("node %d vmgr handling cluster state version %d", v.cfg.NodeID, cs.Version)
	if v.isLeader(&cs) {
		log.Debugf("node is leader, activating? %t active? %t", v.IsActivating(), v.active)
		if !v.IsActivating() && !v.active {
			v.activate()
			return nil
		}
	} else {
		if v.active {
			v.active = false
			if v.broadcastVersionsTimer != nil {
				v.broadcastVersionsTimer.Stop()
			}
			if v.flushedVersionTimer != nil {
				v.flushedVersionTimer.Stop()
			}
			v.remotingClient.Stop()
			v.lock.Unlock()
			unlocked = true
			// Must wait outside lock
			if v.broadcastVersionsTimer != nil {
				v.broadcastVersionsTimer.WaitComplete()
				v.broadcastVersionsTimer = nil
			}
			if v.flushedVersionTimer != nil {
				v.flushedVersionTimer.WaitComplete()
				v.flushedVersionTimer = nil
			}
		} else if v.IsActivating() {
			// If activating this will prevent activation completing
			v.activating.Store(false)
			// And we must wait for activation goroutine to complete
			v.lock.Unlock()
			unlocked = true
			v.activateWg.Wait()
		}
	}
	return nil
}

func (v *VersionManager) isLeader(cs *clustmgr.ClusterState) bool {
	if len(cs.GroupStates) == 0 {
		return false
	}
	groupState := cs.GroupStates[VersionManagerProcessorID]
	leaderNode := -1
	for _, groupNode := range groupState {
		if groupNode.Leader {
			leaderNode = groupNode.NodeID
			break
		}
	}
	log.Debugf("leader node for version manager is %d", leaderNode)
	return leaderNode == v.cfg.NodeID
}

func (v *VersionManager) GetVersions() (int, int, int, error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		return 0, 0, 0, err
	}
	return v.currentVersion, v.lastCompletedVersion, v.lastFlushedVersion, nil
}

func (v *VersionManager) GetLastCompletedVersion() (int, error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if err := v.checkActive(); err != nil {
		return 0, err
	}
	return v.lastCompletedVersion, nil
}

func (v *VersionManager) broadcastVersionsAsync() {
	common.Go(func() {
		v.lock.Lock()
		defer v.lock.Unlock()
		if !v.active {
			return
		}
		v.broadcastVersions()
	})
}

func (v *VersionManager) VersionFlushed(nodeID int, version int, liveProcessorCount int, clusterVersion int) error {
	v.lock.Lock()
	defer v.lock.Unlock()
	log.Debugf("version flushed called node id %d version %d pcount %d cv %d",
		nodeID, version, liveProcessorCount, clusterVersion)
	if err := v.checkActive(); err != nil {
		return err
	}
	if v.disableFlush {
		return nil
	}
	if version <= v.lastVersionToFlush {
		// When a node starts up it will flush last completed version which might have already been flushed
		// Ignore
		return nil
	}
	if clusterVersion < v.flushingClusterVersion {
		log.Debugf("flush version from node %d rejected as cluster version too low", nodeID)
		// Ignore
		return nil
	}
	if clusterVersion > v.flushingClusterVersion {
		log.Debugf("flush version from node %d has increased so resetting entries", nodeID)
		// all entries must be at the same cluster version
		v.flushingClusterVersion = clusterVersion
		v.flushedEntries = map[int]flushedEntry{}
	}
	v.flushedEntries[nodeID] = flushedEntry{
		flushedVersion: version,
		processorCount: liveProcessorCount,
	}
	minVersion := math.MaxInt64
	count := 0
	for _, entry := range v.flushedEntries {
		count += entry.processorCount
		// We take the min version that all processors have flushed at. This gives us the overall flushed
		// version across the cluster
		if entry.flushedVersion < minVersion {
			minVersion = entry.flushedVersion
		}
	}
	if count == v.requiredProcessorCount && minVersion > v.lastVersionToFlush {
		log.Debugf("version %d has been flushed to version manager", minVersion)
		v.lastVersionToFlush = minVersion
		if v.shutdownWg != nil && v.lastVersionToFlush >= v.shutdownFlushVersion {
			v.shutdownFlushVersion = -1
			v.shutdownWg.Done()
			v.disableFlush = true
			v.shutdownWg = nil
		}
		v.broadcastVersions()
	}
	return nil
}

func (v *VersionManager) getLastVersionToFlush() int {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.lastVersionToFlush
}

func (v *VersionManager) VersionComplete(version int, requiredCompletions int, commandID int, doom bool) error {
	v.lock.Lock()
	defer v.lock.Unlock()
	log.Debugf("vmgr got versioncomplete version %d rq %d cc %d command id %d doom %t", version,
		requiredCompletions, v.completionCount, commandID, doom)
	if err := v.checkActive(); err != nil {
		return err
	}
	if version <= v.lastCompletedVersion {
		// This can occur if a version has completed then a stream is deployed, increasing the value of
		// required completions, so a completion comes in after the version is complete. We ignore this.
		return nil
	}
	if !doom && version > v.currentVersion {
		panic(fmt.Sprintf("version %d > currentVersion %d", version, v.currentVersion))
	}
	if version < v.currentVersion {
		// This can occur after version manager has failed over and loaded a new sequence value for current version
		// which is likely to be higher than the current version being completed. In this case we ignore it
		// the higher version will have barriers injected and inject soon after
		return nil
	}
	if version < v.minCompletableVersion {
		// We have doomed versions, do not allow them to be completed, or we're dooming an already doomed version
		return nil
	}
	if !doom && version != v.currentVersion {
		panic("not current version") // sanity
	}
	if v.currentCommandID != -1 && v.currentCommandID != commandID {
		// command id has changed - we will doom the version
		doom = true
	}
	if doom {
		// Command ID has changed or client has called to doom a version - this means that a stream was deployed / undeployed
		// and therefore version could be invalid, so we must not complete it
		return v.doomVersion(version)
	}
	v.completionCount++
	if v.completionCount == requiredCompletions {
		if v.deadVersionRange != nil && version >= int(v.deadVersionRange.VersionEnd) {
			if v.failureVersionsCompletedRunning.CompareAndSwap(false, true) {
				fcv := v.failureClusterVersion
				dvr := *v.deadVersionRange
				common.Go(func() {
					defer v.failureVersionsCompletedRunning.Store(false)
					// We run on separate GR as can block
					if err := v.failureVersionsCompleted(dvr, fcv); err != nil {
						log.Errorf("failure in handleFailure: %v", err)
					}
				})
			}
		}
		// We only complete at the current version. Processor managers never unilaterally increment current version
		v.lastCompletedVersion = v.currentVersion
		if err := v.setNextCurrentVersion(); err != nil {
			return err
		}
		log.Debugf("node %d version %d is complete current version is now %d", v.cfg.NodeID, v.lastCompletedVersion, v.currentVersion)
		v.completionCount = 0
		v.currentCommandID = -1
		v.broadcastVersions()
	}
	return nil
}

func (v *VersionManager) doomVersion(version int) error {
	v.minCompletableVersion = version + 1

	// Now increment the current version until it's at least as great at this
	for v.currentVersion < v.minCompletableVersion {
		if err := v.setNextCurrentVersion(); err != nil {
			return err
		}
	}
	// We changed currentVersion so set completion count to zero
	v.completionCount = 0

	v.currentCommandID = -1

	// And broadcast so processor managers get the update, and can inject barriers at the later versions.
	v.broadcastVersionsAsync()

	return nil
}

func (v *VersionManager) checkActive() error {
	if v.shutdown {
		// We don't return an unavailable as that would cause clients to retry - and then that can cause shutdown to
		// hang, e.g. waiting for stores to close who are trying to flush a version.
		return errors.NewTektiteErrorf(errors.VersionManagerShutdown,
			fmt.Sprintf("version manager is shutdown on node %d", v.cfg.NodeID))
	}
	if !v.active {
		return errors.NewTektiteErrorf(errors.Unavailable,
			fmt.Sprintf("version manager is not active on node %d", v.cfg.NodeID))
	}
	return nil
}

func (v *VersionManager) scheduleBroadcast(first bool) {
	v.broadcastVersionsTimer = common.ScheduleTimer(v.cfg.VersionCompletedBroadcastInterval, first, func() {
		v.lock.Lock()
		defer v.lock.Unlock()
		if !v.active {
			return
		}
		// We check the hasBroadcast flag - we don't broadcast if a broadcast has been sent (e.g. because of
		// a version completing, since the last check)
		if !v.hasBroadcast {
			v.broadcastVersions()
		}
		v.hasBroadcast = false
		v.scheduleBroadcast(false)
	})
}

func (v *VersionManager) broadcastVersions() {
	v.doBroadcastVersions()
	v.hasBroadcast = true
}

func (v *VersionManager) doBroadcastVersions() {
	// broadcast is best-effort
	v.remotingClient.BroadcastAsync(func(err error) {
		if err != nil {
			log.Debugf("failed to broadcast versions %v", err)
		}
	}, &clustermsgs.VersionsMessage{
		CurrentVersion:   int64(v.currentVersion),
		CompletedVersion: int64(v.lastCompletedVersion),
		FlushedVersion:   int64(v.lastFlushedVersion),
	}, v.serverAddresses...)
}

func (v *VersionManager) scheduleLastFlushedVersion(first bool) {
	log.Debugf("vmgr scheduling flush after %d ms", v.cfg.VersionManagerStoreFlushedInterval.Milliseconds())
	v.flushedVersionTimer = common.ScheduleTimer(v.cfg.VersionManagerStoreFlushedInterval, first, func() {
		v.storeLastFlushedVersion()
	})
}

func (v *VersionManager) scheduleStuckVersionCheck(first bool, lastVersion int) {
	v.stuckVersionTimer = common.ScheduleTimer(5*time.Second, first, func() {
		v.checkStuckVersion(lastVersion)
	})
}

func (v *VersionManager) checkStuckVersion(lastVersion int) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.currentVersion == lastVersion {
		log.Warnf("version manager advancing stuck version %d", lastVersion)
		if err := v.doomVersion(v.currentVersion); err != nil {
			log.Warnf("failed to doom stuck version %v", err)
		}
	}
	v.scheduleStuckVersionCheck(false, v.currentVersion)
}

func (v *VersionManager) storeLastFlushedVersion() {
	v.lock.Lock()
	defer v.lock.Unlock()
	defer v.scheduleLastFlushedVersion(false)
	lvf := v.lastVersionToFlush
	log.Debugf("vmgr last version to flush is %d", lvf)
	if lvf == -1 || lvf == v.lastFlushedVersion {
		// Already flushed this version, or nothing flushed yet
		return
	}
	if err := v.levelMgrClient.StoreLastFlushedVersion(int64(lvf)); err != nil {
		log.Warnf("vmgr:%p failed to store last flushed version: %v", v, err)
		return
	}
	v.lastFlushedVersion = lvf
	log.Debugf("vmgr: version %d has been flushed to level manager", lvf)
}

func (v *VersionManager) SetClusterMessageHandlers(remotingServer remoting.Server) {
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageGetVersionMessage, &getVersionHandler{v: v})
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageVersionCompleteMessage, &versionCompleteHandler{v: v})
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageVersionFlushedMessage, &versionFlushedHandler{v: v})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageFailureDetectedMessage, &failureDetectedHandler{v: v})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageGetLastFailureFlushedVersionMessage, &getLastFailureFlushedVersionHandler{v: v})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageFailureCompleteMessage, &failureCompleteHandler{v: v})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageIsFailureCompleteMessage, &isFailureCompleteHandler{v: v})

}

type getVersionHandler struct {
	v *VersionManager
}

func (g *getVersionHandler) HandleMessage(_ remoting.MessageHolder) (remoting.ClusterMessage, error) {
	currVersion, lastCompleted, lastFlushed, err := g.v.GetVersions()
	if err != nil {
		return nil, err
	}
	return &clustermsgs.VersionsMessage{
		CurrentVersion:   int64(currVersion),
		CompletedVersion: int64(lastCompleted),
		FlushedVersion:   int64(lastFlushed),
	}, nil
}

type failureDetectedHandler struct {
	v *VersionManager
}

func (f *failureDetectedHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.FailureDetectedMessage)
	err := f.v.FailureDetected(int(msg.ProcessorCount), int(msg.ClusterVersion))
	return nil, err
}

type getLastFailureFlushedVersionHandler struct {
	v *VersionManager
}

func (g *getLastFailureFlushedVersionHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.GetLastFailureFlushedVersionMessage)
	ver, err := g.v.GetLastFailureFlushedVersion(int(msg.ClusterVersion))
	return &clustermsgs.GetLastFailureFlushedVersionResponse{FlushedVersion: int64(ver)}, err
}

type failureCompleteHandler struct {
	v *VersionManager
}

func (f *failureCompleteHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.FailureCompleteMessage)
	err := f.v.FailureComplete(int(msg.ProcessorCount), int(msg.ClusterVersion))
	return nil, err
}

type isFailureCompleteHandler struct {
	v *VersionManager
}

func (i *isFailureCompleteHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.IsFailureCompleteMessage)
	complete, err := i.v.IsFailureComplete(int(msg.ClusterVersion))
	if err != nil {
		return nil, err
	}
	return &clustermsgs.IsFailureCompleteResponse{Complete: complete}, nil
}

type versionCompleteHandler struct {
	v *VersionManager
}

func (v *versionCompleteHandler) HandleMessage(messageHolder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	msg := messageHolder.Message.(*clustermsgs.VersionCompleteMessage)
	err := v.v.VersionComplete(int(msg.Version), int(msg.RequiredCompletions), int(msg.CommandId), msg.Doom)
	completionFunc(nil, err)
}

type versionFlushedHandler struct {
	v *VersionManager
}

func (v *versionFlushedHandler) HandleMessage(messageHolder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	msg := messageHolder.Message.(*clustermsgs.VersionFlushedMessage)
	completionFunc(nil, v.v.VersionFlushed(int(msg.NodeId), int(msg.Version), int(msg.ProcessorCount), int(msg.ClusterVersion)))
}
