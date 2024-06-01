package levels

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"sync"
)

type VersionSetter struct {
	cfg             *conf.Config
	stopped         bool
	lock            sync.Mutex
	stateNotifier   clustmgr.ClusterStateNotifier
	versionConsumer versionConsumer
}

type versionConsumer interface {
	SetClusterVersion(version int)
}

func NewVersionSetter(cfg *conf.Config, stateNotifier clustmgr.ClusterStateNotifier, versionConsumer versionConsumer) *VersionSetter {
	return &VersionSetter{
		cfg:             cfg,
		stateNotifier:   stateNotifier,
		versionConsumer: versionConsumer,
	}
}

func (v *VersionSetter) Start() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.stateNotifier.RegisterStateHandler(v.handleClusterState)
	return nil
}

func (v *VersionSetter) Stop() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.stopped = true
	return nil
}

// HandleClusterState MUST be called before processors get made leaders
func (v *VersionSetter) handleClusterState(cs clustmgr.ClusterState) error {
	if !*v.cfg.ProcessingEnabled {
		// If we're on a LevelManager only cluster, we DO NOT want to set cluster version on the LevelManager from within
		// this cluster. The setting of the cluster version must be done from the processor manager cluster.
		return nil
	}
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.stopped {
		return nil
	}
	// First we calculate whether the current node is considered in the cluster. If not, it could be that we fell
	// out of the cluster (e.g. due to a network partition), in which case we do not want to call setClusterState on the
	// store, as another node could have taken leadership of processors.
	inCluster := false
	for _, gn := range cs.GroupStates[0] {
		if gn.NodeID == *v.cfg.NodeID {
			inCluster = true
			break
		}
	}
	if inCluster {
		v.versionConsumer.SetClusterVersion(cs.Version)
	}
	return nil
}
