package levels

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/types"
	"sync"
)

type LevelManagerService struct {
	lock                 sync.RWMutex
	cfg                  *conf.Config
	cloudStore           objstore.Client
	tabCache             *tabcache.Cache
	commandBatchIngestor commandBatchIngestor
	levelManager         *LevelManager
	leaderNodeProvider   LeaderNodeProvider
	stopped              bool
	clustStateNotifier   clustmgr.ClusterStateNotifier
}

const (
	ApplyChangesCommand byte = iota + 10
	RegisterPrefixRetentionsCommand
	RegisterDeadVersionRangeCommand
	StoreLastFlushedVersionCommand
)

var CommandColumnTypes = []types.ColumnType{types.ColumnTypeBytes}
var CommandSchema = evbatch.NewEventSchema([]string{"command"}, CommandColumnTypes)

type LeaderNodeProvider interface {
	GetLeaderNode(processorID int) (int, error)
}

func NewLevelManagerService(clustStateNotifier clustmgr.ClusterStateNotifier, cfg *conf.Config, cloudStore objstore.Client,
	tabCache *tabcache.Cache, ingestor commandBatchIngestor, leaderNodeProvider LeaderNodeProvider) *LevelManagerService {
	return &LevelManagerService{
		cfg:                  cfg,
		cloudStore:           cloudStore,
		tabCache:             tabCache,
		commandBatchIngestor: ingestor,
		clustStateNotifier:   clustStateNotifier,
		leaderNodeProvider:   leaderNodeProvider,
	}
}

func (l *LevelManagerService) Start() error {
	return nil
}

func (l *LevelManagerService) Stop() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.levelManager != nil {
		return l.levelManager.Stop()
	}
	l.stopped = true
	return nil
}

func (l *LevelManagerService) GetLevelManager() *LevelManager {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.levelManager
}

func (l *LevelManagerService) HandleClusterState(cs clustmgr.ClusterState) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.stopped {
		return nil
	}
	if l.isLeader(&cs) {
		if l.levelManager == nil {
			log.Debugf("levelmanager starting on node %d", l.cfg.NodeID)
			l.levelManager = NewLevelManager(l.cfg, l.cloudStore, l.tabCache, l.commandBatchIngestor,
				true, false, true)
			return l.levelManager.Start(false)
		}
	} else {
		if l.levelManager != nil {
			if err := l.levelManager.Stop(); err != nil {
				return err
			}
			l.levelManager = nil
		}
	}
	return nil
}

func (l *LevelManagerService) ActivateLevelManager() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.stopped {
		return nil
	}
	if l.levelManager != nil {
		return l.levelManager.Activate()
	}
	return nil
}

func (l *LevelManagerService) isLeader(cs *clustmgr.ClusterState) bool {
	if len(cs.GroupStates) == 0 {
		return false
	}
	// The last group is a special one, only use for level-manager
	groupState := cs.GroupStates[*l.cfg.ProcessorCount]
	leaderNode := -1
	for _, groupNode := range groupState {
		if groupNode.Leader {
			leaderNode = groupNode.NodeID
			break
		}
	}
	return leaderNode == *l.cfg.NodeID
}

func (l *LevelManagerService) GetMapper() (*LevelManager, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.stopped {
		return nil, errors.New("LevelManager service is stopped")
	}
	return l.levelManager, nil
}

func (l *LevelManagerService) Shutdown() (bool, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.stopped {
		return false, errors.New("LevelManager service is stopped")
	}
	if l.levelManager != nil {
		// Flush all level manager changes to object store
		if _, _, err := l.levelManager.Flush(true); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (l *LevelManagerService) AddFlushedCallback(callback func(err error)) error {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.stopped {
		return errors.New("LevelManager service is stopped")
	}
	if l.levelManager != nil {
		l.levelManager.AddFlushedCallback(callback)
	}
	return nil
}
