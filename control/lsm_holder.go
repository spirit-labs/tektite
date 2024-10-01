package control

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"

	"sync"
	"sync/atomic"
)

/*
LsmHolder is a wrapper around the Lsm manager which handles queueing of apply changes requests and persistence to
object storage
*/
type LsmHolder struct {
	lock                   sync.RWMutex
	objStore               objstore.Client
	lsmOpts                lsm.Conf
	clusteredData          *cluster.ClusteredData
	lsmManager             *lsm.Manager
	started                bool
	hasQueuedRegistrations atomic.Bool
	queuedRegistrations    []queuedRegistration
}

type queuedRegistration struct {
	regBatch       lsm.RegistrationBatch
	completionFunc func(error) error
}

func NewLsmHolder(stateUpdaterBucketName string, stateUpdaterKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, lsmOpts lsm.Conf) *LsmHolder {
	clusteredData := cluster.NewClusteredData(stateUpdaterBucketName, stateUpdaterKeyPrefix, dataBucketName, dataKeyPrefix,
		objStoreClient, cluster.ClusteredDataOpts{})
	return &LsmHolder{
		objStore:      objStoreClient,
		lsmOpts:       lsmOpts,
		clusteredData: clusteredData,
	}
}

func (s *LsmHolder) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	metaData, err := s.clusteredData.AcquireData()
	if err != nil {
		return err
	}
	lsmManager := lsm.NewManager(s.objStore, s.maybeRetryApplies, true, false, s.lsmOpts)
	if err := lsmManager.Start(metaData); err != nil {
		return err
	}
	s.lsmManager = lsmManager
	s.started = true
	return nil
}

func (s *LsmHolder) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	return s.stop()
}

func (s *LsmHolder) stop() error {
	if err := s.lsmManager.Stop(); err != nil {
		return err
	}
	s.clusteredData.Stop()
	return nil
}

// ApplyLsmChanges - apply some changes to the LSM structure. Note that this method completes asynchronously as
// L0 registrations will be queued if there is not enough free space
func (s *LsmHolder) ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.checkStarted(); err != nil {
		return completionFunc(err)
	}
	ok, err := s.lsmManager.ApplyChanges(regBatch, false)
	if err != nil {
		return completionFunc(err)
	}
	if ok {
		return s.afterApplyChanges(completionFunc)
	}
	// L0 is full - queue the registration - it will be retried when there is space in L0
	s.hasQueuedRegistrations.Store(true)
	s.queuedRegistrations = append(s.queuedRegistrations, queuedRegistration{
		regBatch:       regBatch,
		completionFunc: completionFunc,
	})
	return nil
}

func (s *LsmHolder) GetTablesForHighestKeyWithPrefix(prefix []byte) ([]sst.SSTableID, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.checkStarted(); err != nil {
		return nil, err
	}
	return s.lsmManager.GetTablesForHighestKeyWithPrefix(prefix)
}

func (s *LsmHolder) afterApplyChanges(completionFunc func(error) error) error {
	// Attempt to store the LSM state
	ok, err := s.clusteredData.StoreData(s.lsmManager.GetMasterRecordBytes())
	if err != nil {
		return completionFunc(err)
	}
	if !ok {
		// Failed to apply changes as epoch has changed
		// Now we need to stop the controller as it's no longer valid
		if err := s.stop(); err != nil {
			log.Warnf("failed to stop controller: %v", err)
		}
		return completionFunc(common.NewTektiteErrorf(common.Unavailable, "lsm holder failed to write as epoch changed"))
	} else {
		return completionFunc(nil)
	}
}

func (s *LsmHolder) maybeRetryApplies() {
	// check atomic outside lock to reduce contention
	if !s.hasQueuedRegistrations.Load() {
		return
	}
	if err := s.maybeRetryApplies0(); err != nil {
		log.Errorf("failed to retry applies: %v", err)
	}
}

func (s *LsmHolder) maybeRetryApplies0() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	pos := 0
	var completionFuncs []func(error) error
	for _, queuedReg := range s.queuedRegistrations {
		ok, err := s.lsmManager.ApplyChanges(queuedReg.regBatch, false)
		if err != nil {
			return queuedReg.completionFunc(err)
		}
		if ok {
			completionFuncs = append(completionFuncs, queuedReg.completionFunc)
			pos++
		} else {
			// full again
			break
		}
	}
	if pos > 0 {
		// We applied one or more queued registrations, now store the state
		ok, err := s.clusteredData.StoreData(s.lsmManager.GetMasterRecordBytes())
		if !ok {
			// Failed to store data as another controller has incremented the epoch - i.e. we are not leader any more
			err = common.NewTektiteErrorf(common.Unavailable, "controller not leader")
			// No longer leader so stop the controller
			if err := s.stop(); err != nil {
				log.Warnf("failed to stop controller: %v", err)
			}
		}
		if err != nil {
			// Send errors back to completions
			for _, cf := range completionFuncs {
				if err := cf(err); err != nil {
					log.Errorf("failed to apply completion function: %v", err)
				}
			}
			return err
		}
		// no errors - remove the elements we successfully applied
		newQueueSize := len(s.queuedRegistrations) - pos
		if newQueueSize > 0 {
			newRegs := make([]queuedRegistration, len(s.queuedRegistrations)-pos)
			copy(newRegs, s.queuedRegistrations[pos:])
			s.queuedRegistrations = newRegs
		} else {
			s.queuedRegistrations = nil
			s.hasQueuedRegistrations.Store(false)
		}
		// Call the completions
		for _, cf := range completionFuncs {
			if err := cf(nil); err != nil {
				log.Errorf("failed to apply completion function: %v", err)
			}
		}
	}
	return nil
}

func (s *LsmHolder) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if err := s.checkStarted(); err != nil {
		return nil, err
	}
	return s.lsmManager.QueryTablesInRange(keyStart, keyEnd)
}

func (s *LsmHolder) checkStarted() error {
	if !s.started {
		return common.NewTektiteErrorf(common.Unavailable, "lsm holder is not started")
	}
	return nil
}
