package cluster

import (
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"sync/atomic"
	"time"
)

type StateUpdater struct {
	lock           sync.Mutex
	stateBucket    string
	stateKey       string
	objStoreClient objstore.Client
	opts           StateUpdaterOpts
	started        bool
	stopping       atomic.Bool
	initialised    bool
	latestState    []byte
	retryCount     int64
}

const DefaultObjStoreCallTimeout = 5 * time.Second
const DefaultAvailabilityRetryInterval = 1 * time.Second

type StateUpdaterOpts struct {
	ObjStoreCallTimeout       time.Duration
	AvailabilityRetryInterval time.Duration
}

func NewStateUpdater(stateBucket string, stateKey string, objStoreClient objstore.Client,
	opts StateUpdaterOpts) *StateUpdater {
	if opts.ObjStoreCallTimeout == 0 {
		opts.ObjStoreCallTimeout = DefaultObjStoreCallTimeout
	}
	if opts.AvailabilityRetryInterval == 0 {
		opts.AvailabilityRetryInterval = DefaultAvailabilityRetryInterval
	}
	sm := &StateUpdater{
		stateBucket:    stateBucket,
		stateKey:       stateKey,
		objStoreClient: objStoreClient,
		opts:           opts,
	}
	return sm
}

func (s *StateUpdater) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	s.started = true
}

func (s *StateUpdater) SetStopping() {
	s.stopping.Store(true)
}

func (s *StateUpdater) Stop() {
	s.stopping.Store(true)
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	s.started = false
}

func (s *StateUpdater) GetState() ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.latestState, nil
}

// FetchLatestState fetches the latest state by performing a no-op update.
func (s *StateUpdater) FetchLatestState() ([]byte, error) {
	return s.Update(func(buffer []byte) ([]byte, error) {
		return buffer, nil
	})
}

func (s *StateUpdater) Update(updateFunc func(state []byte) ([]byte, error)) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for !s.stopping.Load() {
		state, err := s.update(updateFunc)
		if err == nil {
			s.latestState = state
			return state, nil
		}
		if !common.IsUnavailableError(err) {
			return nil, err
		}
		// Retry
		log.Warnf("state machine update failed, will retry: %v", err)
		atomic.AddInt64(&s.retryCount, 1)
		time.Sleep(s.opts.AvailabilityRetryInterval)
	}
	return nil, errors.New("updater is stopping")
}

func (s *StateUpdater) update(updateFunc func(state []byte) ([]byte, error)) ([]byte, error) {
	for {
		// Get the latest state
		info, exists, err := objstore.GetObjectInfoWithTimeout(s.objStoreClient, s.stateBucket, s.stateKey, s.opts.ObjStoreCallTimeout)
		if err != nil {
			return nil, err
		}
		var state []byte
		if exists {
			state, err = objstore.GetWithTimeout(s.objStoreClient, s.stateBucket, s.stateKey, s.opts.ObjStoreCallTimeout)
			if err != nil {
				return nil, err
			}
		}
		newState, err := updateFunc(state)
		if err != nil {
			return nil, err
		}
		var ok bool
		if exists {
			ok, err = objstore.PutIfMatchingEtagWithTimeout(s.objStoreClient, s.stateBucket, s.stateKey, newState,
				info.Etag, s.opts.ObjStoreCallTimeout)
		} else {
			ok, _, err = objstore.PutIfNotExistsWithTimeout(s.objStoreClient, s.stateBucket, s.stateKey, newState,
				s.opts.ObjStoreCallTimeout)
		}
		if err != nil {
			return nil, err
		}
		if ok {
			return newState, nil
		}
	}
}
