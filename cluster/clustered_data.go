package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"sync/atomic"
	"time"
)

/*
ClusteredData - provides a mechanism whereby potentially large state can be loaded by a member of the cluster, updated
and then new state stored without fear of another member overwriting the state.

This is used in Tektite for persisting the controller metadata.

A member of the cluster takes ownership of the state by calling `LoadState` which atomically increments an 'epoch' value
in a `StateUpdator` instance and returns the current state, if any.

The owner makes changes to the state then calls `StoreState` to persist it. This stores the state as on object in the
object store external to the `StateUpdator`. Ideally we would store the state in the `StateUpdator` but the current S3
implementation of conditional PUTs requires us to create a new object on each update. If the object is large this
results in a lot of objects and a lot of storage under load.

Before the call to `StoreState` returns a no-op update on `StatePersistor` occurs to read the latest epoch. If this does
not match the epoch the state was stored with thew call returns an error.

The key that we store the state under includes the epoch in it, e.g. `my-data-key-prefix-0000000234`. When data is
loaded, the key with the highest epoch prefix is loaded.
*/
type ClusteredData struct {
	lock           sync.Mutex
	dataBucketName string
	dataKeyPrefix  string
	stateMachine   *StateUpdator
	objStoreClient objstore.Client
	epoch          uint64
	opts           ClusteredDataOpts
	readyState     clusteredDataState
	stopping       atomic.Bool
}

type clusteredDataState int

const (
	clusteredDataStateReady = iota
	clusteredDataStateLoaded
	clusteredDataStateStopped
)

func NewClusteredData(stateMachineBucketName string, stateMachineKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, opts ClusteredDataOpts) *ClusteredData {
	return &ClusteredData{
		objStoreClient: objStoreClient,
		dataBucketName: dataBucketName,
		dataKeyPrefix:  dataKeyPrefix,
		stateMachine: NewStateUpdator(stateMachineBucketName, stateMachineKeyPrefix, objStoreClient,
			StateUpdatorOpts{}),
		opts:       opts,
		readyState: clusteredDataStateReady,
	}
}

type ClusteredDataOpts struct {
	AvailabilityRetryInterval time.Duration
	ObjStoreCallTimeout       time.Duration
}

func (mo *ClusteredDataOpts) setDefaults() {
	if mo.AvailabilityRetryInterval == 0 {
		mo.AvailabilityRetryInterval = DefaultAvailabilityRetryInterval
	}
}

func (m *ClusteredData) LoadData() ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.readyState == clusteredDataStateStopped {
		return nil, errors.New("stopped")
	}
	// Atomically increment the epoch
	buff, err := m.stateMachine.Update(func(state []byte) ([]byte, error) {
		epoch := buffToEpoch(state)
		newState := make([]byte, 8)
		binary.BigEndian.PutUint64(newState, epoch+1)
		return newState, nil
	})
	if err != nil {
		return nil, err
	}
	m.epoch = buffToEpoch(buff)
	prevEpoch := m.epoch - 1
	// Now we try and load the key with the highest epoch - the key might be lower than prevEpoch as no data might
	// have been stored in previous epoch
	var data []byte
	for {
		dataKey := m.createDataKey(prevEpoch)
		data, err = m.getWithRetry(dataKey)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			// no key found - try with next lower epoch
			if prevEpoch == 0 {
				// No data to load
				break
			}
			prevEpoch--
		} else {
			break
		}
	}
	m.readyState = clusteredDataStateLoaded
	return data, nil
}

func (m *ClusteredData) StoreData(data []byte) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.readyState != clusteredDataStateLoaded {
		return false, errors.New("not loaded")
	}
	dataKey := m.createDataKey(m.epoch)
	if err := m.putWithRetry(dataKey, data); err != nil {
		return false, err
	}
	// Then we get latest epoch using a no-op update
	buff, err := m.stateMachine.Update(func(buff []byte) ([]byte, error) {
		return buff, nil
	})
	if err != nil {
		return false, err
	}
	currEpoch := buffToEpoch(buff)
	if currEpoch != m.epoch {
		// Epoch has changed - fail the store
		log.Debug("controller failed to store metadata as epoch has changed")
		return false, nil
	}
	return true, nil
}

func (m *ClusteredData) Stop() {
	// Allow any retry loops to exit
	m.stopping.Store(true)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.readyState = clusteredDataStateStopped
}

func (m *ClusteredData) getWithRetry(dataKey string) ([]byte, error) {
	for {
		if m.stopping.Load() {
			return nil, errors.New("clustered data is stopping")
		}
		data, err := objstore.GetWithTimeout(m.objStoreClient, m.dataBucketName, dataKey, m.opts.ObjStoreCallTimeout)
		if err == nil {
			return data, nil
		}
		if !common.IsUnavailableError(err) {
			return nil, err
		}
		// retry
		time.Sleep(m.opts.AvailabilityRetryInterval)
	}
}

func (m *ClusteredData) putWithRetry(dataKey string, data []byte) error {
	for {
		if m.stopping.Load() {
			return errors.New("clustered data is stopping")
		}
		err := objstore.PutWithTimeout(m.objStoreClient, m.dataBucketName, dataKey, data, m.opts.ObjStoreCallTimeout)
		if err == nil {
			return err
		}
		if !common.IsUnavailableError(err) {
			return err
		}
		// retry
		time.Sleep(m.opts.AvailabilityRetryInterval)
	}
}

func (m *ClusteredData) createDataKey(epoch uint64) string {
	return fmt.Sprintf("%s-%010d", m.dataKeyPrefix, epoch)
}

func buffToEpoch(buff []byte) uint64 {
	var epoch uint64
	if len(buff) > 0 {
		epoch = binary.BigEndian.Uint64(buff)
	}
	return epoch
}
