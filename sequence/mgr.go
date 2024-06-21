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

package sequence

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/lock"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

type Manager interface {
	GetNextID(sequenceName string, batchSize int) (int, error)
}

func NewSequenceManager(objStore objstore.Client, sequencesObjectName string, lockManager lock.Manager,
	unavailabilityRetryDelay time.Duration) Manager {
	if sequencesObjectName == "" {
		panic("sequencesObjectName must be specified")
	}
	if unavailabilityRetryDelay == 0 {
		panic("unavailabilityRetryDelay must be > 0")
	}

	return &mgr{
		objStore:                 objStore,
		lockManager:              lockManager,
		sequencesObjectName:      sequencesObjectName,
		availSequencesMap:        map[string]*availSequences{},
		unavailabilityRetryDelay: unavailabilityRetryDelay,
	}
}

const (
	sequencesLockName = "sequences"
)

type mgr struct {
	lock                     sync.Mutex
	objStore                 objstore.Client
	lockManager              lock.Manager
	sequencesObjectName      string
	availSequencesMap        map[string]*availSequences
	unavailabilityRetryDelay time.Duration
}

type availSequences struct {
	startSeq int // inclusive
	endSeq   int // exclusive
}

func (m *mgr) GetNextID(sequenceName string, batchSize int) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	avail, ok := m.availSequencesMap[sequenceName]
	if ok {
		seq := avail.startSeq
		avail.startSeq++
		if avail.startSeq == avail.endSeq {
			// No more cached sequences
			delete(m.availSequencesMap, sequenceName)
		}
		return seq, nil
	}
	// We need to get more sequences from the object store

	// First we need to get a cluster wide exclusive lock
	if err := m.getLock(); err != nil {
		return 0, err
	}
	defer func() {
		if err := m.releaseLock(); err != nil {
			log.Errorf("failed to release sequences lock %v", err)
		}
	}()

	var bytes []byte
	for {
		// Get batch of sequences from the object store
		var err error
		bytes, err = m.objStore.Get([]byte(m.sequencesObjectName))
		if err != nil {
			if common.IsUnavailableError(err) {
				// Retry on temporary unavailability
				log.Warnf("sequence manager unable to contact cloud store to load sequence batch, will retry. %v", err)
				time.Sleep(m.unavailabilityRetryDelay)
				continue
			}
			return 0, err
		}
		break
	}
	// deserialize the sequences
	sequences := map[string]int{}
	if bytes != nil {
		numSequences, offset := encoding.ReadUint64FromBufferLE(bytes, 0)
		for i := 0; i < int(numSequences); i++ {
			var sequenceName string
			sequenceName, offset = encoding.ReadStringFromBufferLE(bytes, offset)
			var sequence uint64
			sequence, offset = encoding.ReadUint64FromBufferLE(bytes, offset)
			sequences[sequenceName] = int(sequence)
		}
	}

	nextSeq := sequences[sequenceName]
	writeSeq := nextSeq + batchSize
	sequences[sequenceName] = writeSeq

	// serialize
	bytes = make([]byte, 0, 256)
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(len(sequences)))
	for sequenceName, seq := range sequences {
		bytes = encoding.AppendStringToBufferLE(bytes, sequenceName)
		bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(seq))
	}
	// and push the sequences back to the object store
	for {
		if err := m.objStore.Put([]byte(m.sequencesObjectName), bytes); err != nil {
			if common.IsUnavailableError(err) {
				log.Warnf("sequence manager unable to contact cloud store to store sequence batch, will retry. %v", err)
				time.Sleep(m.unavailabilityRetryDelay)
				continue
			}
			return 0, err
		}
		break
	}

	avail = &availSequences{
		startSeq: nextSeq + 1,
		endSeq:   nextSeq + batchSize,
	}
	m.availSequencesMap[sequenceName] = avail
	return nextSeq, nil
}

func (m *mgr) getLock() error {
	for {
		ok, err := m.lockManager.GetLock(sequencesLockName)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Lock is already held - retry after delay
		log.Warnf("lock %s already held, will retry", sequencesLockName)
		time.Sleep(m.unavailabilityRetryDelay)
		continue
	}
}

func (m *mgr) releaseLock() error {
	_, err := m.lockManager.ReleaseLock(sequencesLockName)
	return err
}
