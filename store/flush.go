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

package store

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	sst2 "github.com/spirit-labs/tektite/sst"
	"sync"
	"sync/atomic"
	"time"
)

type ssTableInfo struct {
	ssTable     *sst2.SSTable
	largestKey  []byte
	smallestKey []byte
}

type flushQueueEntry struct {
	memtable             *mem.Memtable
	tableInfo            *ssTableInfo
	lastCompletedVersion int64
	minVersion           uint64
	maxVersion           uint64
}

type FmtEntry struct {
	SstableID sst2.SSTableID
	HasWrites bool
	Halted    bool
}

var FlushedMemTableSSTableMapping sync.Map

func (s *Store) flushLoop() {
	for range s.flushChan {
		if err := s.maybeFlushSSTables(); err != nil {
			log.Errorf("failure in store flush loop %+v", err)
		}
	}
	s.stopWg.Done()
}

func (s *Store) triggerFlushLoop() {
	// Write to channel without blocking
	select {
	case s.flushChan <- struct{}{}:
	default:
	}
}

func (s *Store) buildSSTable(entry *flushQueueEntry) error {
	log.Debugf("store %d in buildSSTables", s.conf.NodeID)
	iter := entry.memtable.NewIterator(nil, nil)
	valid, err := iter.IsValid()
	if err != nil {
		return err
	}
	if !valid {
		return err
	}
	ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst2.BuildSSTable(s.conf.TableFormat,
		int(s.conf.MemtableMaxSizeBytes), 8*1024, iter)
	if err != nil {
		return err
	}
	entry.tableInfo = &ssTableInfo{
		largestKey:  largestKey,
		smallestKey: smallestKey,
		ssTable:     ssTable,
	}
	entry.minVersion = minVersion
	entry.maxVersion = maxVersion
	if minVersion == 0 && maxVersion == 0 {
		log.Warnf("building sstable from memtable %s min and max version is zero", entry.memtable.Uuid)
	}
	return nil
}

func (s *Store) ClearUnflushedData() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		return
	}
	// Set the clearing flag - this enables any flush of sstables that is in a retry loop (e.g. due to cluster version
	// being too low) to exit and for us to proceed
	s.clearing.Store(true)
	s.clearFlushedLock.Lock()
	defer s.clearFlushedLock.Unlock()
	s.mtFlushQueueLock.Lock()
	defer s.mtFlushQueueLock.Unlock()
	log.Debugf("node %d ClearUnflushedData", s.conf.NodeID)
	s.createNewMemtable()
	s.mtQueue = nil
	s.queueFull.Store(false)
	s.clearing.Store(false)
}

func (s *Store) maybeFlushSSTables() error {

	log.Debugf("node %d - maybeFlushSSTables", s.conf.NodeID)

	// We use a different lock exclusively for disabling flush as it must lock the entire flush process so we can
	// guarantee that no flush occurs after disabling flush and before re-enabling it.
	s.clearFlushedLock.Lock()
	defer s.clearFlushedLock.Unlock()

	s.mtFlushQueueLock.Lock()
	if len(s.mtQueue) == 0 {
		log.Debugf("node %d nothing in queue so not doing anything", s.conf.NodeID)
		s.mtFlushQueueLock.Unlock()
		// nothing to do
		return nil
	}

	// The new flushed version is the last completed version of the last entry in the queue
	flushedVersion := s.mtQueue[len(s.mtQueue)-1].lastCompletedVersion
	entriesToPush := make([]*flushQueueEntry, len(s.mtQueue))
	// Make a copy
	copy(entriesToPush, s.mtQueue)

	// Release the lock now as we don't want to hold it when pushing - this can take time and retry and we don't
	// want to block writers that result in a new table being added to the queue

	s.mtFlushQueueLock.Unlock()

	log.Debugf("there are %d tables that will be flushed", len(entriesToPush))

	// actually push the mem-tables to cloud and register them with the level-manager
	for _, entry := range entriesToPush {
		if entry.memtable.HasWrites() { // empty memtables are used for flush
			if err := s.buildSSTable(entry); err != nil {
				return err
			}

			// Push and register the SSTable
			id := []byte(fmt.Sprintf("sst-%s", uuid.New().String()))
			tableBytes := entry.tableInfo.ssTable.Serialize()
			for {
				if !s.started.Load() {
					return nil
				}
				start := time.Now()
				if err := s.cloudStoreClient.Put(id, tableBytes); err != nil {
					if common.IsUnavailableError(err) {
						// Transient availability error - retry
						log.Warnf("cloud store is unavailable, will retry: %v", err)
						time.Sleep(s.conf.SSTablePushRetryDelay)
						continue
					}
					return err
				}
				log.Debugf("store %d added sstable with id %v for memtable %s to cloud store", s.conf.NodeID, id,
					entry.memtable.Uuid)
				log.Debugf("objstore put took %d ms", time.Now().Sub(start).Milliseconds())
				break
			}
			if err := s.tableCache.AddSSTable(id, entry.tableInfo.ssTable); err != nil {
				return err
			}
			log.Debugf("store %d added sstable with id %v for memtable %s to table cache", s.conf.NodeID, id,
				entry.memtable.Uuid)
			for {
				if !s.started.Load() || s.clearing.Load() {
					return nil
				}
				clusterVersion := s.getClusterVersion()
				// register with level-manager
				log.Debugf("node %d calling RegisterL0Tables", s.conf.NodeID)
				start := time.Now()
				if err := s.levelManagerClient.RegisterL0Tables(levels.RegistrationBatch{
					ClusterName:    s.conf.ClusterName,
					ClusterVersion: clusterVersion,
					Registrations: []levels.RegistrationEntry{{
						Level:            0,
						TableID:          id,
						MinVersion:       uint64(entry.minVersion),
						MaxVersion:       uint64(entry.maxVersion),
						KeyStart:         entry.tableInfo.smallestKey,
						KeyEnd:           entry.tableInfo.largestKey,
						DeleteRatio:      entry.tableInfo.ssTable.DeleteRatio(),
						AddedTime:        entry.tableInfo.ssTable.CreationTime(),
						NumEntries:       uint64(entry.tableInfo.ssTable.NumEntries()),
						TableSize:        uint64(entry.tableInfo.ssTable.SizeBytes()),
						NumPrefixDeletes: uint32(entry.tableInfo.ssTable.NumPrefixDeletes()),
					}},
					DeRegistrations: nil,
				}); err != nil {
					var tektiteErr errors.TektiteError
					if errors.As(err, &tektiteErr) {
						if tektiteErr.Code == errors.Unavailable || tektiteErr.Code == errors.LevelManagerNotLeaderNode {
							if !s.started.Load() || s.clearing.Load() {
								// Allow to break out of the loop if stopped or clearing
								return nil
							}
							// Transient availability error - retry
							log.Warnf("store failed to register new ss-table with level manager, will retry: %v", err)
							time.Sleep(s.conf.SSTableRegisterRetryDelay)
							continue
						}
					}
					return err
				}
				log.Debugf("RegisterL0Tables took %d ms", time.Now().Sub(start).Milliseconds())

				FlushedMemTableSSTableMapping.Store(entry.memtable.Uuid, FmtEntry{
					SstableID: id,
					HasWrites: entry.memtable.HasWrites(),
				})
				log.Debugf("node %d registered memtable %s with levelManager sstableid %v- max version %d",
					s.conf.NodeID, entry.memtable.Uuid, id, entry.maxVersion)
				log.Debug("store flushed sstable ok")
				break
			}
		} else {
			log.Debugf("node %d entry to flush has no writes", s.conf.NodeID)
		}
		log.Debugf("store %d calling flushed callback for memtable %s", s.conf.NodeID, entry.memtable.Uuid)
		if err := entry.memtable.Flushed(nil); err != nil {
			return err
		}
	}
	log.Debugf("store %d in maybeFlushSSTables done flushed version %d", s.conf.NodeID, flushedVersion)

	// Report the flushed version
	if s.versionFlushedHandler != nil && flushedVersion != -1 {
		s.versionFlushedHandler(int(flushedVersion))
	}
	atomic.StoreInt64(&s.lastLocalFlushedVersion, flushedVersion)

	// remove the flushed entries
	lep := len(entriesToPush)
	if lep > 0 {
		s.mtFlushQueueLock.Lock()
		s.mtQueue = s.mtQueue[lep:]
		if len(s.mtQueue) < s.conf.MemtableFlushQueueMaxSize {
			s.queueFull.Store(false)
		}
		s.mtFlushQueueLock.Unlock()
	}
	return nil
}

func (s *Store) GetFlushedVersion() int64 {
	return atomic.LoadInt64(&s.lastLocalFlushedVersion)
}
