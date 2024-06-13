package store

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	sst2 "github.com/spirit-labs/tektite/sst"
	"sync"
)

type Iterator struct {
	s          *Store
	lock       *sync.RWMutex
	rangeStart []byte
	rangeEnd   []byte
	lastKey    []byte
	iter       *iteration.MergingIterator
}

func (s *Store) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	log.Debugf("creating store iterator from keystart %v to keyend %v", keyStart, keyEnd)

	if !s.started.Load() {
		return nil, errors.NewTektiteErrorf(errors.Unavailable, "store not started")
	}

	// We create a merging iterator which merges from a set of potentially overlapping Memtables/SSTables in order
	// from newest to oldest

	s.lock.RLock()
	s.mtFlushQueueLock.Lock()

	// First we add the current memtable
	iters := []iteration.Iterator{s.memTable.NewIterator(keyStart, keyEnd)}

	// Then we add each memtable in the flush queue, in order from newest to oldest
	for i := len(s.mtQueue) - 1; i >= 0; i-- {
		entry := s.mtQueue[i]
		iters = append(iters, entry.memtable.NewIterator(keyStart, keyEnd))
	}

	s.mtFlushQueueLock.Unlock()
	s.lock.RUnlock()
	ssTableIters, err := s.createSSTableIterators(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	iters = append(iters, ssTableIters...)
	si, err := s.newStoreIterator(keyStart, keyEnd, iters, &s.lock, highestVersion, preserveTombstones)
	if err != nil {
		return nil, err
	}
	s.iterators.Store(si, nil)
	return si, nil
}

func (s *Store) newStoreIterator(rangeStart []byte, rangeEnd []byte, iters []iteration.Iterator, lock *sync.RWMutex,
	highestVersion uint64, preserveTombstones bool) (*Iterator, error) {
	mi, err := iteration.NewMergingIterator(iters, preserveTombstones, highestVersion)
	if err != nil {
		return nil, err
	}
	si := &Iterator{
		s:          s,
		lock:       lock,
		rangeStart: rangeStart,
		rangeEnd:   rangeEnd,
		iter:       mi,
	}
	return si, nil
}

func (s *Store) createSSTableIterators(keyStart []byte, keyEnd []byte) ([]iteration.Iterator, error) {
	ids, deadVersions, err := s.levelManagerClient.GetTableIDsForRange(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	log.Debugf("creating sstable iters for keystart %v keyend %v", keyStart, keyEnd)
	// Then we add each flushed SSTable with overlapping keys from the levelManagerClient. It's possible we might have the included
	// the same keys twice in a memtable from the flush queue which has been already flushed and one from the LSM
	// This is ok as he later one (the sstable) will just be ignored in the iterator.
	var iters []iteration.Iterator
	for i, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			log.Debugf("using sstable %v in iterator [%d, 0] for key start %v", nonOverLapIDs[0], i, keyStart)
			lazy, err := sst2.NewLazySSTableIterator(nonOverLapIDs[0], s.tableCache, keyStart, keyEnd, s.iterFactoryFunc())
			if err != nil {
				return nil, err
			}
			iters = append(iters, lazy)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				log.Debugf("using sstable %v in iterator [%d, %d] for key start %v", nonOverlapID, i, j, keyStart)
				lazy, err := sst2.NewLazySSTableIterator(nonOverlapID, s.tableCache, keyStart, keyEnd, s.iterFactoryFunc())
				if err != nil {
					return nil, err
				}
				itersInChain[j] = lazy
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	if len(deadVersions) > 0 {
		log.Debugf("dead versions are: %v", deadVersions)
		// We have dead versions that we need to remove on this side. This occurs after failure when we rollback to
		// the last good snapshot and we need to filter out any versions between that and when recovery completed.
		for i, iter := range iters {
			iters[i] = levels.NewRemoveDeadVersionsIterator(iter, deadVersions)
		}
	}
	return iters, nil
}

func (s *Store) iterFactoryFunc() func(sst *sst2.SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	return func(sst *sst2.SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
		sstIter, err := sst.NewIterator(keyStart, keyEnd)
		if err != nil {
			return nil, err
		}
		return sstIter, nil
	}
}

func (s *Store) removeIterator(iter *Iterator) {
	s.iterators.Delete(iter)
}

func (s *Store) NumIterators() int {
	cnt := 0
	s.iterators.Range(func(_, _ any) bool {
		cnt++
		return true
	})
	return cnt
}

func (s *Store) updateIterators(mt *mem.Memtable) error {
	var err error
	s.iterators.Range(func(key, value any) bool {
		iter := key.(*Iterator) //nolint:forcetypeassert
		rs, re, lastKey := iter.getRange()
		if lastKey != nil {
			// lastKey includes the version
			lk := lastKey[:len(lastKey)-8]
			rs = common.IncrementBytesBigEndian(lk)
			rs = append(rs, lastKey[len(lastKey)-8:]...) // put version back on
		}
		mtIter := mt.NewIterator(rs, re)
		if err = iter.addNewMemtableIterator(mtIter); err != nil {
			return false
		}
		return true
	})
	return err
}

func (s *Iterator) getRange() ([]byte, []byte, []byte) {
	return s.rangeStart, s.rangeEnd, s.lastKey
}

func (s *Iterator) addNewMemtableIterator(iter iteration.Iterator) error {
	return s.iter.PrependIterator(iter)
}

func (s *Iterator) Close() {
	s.s.removeIterator(s)
}

func (s *Iterator) Current() common.KV {
	s.lock.RLock()
	defer s.lock.RUnlock()
	curr := s.iter.Current()
	s.lastKey = curr.Key
	return curr
}

func (s *Iterator) Next() error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.iter.Next()
}

func (s *Iterator) IsValid() (bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.iter.IsValid()
}

// FindKey - Useful method for debugging
func (s *Store) FindKey(key []byte) string {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.mtFlushQueueLock.Lock()
	defer s.mtFlushQueueLock.Unlock()

	keyEnd := common.IncrementBytesBigEndian(key)
	iter := s.memTable.NewIterator(key, keyEnd)
	valid, err := iter.IsValid()
	if err != nil {
		panic(err)
	}
	if valid {
		return fmt.Sprintf("node %d key %v found in live memtable", s.conf.NodeID, key)
	}
	for _, entry := range s.mtQueue {
		mt := entry.memtable
		iter := mt.NewIterator(key, keyEnd)
		valid, err := iter.IsValid()
		if err != nil {
			panic(err)
		}
		if valid {
			return fmt.Sprintf("node %d key %v found in memtable %s in flush queue", s.conf.NodeID, key,
				mt.Uuid)
		}
	}
	otids, _, err := s.levelManagerClient.GetTableIDsForRange(key, keyEnd)
	if err != nil {
		panic(err)
	}
	if len(otids) > 0 {
		if len(otids) != 1 {
			panic("unexpected number of tables")
		}
		tids := otids[0]
		if len(tids) != 1 {
			panic("unexpected number of tables")
		}
		return fmt.Sprintf("key %v found in sstable %v", key, tids[0])
	}
	return fmt.Sprintf("node %d key not found in store", s.conf.NodeID)
}
