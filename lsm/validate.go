package lsm

import (
	"bytes"
	"github.com/spirit-labs/tektite/asl/errwrap"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"
)

// Validate checks the LSM is sound - no overlapping keys in L > 0, keys in each table are sorted, etc.
func (m *Manager) Validate(validateTables bool) error {
	lse := m.masterRecord.levelEntries
	if len(lse) == 0 {
		return nil
	}
	for level, levEntry := range lse {
		if err := m.validateLevelEntry(level, levEntry, validateTables); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) validateLevelEntry(level int, levEntry *levelEntry, validateTables bool) error {
	var smallestKey, largestKey []byte
	var prevRangeEnd []byte
	for _, lte := range levEntry.tableEntries {
		te := lte.Get(levEntry)
		if smallestKey == nil || bytes.Compare(te.RangeStart, smallestKey) < 0 {
			smallestKey = te.RangeStart
		}
		if largestKey == nil || bytes.Compare(te.RangeEnd, largestKey) > 0 {
			largestKey = te.RangeEnd
		}
		// L0 segment has overlap
		if level > 0 {
			// Make sure in order
			if prevRangeEnd != nil && bytes.Compare(prevRangeEnd, te.RangeStart) > 0 {
				return errwrap.Errorf("level %d has unordered entries", level)
			}
			prevRangeEnd = te.RangeEnd
		}
		if validateTables {
			if err := m.validateTable(te); err != nil {
				return err
			}
		}
	}
	if !bytes.Equal(smallestKey, levEntry.rangeStart) {
		return errwrap.Errorf("levEntry.rangeStart does not match table entries")
	}
	if !bytes.Equal(largestKey, levEntry.rangeEnd) {
		return errwrap.Errorf("levEntry.rangeEnd does not match table entries")
	}
	return nil
}

func (m *Manager) validateTable(te *TableEntry) error {
	buff, err := objstore.GetWithTimeout(m.objStore, m.cfg.SSTableBucketName, string(te.SSTableID), objstore.DefaultCallTimeout)
	if err != nil {
		return err
	}
	if buff == nil {
		return errwrap.Errorf("cannot find sstable %v", te.SSTableID)
	}
	table, err := sst.GetSSTableFromBytes(buff)
	if err != nil {
		return err
	}
	iter, err := table.NewIterator(nil, nil)
	if err != nil {
		return err
	}
	first := true
	var prevKey []byte
	for {
		v, curr, err := iter.Next()
		if err != nil {
			return err
		}
		if !v {
			break
		}
		if first {
			if !bytes.Equal(te.RangeStart, curr.Key) {
				return errwrap.Errorf("table %v (%s) range start %s does not match first entry key %s",
					te.SSTableID, string(te.SSTableID), string(te.RangeStart), string(curr.Key))
			}
			first = false
		}
		if prevKey != nil {
			if bytes.Compare(prevKey, curr.Key) >= 0 {
				dumpTable(te.SSTableID, table)
				return errwrap.Errorf("table %v (%s) keys out of order or duplicates %s %s", te.SSTableID, string(te.SSTableID),
					string(prevKey), string(curr.Key))
			}
		}
		prevKey = curr.Key
	}
	if prevKey == nil {
		return errwrap.Errorf("table %v has no entries", te.SSTableID)
	}
	if !bytes.Equal(te.RangeEnd, prevKey) {
		return errwrap.Errorf("table %v range end does not match last entry key", te.SSTableID)
	}
	return nil
}

func dumpTable(tableID []byte, sst *sst.SSTable) {
	iter, err := sst.NewIterator(nil, nil)
	if err != nil {
		panic(err)
	}
	log.Debugf("========== dumping table %v (%s)", tableID, string(tableID))
	for {
		valid, curr, err := iter.Next()
		if err != nil {
			panic(err)
		}
		if !valid {
			break
		}
		log.Debugf("key: %v (%s) val: %v (%s)", curr.Key, string(curr.Key), curr.Value, string(curr.Value))
	}
	log.Debugf("========== end dumping table %v (%s)", tableID, string(tableID))
}
