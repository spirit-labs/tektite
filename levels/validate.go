package levels

import (
	"bytes"
	"github.com/spirit-labs/tektite/asl/errwrap"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sst"
)

// Validate checks the LevelManager is sound - no overlapping keys in L > 0 etc
func (lm *LevelManager) Validate(validateTables bool) error {
	lse := lm.masterRecord.levelSegmentEntries
	if len(lse) == 0 {
		return nil
	}
	for level, segentries := range lse {
		if level == 0 {
			for _, segEntry := range segentries.segmentEntries {
				if err := lm.validateSegment(segEntry, level, validateTables); err != nil {
					return err
				}
			}
		} else {
			for i, segEntry := range segentries.segmentEntries {
				if i > 0 {
					if bytes.Compare(segentries.segmentEntries[i-1].rangeEnd, segEntry.rangeStart) >= 0 {
						return errwrap.Errorf("inconsistency. level %d segment entry %d overlapping range with previous", level, i)
					}
				}
				if i < len(segentries.segmentEntries)-1 {
					if bytes.Compare(segentries.segmentEntries[i+1].rangeStart, segEntry.rangeEnd) <= 0 {
						return errwrap.Errorf("inconsistency. level %d segment entry %d overlapping range with next", level, i)
					}
				}
				if err := lm.validateSegment(segEntry, level, validateTables); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (lm *LevelManager) validateSegment(segEntry segmentEntry, level int, validateTables bool) error {
	seg, err := lm.getSegment(segEntry.segmentID)
	if err != nil {
		return err
	}
	if seg == nil {
		return errwrap.Errorf("segment with id %s not found", string(segEntry.segmentID))
	}
	if len(seg.tableEntries) == 0 {
		return errwrap.Errorf("inconsistency. level %d. segment %v has zero table entries", level, segEntry.segmentID)
	}
	if len(seg.tableEntries) > lm.conf.MaxRegistrySegmentTableEntries {
		return errwrap.Errorf("inconsistency. level %d. segment %v has > %d table entries", level, segEntry.segmentID, lm.conf.MaxRegistrySegmentTableEntries)
	}
	var smallestKey, largestKey []byte
	for i, te := range seg.tableEntries {
		if smallestKey == nil || bytes.Compare(te.RangeStart, smallestKey) < 0 {
			smallestKey = te.RangeStart
		}
		if largestKey == nil || bytes.Compare(te.RangeEnd, largestKey) > 0 {
			largestKey = te.RangeEnd
		}
		// L0 segment has overlap
		if level > 0 {
			if i > 0 {
				prevLastKey := seg.tableEntries[i-1].RangeEnd
				prevLastKeyNoVersion := prevLastKey[:len(prevLastKey)-8]
				currFirstKey := te.RangeStart
				currFirstKeyNoVersion := currFirstKey[:len(currFirstKey)-8]
				if bytes.Compare(prevLastKeyNoVersion, currFirstKeyNoVersion) >= 0 {
					return errwrap.Errorf("inconsistency. segment %v, table entry %d has overlap with previous key1:%s key2:%s",
						segEntry.segmentID, i, string(prevLastKey), string(currFirstKey))
				}
			}
			if i < len(seg.tableEntries)-1 {
				nextFirstKey := seg.tableEntries[i+1].RangeStart
				nextFirstKeyNoVersion := nextFirstKey[:len(nextFirstKey)-8]
				currLastKey := te.RangeEnd
				currLastKeyNoVersion := currLastKey[:len(currLastKey)-8]
				if bytes.Compare(nextFirstKeyNoVersion, currLastKeyNoVersion) <= 0 {
					return errwrap.Errorf("inconsistency. segment %v, table entry %d has overlap with next key1:%v (%s) key2:%v (%s)",
						segEntry.segmentID, i, currLastKey, string(currLastKey), nextFirstKey, string(nextFirstKey))
				}
			}
		}
		if level == lm.getLastLevel() {
			// On the last level, there should be zero deletes
			if te.DeleteRatio != 0 {
				return errwrap.Errorf("last level %d table %s has delete ratio %f", level, string(te.SSTableID),
					te.DeleteRatio)
			}
		}
		if validateTables {
			if err := lm.validateTable(te); err != nil {
				return err
			}
		}
	}
	if !bytes.Equal(smallestKey, segEntry.rangeStart) {
		return errwrap.Errorf("inconsistency. segment %v, smallest table entry does not match RangeStart for the segment", segEntry.segmentID)
	}
	if !bytes.Equal(largestKey, segEntry.rangeEnd) {
		return errwrap.Errorf("inconsistency. segment %v, largest table entry does not match RangeEnd for the segment", segEntry.segmentID)
	}

	return nil
}

func (lm *LevelManager) validateTable(te *TableEntry) error {
	buff, err := lm.objStore.Get(te.SSTableID)
	if err != nil {
		return err
	}
	if buff == nil {
		return errwrap.Errorf("cannot find sstable %v", te.SSTableID)
	}
	table := &sst.SSTable{}
	table.Deserialize(buff, 0)
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
