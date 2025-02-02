package lsm

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafkaencoding"
)

type CompactedTopicIterator struct {
	curr                 common.KV
	iter                 iteration.Iterator
	isCompactedTopicFunc isCompactedTopicFunc
	lastOffsetForKeyFunc lastOffsetForKeyFunc
}

type isCompactedTopicFunc func(topicID int64) (bool, error)

type lastOffsetForKeyFunc func(topicID int64, partitionID int64, key []byte) (int64, bool, error)

func NewCompactedTopicIterator(iter iteration.Iterator, isCompactedTopicFunc isCompactedTopicFunc,
	lastOffsetForKeyFunc lastOffsetForKeyFunc) iteration.Iterator {
	return &CompactedTopicIterator{
		iter:                 iter,
		isCompactedTopicFunc: isCompactedTopicFunc,
		lastOffsetForKeyFunc: lastOffsetForKeyFunc,
	}
}

func (c *CompactedTopicIterator) Next() (bool, common.KV, error) {
	ok, kv, err := c.next()
	if err != nil {
		return false, kv, err
	}
	if !ok {
		return false, kv, nil
	}
	c.curr = kv
	return true, kv, nil
}

func (c *CompactedTopicIterator) next() (bool, common.KV, error) {
	ok, kv, err := c.iter.Next()
	if err != nil {
		return false, common.KV{}, err
	}
	if !ok {
		return false, common.KV{}, nil
	}
	if len(kv.Value) < 2 {
		// Tombstone or end marker 'x'
		return true, kv, nil
	}
	meta, bytes := common.ReadAndRemoveValueMetadata(kv.Value)
	if len(meta) != 2 || len(kv.Key) < 17 {
		return true, kv, nil
	}
	entryType := kv.Key[16]
	if entryType != common.EntryTypeTopicData {
		return true, kv, nil
	}
	topicID := meta[0]
	partitionID := meta[1]
	compacted, err := c.isCompactedTopicFunc(topicID)
	if err != nil {
		return true, common.KV{}, err
	}
	if !compacted {
		return true, kv, nil
	}
	lastOffsetMap := map[string]int64{}

	// Iterate through individual records, and keep results in outBuff
	var outBuff []byte
	baseOffset := int64(binary.BigEndian.Uint64(bytes))
	baseTimeStamp := int64(binary.BigEndian.Uint64(bytes[27:]))
	off := 57
	numRecords := int(binary.BigEndian.Uint32(bytes[off:]))
	off += 4
	outBuff = append(outBuff, bytes[:off]...)
	recordCount := 0
	outBaseTimestamp := int64(0)
	outMaxTimestamp := int64(0)
	for i := 0; i < numRecords; i++ {
		recordStart := off
		recordLength, recordLengthBytesRead := binary.Varint(bytes[off:])
		off += recordLengthBytesRead
		off++ // skip past attributes
		timestampDelta, bytesRead := binary.Varint(bytes[off:])
		recordTimestamp := baseTimeStamp + timestampDelta
		if outBaseTimestamp == 0 {
			outBaseTimestamp = recordTimestamp
		}
		if recordTimestamp > outMaxTimestamp {
			outMaxTimestamp = recordTimestamp
		}
		off += bytesRead
		offsetDelta, bytesRead := binary.Varint(bytes[off:]) // offsetDelta
		off += bytesRead
		keyLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		var key []byte
		skip := false
		if keyLength != -1 {
			ikl := int(keyLength)
			key = bytes[off : off+ikl]
			topicOffset := baseOffset + offsetDelta
			sKey := common.ByteSliceToStringZeroCopy(key)
			latestOffsetForKey, ok := lastOffsetMap[sKey]
			if !ok {
				latestOffsetForKey, ok, err = c.lastOffsetForKeyFunc(topicID, partitionID, key)
				if err != nil {
					return true, common.KV{}, err
				}
				if !ok {
					latestOffsetForKey = -1
				} else {
					lastOffsetMap[sKey] = latestOffsetForKey
				}
			}
			if topicOffset < latestOffsetForKey {
				// skip the KV
				skip = true
			}
			off += ikl
		}
		valueLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ivl := int(valueLength)
		off += ivl
		recordEnd := recordStart + int(recordLength) + recordLengthBytesRead
		if !skip {
			// append the record
			outBuff = append(outBuff, bytes[recordStart:recordEnd]...)
			recordCount++
			if recordTimestamp > outMaxTimestamp {
				outMaxTimestamp = recordTimestamp
			}
		}
		off = recordEnd
	}
	if recordCount == numRecords {
		// No change
		return true, kv, nil
	}
	// At least one removed - update the records. Maybe all records have been removed
	// Note, we must keep BaseOffset and LastOffsetDelta the same, even if we removed records - as these are
	// used for duplicate detection, and if we changed this we could get duplicate records accepted or
	// out of sequence errors on producing.
	// BaseTimestamp and MaxTimestamp are not preserved if records are removed
	kafkaencoding.SetBaseTimestamp(outBuff, outBaseTimestamp)
	kafkaencoding.SetMaxTimestamp(outBuff, outMaxTimestamp)
	kafkaencoding.SetNumRecords(outBuff, recordCount)
	kafkaencoding.SetBatchLength(outBuff, int32(len(outBuff)-12))
	kafkaencoding.CalcAndSetCrc(outBuff)
	outBuff = common.AppendValueMetadata(outBuff, meta...)
	kv.Value = outBuff
	return true, kv, nil
}

func (c *CompactedTopicIterator) Current() common.KV {
	return c.curr
}

func (c *CompactedTopicIterator) Close() {
	c.iter.Close()
}
