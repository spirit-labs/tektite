package kafkaencoding

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/types"
	"hash"
)

func SetBatchHeader(batchBytes []byte, firstOffset int64, lastOffset int64, firstTimestamp types.Timestamp,
	lastTimestamp types.Timestamp, numRecords int, crc hash.Hash32) {
	/*
		baseOffset: int64
		batchLength: int32
		partitionLeaderEpoch: int32
		magic: int8 (current magic value is 2)
		crc: int32
		attributes: int16
		    bit 0~2:
		        0: no compression
		        1: gzip
		        2: snappy
		        3: lz4
		        4: zstd
		    bit 3: timestampType
		    bit 4: isTransactional (0 means not transactional)
		    bit 5: isControlBatch (0 means not a control batch)
		    bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
		    bit 7~15: unused
		lastOffsetDelta: int32
		baseTimestamp: int64
		maxTimestamp: int64
		producerId: int64
		producerEpoch: int16
		baseSequence: int32
		records: [Record]
	*/
	binary.BigEndian.PutUint64(batchBytes, uint64(firstOffset))
	binary.BigEndian.PutUint32(batchBytes[8:], uint32(len(batchBytes)-12)) // len does not include first 2 fields
	batchBytes[16] = 2                                                     // Magic
	if _, err := crc.Write(batchBytes[21:]); err != nil {
		panic(err)
	}
	checksum := crc.Sum32()
	crc.Reset()
	binary.BigEndian.PutUint32(batchBytes[17:], checksum)
	binary.BigEndian.PutUint32(batchBytes[23:], uint32(lastOffset-firstOffset))
	binary.BigEndian.PutUint64(batchBytes[27:], uint64(firstTimestamp.Val))
	binary.BigEndian.PutUint64(batchBytes[35:], uint64(lastTimestamp.Val))
	binary.BigEndian.PutUint32(batchBytes[57:], uint32(numRecords))
}

func AppendToBatch(batchBytes []byte, offset int64, key []byte, hdrs []byte, val []byte, timestamp types.Timestamp,
	firstTimestamp types.Timestamp, firstOffset int64, maxBytes int, first bool) ([]byte, bool) {
	/*
			length: varint
		   attributes: int8
		       bit 0~7: unused
		   timestampDelta: varlong
		   offsetDelta: varint
		   keyLength: varint
		   key: byte[]
		   valueLen: varint
		   value: byte[]
		   Headers => [Header]
	*/

	timestampDelta := timestamp.Val - firstTimestamp.Val
	offsetDelta := offset - firstOffset
	lk := int64(len(key))
	lv := int64(len(val))

	// calculate the length
	l := 1 + varintLength(timestampDelta) + varintLength(offsetDelta) + varintLength(lk) +
		+len(key) + varintLength(lv) + len(val) + len(hdrs)

	if !first && len(batchBytes)+l > maxBytes {
		// If maxBytes exceeded do not append, unless it's the first record  - we always return at least one record
		// even if it exceeds maxSize to enable the consumer to make progress
		return batchBytes, false
	}

	batchBytes = binary.AppendVarint(batchBytes, int64(l))
	batchBytes = append(batchBytes, 0) // attributes
	batchBytes = binary.AppendVarint(batchBytes, timestampDelta)
	batchBytes = binary.AppendVarint(batchBytes, offsetDelta)
	batchBytes = binary.AppendVarint(batchBytes, lk)
	batchBytes = append(batchBytes, key...)
	batchBytes = binary.AppendVarint(batchBytes, lv)
	batchBytes = append(batchBytes, val...)
	batchBytes = append(batchBytes, hdrs...)

	return batchBytes, true
}

func varintLength(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	i := 0
	for ux >= 0x80 {
		ux >>= 7
		i++
	}
	return i + 1
}
