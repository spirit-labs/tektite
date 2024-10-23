package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
	"math"
	"time"
)

type SSTableID []byte

type SSTable struct {
	format           common.DataFormat
	maxKeyLength     uint32
	numEntries       uint32
	numDeletes       uint32
	numPrefixDeletes uint32
	indexOffset      uint32
	creationTime     uint64

	//  data
	//  Initial 5 bytes contain format and metadataOffset
	//  ╭──────┬──────────────╮
	//  │format│metadataOffset│
	//  ├──────┼──────────────┤
	//  │1 byte│ 4 bytes      │
	//  ╰──────┴──────────────╯
	//  Then we have key-value pairs with length prefix
	//  ╭─────────┬────────────────┬────────────┬──────────────────╮
	//  │keyLength│ key            │ valueLength│ value            │
	//  ├─────────┼────────────────┼────────────┼──────────────────┤ ... repeat key-value pairs
	//  │4 bytes  │ keyLength bytes│ 4 bytes    │ valueLength bytes│
	//  ╰─────────┴────────────────┴────────────┴──────────────────╯
	//  Then we have 'index' which maps each key to its offset in the above key-value pairs
	//  SSTable.indexOffset refers to this point where 'index' begins
	//  ╭───────────────────────────────────────────┬──────────╮
	//  │key + (padding if keyLength < maxKeyLength)│ keyOffset│
	//  ├───────────────────────────────────────────┼──────────┤ ... repeat Key and offset pairs
	//  │maxKeyLength bytes                         │ 4 bytes  │
	//  ╰───────────────────────────────────────────┴──────────╯
	data []byte
	// Varint encoded metadata length
	metadataSize int
}

// metadata contains
// ╭────────────┬───────────┬───────────┬──────────────────┬────────────┬─────────────╮
// │maxKeyLength│ numEntries│ numDeletes│ numPrefixDeletes │ indexOffset│ creationTime│
// ├────────────┼───────────┼───────────┼──────────────────┼────────────┼─────────────┤
// │4 bytes     │ 4 bytes   │ 4 bytes   │ 4 bytes          │ 4 bytes    │ 8 bytes     │
// ╰────────────┴───────────┴───────────┴──────────────────┴────────────┴─────────────╯
// - size mentioned are max sizes. variable integer encoding can produce different size results
const maxMetadataSize = 28

func BuildSSTable(format common.DataFormat, buffSizeEstimate int, entriesEstimate int,
	iter iteration.Iterator) (ssTable *SSTable, smallestKey []byte, largestKey []byte, minVersion uint64,
	maxVersion uint64, err error) {

	type indexEntry struct {
		key    []byte
		offset uint32
	}

	indexEntries := make([]indexEntry, 0, entriesEstimate)
	buff := make([]byte, 0, buffSizeEstimate+maxMetadataSize)

	// First byte is the format, then 4 bytes (uint32) which is an offset to the metadata section that we will fill in
	// later
	buff = append(buff, byte(format), 0, 0, 0, 0)

	minVersion = math.MaxUint64
	maxKeyLength := 0
	numEntries := 0
	numDeletes := 0
	numPrefixDeletes := 0
	first := true
	var prevKey []byte
	for {
		v, kv, err := iter.Next()
		if err != nil {
			return nil, nil, nil, 0, 0, err
		}
		if !v {
			break
		}
		// Sanity checks - can maybe remove them or activate them only with a flag for performance
		if prevKey != nil && bytes.Compare(prevKey, kv.Key) >= 0 {
			panic("keys not in order / contains duplicates")
		}
		prevKey = kv.Key
		if first {
			smallestKey = kv.Key
			first = false
		}
		offset := uint32(len(buff))
		lk := len(kv.Key)
		if lk > maxKeyLength {
			maxKeyLength = lk
		}
		buff = appendBytesWithLengthPrefix(buff, kv.Key)
		buff = appendBytesWithLengthPrefix(buff, kv.Value)
		indexEntries = append(indexEntries, indexEntry{
			key:    kv.Key,
			offset: offset,
		})
		numEntries++
		if len(kv.Value) == 0 {
			if len(kv.Key) == 32 { // [partition_hash, slab_id, version]
				numPrefixDeletes++
			}
			numDeletes++
		}
		largestKey = kv.Key
		version := math.MaxUint64 - binary.BigEndian.Uint64(kv.Key[len(kv.Key)-8:]) // last 8 bytes is version

		if version != math.MaxUint64 && version > maxVersion {
			// prefix delete tombstones have a special version = math.MaxUint64 which identifies them in merging_iterator
			// we don't consider this a real version and don't take it into account here
			maxVersion = version
		}
		if version < minVersion {
			minVersion = version
		}
	}

	indexOffset := len(buff)

	for _, entry := range indexEntries {
		buff = append(buff, entry.key...)
		paddingBytes := maxKeyLength - len(entry.key)
		if paddingBytes > 0 {
			if len(buff)+paddingBytes <= cap(buff) {
				// Extend the buffer by slicing - more efficient than allocating a new buffer
				buff = buff[:len(buff)+paddingBytes]
			} else {
				buff = append(buff, make([]byte, paddingBytes)...)
			}
		}
		buff = encoding.AppendUint32ToBufferLE(buff, entry.offset)
	}

	// Now fill in metadata offset
	metadataOffset := len(buff)
	if metadataOffset > math.MaxUint32 {
		return nil, nil, nil, 0, 0, errwrap.New("SSTable too big")
	}

	buff[1] = byte(metadataOffset)
	buff[2] = byte(metadataOffset >> 8)
	buff[3] = byte(metadataOffset >> 16)
	buff[4] = byte(metadataOffset >> 24)

	selfTable := &SSTable{
		format:           format,
		maxKeyLength:     uint32(maxKeyLength),
		numEntries:       uint32(numEntries),
		numDeletes:       uint32(numDeletes),
		numPrefixDeletes: uint32(numPrefixDeletes),
		indexOffset:      uint32(indexOffset),
		creationTime:     uint64(time.Now().UTC().UnixMilli()),
	}

	buff, selfTable.metadataSize = selfTable.serializedMetdata(buff)
	selfTable.data = buff

	return selfTable, smallestKey, largestKey, minVersion, maxVersion, nil
}

func (s *SSTable) serializedMetdata(buff []byte) ([]byte, int) {
	prevLen := len(buff)
	buff = binary.AppendUvarint(buff, uint64(s.maxKeyLength))
	buff = binary.AppendUvarint(buff, uint64(s.numEntries))
	buff = binary.AppendUvarint(buff, uint64(s.numDeletes))
	buff = binary.AppendUvarint(buff, uint64(s.numPrefixDeletes))
	buff = binary.AppendUvarint(buff, uint64(s.indexOffset))
	buff = binary.AppendUvarint(buff, s.creationTime)

	return buff, prevLen - len(buff)
}

func (s *SSTable) Serialize() []byte {
	return s.data
}

func (s *SSTable) Deserialize(buff []byte, offset int) int {
	s.format = common.DataFormat(buff[offset])
	offset++
	var metadataOffset uint32
	metadataOffset, _ = encoding.ReadUint32FromBufferLE(buff, offset)
	offset = int(metadataOffset)

	metadataStartOffset := offset
	var n int
	var value uint64
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.maxKeyLength = uint32(value)
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.numEntries = uint32(value)
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.numDeletes = uint32(value)
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.numPrefixDeletes = uint32(value)
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.indexOffset = uint32(value)
	value, n = binary.Uvarint(buff[offset:])
	offset += n
	s.creationTime = value

	metadataSize := offset - metadataStartOffset

	s.data = buff

	s.metadataSize = metadataSize

	return offset
}

func (s *SSTable) SizeBytes() int {
	return len(s.data)
}

func (s *SSTable) NumEntries() int {
	return int(s.numEntries)
}

func (s *SSTable) NumDeletes() int {
	return int(s.numDeletes)
}

func (s *SSTable) NumPrefixDeletes() int {
	return int(s.numPrefixDeletes)
}

func (s *SSTable) DeleteRatio() float64 {
	return float64(s.numDeletes) / float64(s.numEntries)
}

func (s *SSTable) CreationTime() uint64 {
	return s.creationTime
}

func appendBytesWithLengthPrefix(buff []byte, bytes []byte) []byte {
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	return buff
}

func (s *SSTable) findOffset(key []byte) int {
	indexRecordLen := int(s.maxKeyLength) + 4
	numEntries := int(s.numEntries)
	indexOffset := int(s.indexOffset)
	maxKeyLength := int(s.maxKeyLength)

	// We do a binary search in the index
	low := 0
	outerHighBound := numEntries - 1
	high := outerHighBound
	for low < high {
		middle := low + (high-low)/2
		recordStart := middle*indexRecordLen + indexOffset
		midKey := s.data[recordStart : recordStart+maxKeyLength]
		if bytes.Compare(midKey, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if high == outerHighBound {
		recordStart := high*indexRecordLen + indexOffset
		highKey := s.data[recordStart : recordStart+maxKeyLength]
		if bytes.Compare(highKey, key) < 0 {
			// Didn't find key
			return -1
		}
	}
	recordStart := high*indexRecordLen + indexOffset
	valueStart := recordStart + maxKeyLength
	off, _ := encoding.ReadUint32FromBufferLE(s.data, valueStart)
	return int(off)
}

func CreateSSTableId() string {
	return fmt.Sprintf("sst-%s", uuid.New().String())
}
