package kafkaencoding

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"hash/crc32"
)

func SetBatchHeader(batchBytes []byte, firstOffset int64, lastOffset int64, firstTimestamp types.Timestamp,
	lastTimestamp types.Timestamp, numRecords int) {
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
	SetBaseOffset(batchBytes, firstOffset)
	SetBatchLength(batchBytes, int32(len(batchBytes)-12))
	batchBytes[16] = 2 // Magic
	SetLastOffsetDelta(batchBytes, int32(lastOffset-firstOffset))
	SetBaseTimestamp(batchBytes, firstTimestamp.Val)
	SetMaxTimestamp(batchBytes, lastTimestamp.Val)
	SetNumRecords(batchBytes, numRecords)
	CalcAndSetCrc(batchBytes)
}

func CalcAndSetCrc(batchBytes []byte) {
	crc := crc32.Checksum(batchBytes[21:], crcTable)
	binary.BigEndian.PutUint32(batchBytes[17:], crc)
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func SetBaseTimestamp(records []byte, baseTimestamp int64) {
	binary.BigEndian.PutUint64(records[27:], uint64(baseTimestamp))
}

func SetMaxTimestamp(records []byte, maxTimestamp int64) {
	binary.BigEndian.PutUint64(records[35:], uint64(maxTimestamp))
}

func AppendToBatch(batchBytes []byte, offsetDelta int64, key []byte, hdrs []byte, val []byte, timestamp types.Timestamp,
	firstTimestamp types.Timestamp, maxBytes int, first bool) ([]byte, bool) {
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
	lk := int64(len(key))
	lv := int64(len(val))

	// calculate the length
	l := 1 + VarintLength(timestampDelta) + VarintLength(offsetDelta) + VarintLength(lk) +
		+len(key) + VarintLength(lv) + len(val) + len(hdrs)

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

func VarintLength(x int64) int {
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

func NumRecords(records []byte) int {
	return int(binary.BigEndian.Uint32(records[57:]))
}

func SetNumRecords(records []byte, numRecords int) {
	binary.BigEndian.PutUint32(records[57:], uint32(numRecords))
}

func BaseOffset(records []byte) int64 {
	return int64(binary.BigEndian.Uint64(records))
}

func SetBaseOffset(records []byte, val int64) {
	binary.BigEndian.PutUint64(records, uint64(val))
}

func BatchLength(records []byte) int32 {
	return int32(binary.BigEndian.Uint32(records[8:]))
}

func SetBatchLength(records []byte, batchLen int32) {
	binary.BigEndian.PutUint32(records[8:], uint32(batchLen))
}

func ProducerID(records []byte) int64 {
	return int64(binary.BigEndian.Uint64(records[43:]))
}

func BaseSequence(records []byte) int32 {
	return int32(binary.BigEndian.Uint32(records[53:]))
}

func LastOffsetDelta(records []byte) int32 {
	return int32(binary.BigEndian.Uint32(records[23:]))
}

func SetLastOffsetDelta(records []byte, lastOffsetDelta int32) {
	binary.BigEndian.PutUint32(records[23:], uint32(lastOffsetDelta))
}

func BaseTimestamp(records []byte) int64 {
	return int64(binary.BigEndian.Uint64(records[27:]))
}

func MaxTimestamp(records []byte) int64 {
	return int64(binary.BigEndian.Uint64(records[35:]))
}

func CompressionType(records []byte) byte {
	return records[22] & 0x07
}

func SetCompressionType(records []byte, compressionType byte) {
	records[22] &= 0xF8            // Clear the first 3 bits
	records[22] |= compressionType // Set the first 3 bits
}

func SetCrc(records []byte, crc uint32) {
	binary.BigEndian.PutUint32(records[17:], crc)
}

func GetCrc(records []byte) uint32 {
	return binary.BigEndian.Uint32(records[17:])
}

type KafkaError struct {
	ErrorCode int
	ErrorMsg  string
}

func (k KafkaError) Error() string {
	return fmt.Sprintf("KafkaProtocolError ErrCode:%d %s", k.ErrorCode, k.ErrorMsg)
}

func ErrorCodeForError(err error, unavailableErrorCode int16) int16 {
	if err == nil {
		return int16(kafkaprotocol.ErrorCodeNone)
	}
	var kerr KafkaError
	if errwrap.As(err, &kerr) {
		log.Warn(err)
		return int16(kerr.ErrorCode)
	} else if common.IsUnavailableError(err) {
		log.Warn(err)
		return unavailableErrorCode
	} else {
		log.Error(err)
		return int16(kafkaprotocol.ErrorCodeUnknownServerError)
	}
}

type RawKafkaMessage struct {
	Key       []byte
	Value     []byte
	Headers   []byte
	Offset    int64
	Timestamp int64
}

func (r *RawKafkaMessage) Size() int {
	return len(r.Key) + len(r.Value) + len(r.Headers) + 16
}

func BatchToRawMessages(bytes []byte) []RawKafkaMessage {
	baseOffset := BaseOffset(bytes)
	baseTimeStamp := BaseTimestamp(bytes)
	off := 57
	numRecords := NumRecords(bytes)
	msgs := make([]RawKafkaMessage, numRecords)
	off += 4
	for i := 0; i < numRecords; i++ {
		recordLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		recordStart := off
		off++ // skip past attributes
		timestampDelta, bytesRead := binary.Varint(bytes[off:])
		recordTimestamp := baseTimeStamp + timestampDelta
		off += bytesRead
		offsetDelta, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		keyLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		var key []byte
		if keyLength != -1 {
			ikl := int(keyLength)
			key = bytes[off : off+ikl]
			off += ikl
		}
		valueLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ivl := int(valueLength)
		value := bytes[off : off+ivl]
		off += ivl
		headersEnd := recordStart + int(recordLength)
		headers := bytes[off:headersEnd]
		msgs[i] = RawKafkaMessage{
			Key:       key,
			Value:     value,
			Headers:   headers,
			Timestamp: recordTimestamp,
			Offset:    baseOffset + offsetDelta,
		}
		off = headersEnd
	}
	return msgs
}
