package opers

import (
	"encoding/binary"
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/proc"
)

func loadOffset(partitionHash []byte, slabID int, processor proc.Processor) (int64, error) {
	key := encoding2.EncodeEntryPrefix(partitionHash, uint64(slabID), 24)
	value, err := processor.Get(key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	seq, _ := encoding2.ReadUint64FromBufferLE(value, 0)
	return int64(seq), nil
}

func storeOffset(execCtx StreamExecContext, offset int64, partitionHash []byte, slabID int, version int) {
	key := encoding2.EncodeEntryPrefix(partitionHash, uint64(slabID), 32)
	key = encoding2.EncodeVersion(key, uint64(version))
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
}
