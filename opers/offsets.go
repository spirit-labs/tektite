package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
)

func loadOffset(partitionHash []byte, slabID int, store store) (int64, error) {
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID), 24)
	value, err := store.Get(key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	seq, _ := encoding.ReadUint64FromBufferLE(value, 0)
	return int64(seq), nil
}

func storeOffset(execCtx StreamExecContext, offset int64, partitionHash []byte, slabID int, version int) {
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID), 32)
	key = encoding.EncodeVersion(key, uint64(version))
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
}
