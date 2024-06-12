package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/proc"
)

func loadOffset(slabID int, partitionID int, mappingID string, store store) (int64, error) {
	partitionHash := proc.CalcPartitionHash(mappingID, uint64(partitionID))
	key := encoding.EncodeEntryPrefix(partitionHash, common.StreamOffsetSequenceSlabID, 40)
	key = encoding.AppendUint64ToBufferBE(key, uint64(slabID))
	key = encoding.AppendUint64ToBufferBE(key, uint64(partitionID))
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

func storeOffset(execCtx StreamExecContext, offset int64, slabID int, version int, mappingID string) {
	// Note the offset is always stored locally to the actual partition it refers to. The zero partition here isn't
	// used, it's just required by the key format.
	partitionHash := proc.CalcPartitionHash(mappingID, uint64(execCtx.PartitionID()))
	key := encoding.EncodeEntryPrefix(partitionHash, common.StreamOffsetSequenceSlabID, 48)
	key = encoding.AppendUint64ToBufferBE(key, uint64(slabID))
	key = encoding.AppendUint64ToBufferBE(key, uint64(execCtx.PartitionID()))
	key = encoding.EncodeVersion(key, uint64(version))
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
}
