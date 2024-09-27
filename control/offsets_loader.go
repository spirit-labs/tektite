package control

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"time"
)

type OffsetsLoader struct {
	lsm             *LsmHolder
	partitionHashes *parthash.PartitionHashes
	objStore        objstore.Client
	dataBucketName  string
}

func NewOffsetsLoader(lsm *LsmHolder, objStore objstore.Client, dataBucketName string) (*OffsetsLoader, error) {
	// We don't cache as loader only loads once
	partHashes, err := parthash.NewPartitionHashes(0)
	if err != nil {
		return nil, err
	}
	return &OffsetsLoader{
		lsm:             lsm,
		objStore:        objStore,
		dataBucketName:  dataBucketName,
		partitionHashes: partHashes,
	}, nil
}

const objectStoreCallTimeout = 5 * time.Second

func (o *OffsetsLoader) LoadHighestOffsetForPartition(topicID int, partitionID int) (int64, error) {
	prefix, err := o.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return 0, err
	}
	tables, err := o.lsm.GetTablesForHighestKeyWithPrefix(prefix)
	if err != nil {
		return 0, err
	}
	for _, tableID := range tables {
		buff, err := objstore.GetWithTimeout(o.objStore, o.dataBucketName, string(tableID), objectStoreCallTimeout)
		if err != nil {
			return 0, err
		}
		if len(buff) == 0 {
			return 0, errors.Errorf("ssttable %s not found", tableID)
		}
		var table sst.SSTable
		table.Deserialize(buff, 0)
		iter, err := table.NewIterator(prefix, nil)
		if err != nil {
			return 0, err
		}
		var offset int64 = -1
		for {
			ok, kv, err := iter.Next()
			if err != nil {
				return 0, err
			}
			if !ok {
				break
			}
			if bytes.Equal(prefix, kv.Key[:len(prefix)]) {
				baseOffset, _ := encoding.KeyDecodeInt(kv.Key, 16)
				numRecords := binary.BigEndian.Uint32(kv.Value[57:])
				offset = baseOffset + int64(numRecords) - 1
			} else {
				break
			}
		}
		return offset, nil
	}
	return -1, nil
}
