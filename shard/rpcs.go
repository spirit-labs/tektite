package shard

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
)

type ApplyChangesRequest struct {
	ShardID  int
	ClusterVersion int
	RegBatch lsm.RegistrationBatch
}

func (a *ApplyChangesRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.ShardID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.ClusterVersion))
	return a.RegBatch.Serialize(buff)
}

func (a *ApplyChangesRequest) Deserialize(buff []byte, offset int) int {
	a.ShardID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	a.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return a.RegBatch.Deserialize(buff, offset)
}

type QueryTablesInRangeRequest struct {
	ShardID  int
	ClusterVersion int
	KeyStart []byte
	KeyEnd   []byte
}

func (a *QueryTablesInRangeRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.ShardID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(a.KeyStart)))
	buff = append(buff, a.KeyStart...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(a.KeyEnd)))
	buff = append(buff, a.KeyEnd...)
	return buff
}

func (a *QueryTablesInRangeRequest) Deserialize(buff []byte, offset int) int {
	a.ShardID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	a.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lks := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	if lks > 0 {
		a.KeyStart = common.ByteSliceCopy(buff[offset : offset+lks])
		offset += lks
	}
	lke := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	if lke > 0 {
		a.KeyEnd = common.ByteSliceCopy(buff[offset : offset+lke])
		offset += lke
	}
	return offset
}
