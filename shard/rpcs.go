package shard

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
)

type ApplyChangesRequest struct {
	ShardID        int
	ClusterVersion int
	RegBatch       lsm.RegistrationBatch
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
	ShardID        int
	ClusterVersion int
	KeyStart       []byte
	KeyEnd         []byte
}

func (q *QueryTablesInRangeRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(q.ShardID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(q.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyStart)))
	buff = append(buff, q.KeyStart...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyEnd)))
	buff = append(buff, q.KeyEnd...)
	return buff
}

func (q *QueryTablesInRangeRequest) Deserialize(buff []byte, offset int) int {
	q.ShardID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	q.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lks := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	if lks > 0 {
		q.KeyStart = common.ByteSliceCopy(buff[offset : offset+lks])
		offset += lks
	}
	lke := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	if lke > 0 {
		q.KeyEnd = common.ByteSliceCopy(buff[offset : offset+lke])
		offset += lke
	}
	return offset
}

type GetOffsetsRequest struct {
	ShardID        int
	ClusterVersion int
	Infos          []GetOffsetInfo
}

func (g *GetOffsetsRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ShardID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Infos)))
	for _, info := range g.Infos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(info.TopicID))
		buff = binary.BigEndian.AppendUint64(buff, uint64(info.PartitionID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(info.NumOffsets))
	}
	return buff
}

func (g *GetOffsetsRequest) Deserialize(buff []byte, offset int) int {
	g.ShardID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lInfos := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Infos = make([]GetOffsetInfo, lInfos)
	for i := 0; i < lInfos; i++ {
		g.Infos[i].TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		g.Infos[i].PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		g.Infos[i].NumOffsets = int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
	}
	return offset
}

type GetOffsetsResponse struct {
	Offsets []int64
}

func (g *GetOffsetsResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Offsets)))
	for _, offset := range g.Offsets {
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset))
	}
	return buff
}

func (g *GetOffsetsResponse) Deserialize(buff []byte, offset int) int {
	numOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Offsets = make([]int64, numOffsets)
	for i := 0; i < numOffsets; i++ {
		g.Offsets[i] = int64(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
	}
	return offset
}
