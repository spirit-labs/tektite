package control

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
)

type RegisterL0Request struct {
	ClusterVersion int
	OffsetInfos    []offsets.UpdateWrittenOffsetInfo
	RegEntry       lsm.RegistrationEntry
}

func (r *RegisterL0Request) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.OffsetInfos)))
	for _, offset := range r.OffsetInfos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.TopicID))
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.PartitionID))
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.OffsetStart))
		buff = binary.BigEndian.AppendUint32(buff, uint32(offset.NumOffsets))
	}
	return r.RegEntry.Serialize(buff)
}

func (r *RegisterL0Request) Deserialize(buff []byte, offset int) int {
	r.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	numOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	r.OffsetInfos = make([]offsets.UpdateWrittenOffsetInfo, numOffsets)
	for i := 0; i < numOffsets; i++ {
		var topicOffsets offsets.UpdateWrittenOffsetInfo
		topicOffsets.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		topicOffsets.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		topicOffsets.OffsetStart = int64(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		topicOffsets.NumOffsets = int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		r.OffsetInfos[i] = topicOffsets
	}
	return r.RegEntry.Deserialize(buff, offset)
}

type ApplyChangesRequest struct {
	ClusterVersion int
	RegBatch       lsm.RegistrationBatch
}

func (a *ApplyChangesRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.ClusterVersion))
	return a.RegBatch.Serialize(buff)
}

func (a *ApplyChangesRequest) Deserialize(buff []byte, offset int) int {
	a.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return a.RegBatch.Deserialize(buff, offset)
}

type QueryTablesInRangeRequest struct {
	ClusterVersion int
	KeyStart       []byte
	KeyEnd         []byte
}

func (q *QueryTablesInRangeRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(q.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyStart)))
	buff = append(buff, q.KeyStart...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyEnd)))
	buff = append(buff, q.KeyEnd...)
	return buff
}

func (q *QueryTablesInRangeRequest) Deserialize(buff []byte, offset int) int {
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
	CacheNum       int
	ClusterVersion int
	Infos          []offsets.GetOffsetTopicInfo
}

func (g *GetOffsetsRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.CacheNum))
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
	g.CacheNum = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lInfos := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Infos = make([]offsets.GetOffsetTopicInfo, lInfos)
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

type GetTopicInfoRequest struct {
	ClusterVersion int
	TopicName      string
}

func (g *GetTopicInfoRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.TopicName)))
	buff = append(buff, g.TopicName...)
	return buff
}

func (g *GetTopicInfoRequest) Deserialize(buff []byte, offset int) int {
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.TopicName = string(buff[offset : offset+ln])
	offset += ln
	return offset
}

type GetTopicInfoResponse struct {
	Sequence int
	Info     topicmeta.TopicInfo
}

func (g *GetTopicInfoResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.Sequence))
	return g.Info.Serialize(buff)
}

func (g *GetTopicInfoResponse) Deserialize(buff []byte, offset int) int {
	g.Sequence = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return g.Info.Deserialize(buff, offset)
}

type CreateTopicRequest struct {
	ClusterVersion int
	Info           topicmeta.TopicInfo
}

func (g *CreateTopicRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ClusterVersion))
	return g.Info.Serialize(buff)
}

func (g *CreateTopicRequest) Deserialize(buff []byte, offset int) int {
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return g.Info.Deserialize(buff, offset)
}

type DeleteTopicRequest struct {
	ClusterVersion int
	TopicName      string
}

func (g *DeleteTopicRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.TopicName)))
	buff = append(buff, g.TopicName...)
	return buff
}

func (g *DeleteTopicRequest) Deserialize(buff []byte, offset int) int {
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.TopicName = string(buff[offset : offset+ln])
	offset += ln
	return offset
}
