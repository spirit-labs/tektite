package control

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
)

type RegisterL0Request struct {
	ClusterVersion int
	OffsetInfos    []offsets.UpdateWrittenOffsetTopicInfo
	RegEntry       lsm.RegistrationEntry
}

func (r *RegisterL0Request) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.OffsetInfos)))
	for _, topicInfo := range r.OffsetInfos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(topicInfo.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(topicInfo.PartitionInfos)))
		for _, partitionInfo := range topicInfo.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partitionInfo.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partitionInfo.OffsetStart))
			buff = binary.BigEndian.AppendUint32(buff, uint32(partitionInfo.NumOffsets))
		}
	}
	return r.RegEntry.Serialize(buff)
}

func (r *RegisterL0Request) Deserialize(buff []byte, offset int) int {
	r.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	numTopicOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	r.OffsetInfos = make([]offsets.UpdateWrittenOffsetTopicInfo, numTopicOffsets)
	for i := 0; i < numTopicOffsets; i++ {
		var topicOffsets offsets.UpdateWrittenOffsetTopicInfo
		topicOffsets.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		numPartitionOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		topicOffsets.PartitionInfos = make([]offsets.UpdateWrittenOffsetPartitionInfo, numPartitionOffsets)
		for j := 0; j < numPartitionOffsets; j++ {
			var partitionInfo offsets.UpdateWrittenOffsetPartitionInfo
			partitionInfo.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partitionInfo.OffsetStart = int64(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partitionInfo.NumOffsets = int(binary.BigEndian.Uint32(buff[offset:]))
			offset += 4
			topicOffsets.PartitionInfos[j] = partitionInfo
		}
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

type RegisterTableListenerRequest struct {
	ClusterVersion int
	TopicID        int
	PartitionID    int
	Address        string
	ResetSequence  int64
}

func (f *RegisterTableListenerRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.ClusterVersion))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.TopicID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.PartitionID))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(f.Address)))
	buff = append(buff, f.Address...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.ResetSequence))
	return buff
}

func (f *RegisterTableListenerRequest) Deserialize(buff []byte, offset int) int {
	f.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	f.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	f.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	la := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	if la > 0 {
		f.Address = string(buff[offset : offset+la])
	}
	offset += la
	f.ResetSequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type RegisterTableListenerResponse struct {
	LastReadableOffset int64
}

func (g *RegisterTableListenerResponse) Serialize(buff []byte) []byte {
	return binary.BigEndian.AppendUint64(buff, uint64(g.LastReadableOffset))
}

func (g *RegisterTableListenerResponse) Deserialize(buff []byte, offset int) int {
	g.LastReadableOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type GetOffsetsRequest struct {
	ClusterVersion int
	Infos          []offsets.GetOffsetTopicInfo
}

func (g *GetOffsetsRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.ClusterVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Infos)))
	for _, topicInfo := range g.Infos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(topicInfo.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(topicInfo.PartitionInfos)))
		for _, partitionInfo := range topicInfo.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partitionInfo.PartitionID))
			buff = binary.BigEndian.AppendUint32(buff, uint32(partitionInfo.NumOffsets))
		}
	}
	return buff
}

func (g *GetOffsetsRequest) Deserialize(buff []byte, offset int) int {
	g.ClusterVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lInfos := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Infos = make([]offsets.GetOffsetTopicInfo, lInfos)
	for i := 0; i < lInfos; i++ {
		tInfo := &g.Infos[i]
		tInfo.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		pInfos := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		tInfo.PartitionInfos = make([]offsets.GetOffsetPartitionInfo, pInfos)
		for j := 0; j < pInfos; j++ {
			partitionInfo := &tInfo.PartitionInfos[j]
			partitionInfo.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partitionInfo.NumOffsets = int(binary.BigEndian.Uint32(buff[offset:]))
			offset += 4
		}
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

type TableRegisteredNotification struct {
	// TODO - need to include cluster version or epoch in here to screen out zombies
	Sequence int64
	ID       sst.SSTableID
	Infos    []offsets.LastReadableOffsetUpdatedTopicInfo
}

func (r *TableRegisteredNotification) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.Sequence))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.ID)))
	buff = append(buff, r.ID...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.Infos)))
	for _, info := range r.Infos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(info.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(info.PartitionInfos)))
		for _, partInfo := range info.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partInfo.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partInfo.LastReadableOffset))
		}
	}
	return buff
}

func (r *TableRegisteredNotification) Deserialize(buff []byte, offset int) int {
	r.Sequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	id := buff[offset : offset+ln]
	r.ID = make([]byte, len(id))
	copy(r.ID, id)
	offset += ln
	nt := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	r.Infos = make([]offsets.LastReadableOffsetUpdatedTopicInfo, nt)
	for i := 0; i < nt; i++ {
		topicInfo := &r.Infos[i]
		topicInfo.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		np := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		topicInfo.PartitionInfos = make([]offsets.LastReadableOffsetUpdatedPartitionInfo, np)
		for j := 0; j < np; j++ {
			partInfo := &topicInfo.PartitionInfos[j]
			partInfo.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partInfo.LastReadableOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
		}
	}
	return offset
}
