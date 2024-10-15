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
	LeaderVersion int
	Sequence int64
	RegEntry      lsm.RegistrationEntry
}

func (r *RegisterL0Request) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.LeaderVersion))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.Sequence))
	return r.RegEntry.Serialize(buff)
}

func (r *RegisterL0Request) Deserialize(buff []byte, offset int) int {
	r.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	r.Sequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return r.RegEntry.Deserialize(buff, offset)
}

type ApplyChangesRequest struct {
	LeaderVersion int
	RegBatch      lsm.RegistrationBatch
}

func (a *ApplyChangesRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(a.LeaderVersion))
	return a.RegBatch.Serialize(buff)
}

func (a *ApplyChangesRequest) Deserialize(buff []byte, offset int) int {
	a.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return a.RegBatch.Deserialize(buff, offset)
}

type QueryTablesInRangeRequest struct {
	LeaderVersion int
	KeyStart      []byte
	KeyEnd        []byte
}

func (q *QueryTablesInRangeRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(q.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyStart)))
	buff = append(buff, q.KeyStart...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(q.KeyEnd)))
	buff = append(buff, q.KeyEnd...)
	return buff
}

func (q *QueryTablesInRangeRequest) Deserialize(buff []byte, offset int) int {
	q.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
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
	LeaderVersion int
	TopicID       int
	PartitionID   int
	Address       string
	ResetSequence int64
}

func (f *RegisterTableListenerRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.LeaderVersion))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.TopicID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.PartitionID))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(f.Address)))
	buff = append(buff, f.Address...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.ResetSequence))
	return buff
}

func (f *RegisterTableListenerRequest) Deserialize(buff []byte, offset int) int {
	f.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
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
	LeaderVersion int
	Infos         []offsets.GetOffsetTopicInfo
}

func (g *GetOffsetsRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
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
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
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
	Offsets []offsets.OffsetTopicInfo
	Sequence int64
}

func (g *GetOffsetsResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Offsets)))
	for _, offset := range g.Offsets {
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(offset.PartitionInfos)))
		for _, partOffset := range offset.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.Offset))
		}
	}
	return binary.BigEndian.AppendUint64(buff, uint64(g.Sequence))
}

func (g *GetOffsetsResponse) Deserialize(buff []byte, offset int) int {
	numOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Offsets = make([]offsets.OffsetTopicInfo, numOffsets)
	for i := 0; i < numOffsets; i++ {
		topicID := int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		numPartOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		partInfos := make([]offsets.OffsetPartitionInfo, numPartOffsets)
		for j := 0; j < numPartOffsets; j++ {
			partitionID := int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			off := int64(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partInfos[j] = offsets.OffsetPartitionInfo{
				PartitionID: partitionID,
				Offset:      off,
			}
		}
		g.Offsets[i] = offsets.OffsetTopicInfo{
			TopicID:        topicID,
			PartitionInfos: partInfos,
		}
	}
	g.Sequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type GetTopicInfoRequest struct {
	LeaderVersion int
	TopicName     string
}

func (g *GetTopicInfoRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.TopicName)))
	buff = append(buff, g.TopicName...)
	return buff
}

func (g *GetTopicInfoRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
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
	LeaderVersion int
	Info          topicmeta.TopicInfo
}

func (g *CreateTopicRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	return g.Info.Serialize(buff)
}

func (g *CreateTopicRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return g.Info.Deserialize(buff, offset)
}

type DeleteTopicRequest struct {
	LeaderVersion int
	TopicName     string
}

func (g *DeleteTopicRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.TopicName)))
	buff = append(buff, g.TopicName...)
	return buff
}

func (g *DeleteTopicRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.TopicName = string(buff[offset : offset+ln])
	offset += ln
	return offset
}

type TablesRegisteredNotification struct {
	Sequence int64
	LeaderVersion int
	TableIDs       []sst.SSTableID
	Infos    []offsets.OffsetTopicInfo
}

func (r *TablesRegisteredNotification) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.Sequence))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.TableIDs)))
	for _, id := range r.TableIDs {
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(id)))
		buff = append(buff, id...)
	}
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(r.Infos)))
	for _, info := range r.Infos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(info.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(info.PartitionInfos)))
		for _, partInfo := range info.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partInfo.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partInfo.Offset))
		}
	}
	return buff
}

func (r *TablesRegisteredNotification) Deserialize(buff []byte, offset int) int {
	r.Sequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	r.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	r.TableIDs = make([]sst.SSTableID, ln)
	for i := 0; i < ln; i++ {
		lid := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		id := buff[offset : offset+lid]
		copied := make([]byte, len(id))
		copy(copied, id)
		r.TableIDs[i] = copied
		offset += lid
	}
	nt := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	r.Infos = make([]offsets.OffsetTopicInfo, nt)
	for i := 0; i < nt; i++ {
		topicInfo := &r.Infos[i]
		topicInfo.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		np := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		topicInfo.PartitionInfos = make([]offsets.OffsetPartitionInfo, np)
		for j := 0; j < np; j++ {
			partInfo := &topicInfo.PartitionInfos[j]
			partInfo.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partInfo.Offset = int64(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
		}
	}
	return offset
}
