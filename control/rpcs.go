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
	Sequence      int64
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
	MemberID      int32
	ResetSequence int64
}

func (f *RegisterTableListenerRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.LeaderVersion))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.TopicID))
	buff = binary.BigEndian.AppendUint64(buff, uint64(f.PartitionID))
	buff = binary.BigEndian.AppendUint32(buff, uint32(f.MemberID))
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
	f.MemberID = int32(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
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

type PrePushRequest struct {
	LeaderVersion int
	Infos         []offsets.GenerateOffsetTopicInfo
	EpochInfos    []EpochInfo
}

type EpochInfo struct {
	Key   string
	Epoch int
}

func (g *PrePushRequest) Serialize(buff []byte) []byte {
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
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.EpochInfos)))
	for _, gInfo := range g.EpochInfos {
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(gInfo.Key)))
		buff = append(buff, gInfo.Key...)
		buff = binary.BigEndian.AppendUint64(buff, uint64(gInfo.Epoch))
	}
	return buff
}

func (g *PrePushRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lInfos := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Infos = make([]offsets.GenerateOffsetTopicInfo, lInfos)
	for i := 0; i < lInfos; i++ {
		tInfo := &g.Infos[i]
		tInfo.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		pInfos := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		tInfo.PartitionInfos = make([]offsets.GenerateOffsetPartitionInfo, pInfos)
		for j := 0; j < pInfos; j++ {
			partitionInfo := &tInfo.PartitionInfos[j]
			partitionInfo.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			partitionInfo.NumOffsets = int(binary.BigEndian.Uint32(buff[offset:]))
			offset += 4
		}
	}
	lInfos = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.EpochInfos = make([]EpochInfo, lInfos)
	for i := 0; i < lInfos; i++ {
		groupEpochInfo := &g.EpochInfos[i]
		lid := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		groupEpochInfo.Key = string(buff[offset : offset+lid])
		offset += lid
		groupEpochInfo.Epoch = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
	}
	return offset
}

type PrePushResponse struct {
	Offsets  []offsets.OffsetTopicInfo
	Sequence int64
	EpochsOK []bool
}

func (g *PrePushResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Offsets)))
	for _, offset := range g.Offsets {
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(offset.PartitionInfos)))
		for _, partOffset := range offset.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.Offset))
		}
	}
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.Sequence))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.EpochsOK)))
	for _, ok := range g.EpochsOK {
		if ok {
			buff = append(buff, 1)
		} else {
			buff = append(buff, 0)
		}
	}
	return buff
}

func (g *PrePushResponse) Deserialize(buff []byte, offset int) int {
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
	numEpochsOK := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.EpochsOK = make([]bool, numEpochsOK)
	for i := 0; i < numEpochsOK; i++ {
		if buff[offset] == 1 {
			g.EpochsOK[i] = true
		}
		offset++
	}
	return offset
}

type GetAllTopicInfosRequest struct {
	LeaderVersion int
}

func (g *GetAllTopicInfosRequest) Serialize(buff []byte) []byte {
	return binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
}

func (g *GetAllTopicInfosRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type GetAllTopicInfosResponse struct {
	TopicInfos []topicmeta.TopicInfo
}

func (g *GetAllTopicInfosResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.TopicInfos)))
	for _, topicInfo := range g.TopicInfos {
		buff = topicInfo.Serialize(buff)
	}
	return buff
}

func (g *GetAllTopicInfosResponse) Deserialize(buff []byte, offset int) int {
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.TopicInfos = make([]topicmeta.TopicInfo, ln)
	for i := 0; i < ln; i++ {
		offset = g.TopicInfos[i].Deserialize(buff, offset)
	}
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
	Exists   bool
	Info     topicmeta.TopicInfo
}

func (g *GetTopicInfoResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.Sequence))
	if g.Exists {
		buff = append(buff, 1)
	} else {
		buff = append(buff, 0)
	}
	return g.Info.Serialize(buff)
}

func (g *GetTopicInfoResponse) Deserialize(buff []byte, offset int) int {
	g.Sequence = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	g.Exists = buff[offset] == 1
	offset++
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

type GetGroupCoordinatorInfoRequest struct {
	LeaderVersion int
	GroupID       string
}

func (g *GetGroupCoordinatorInfoRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.GroupID)))
	buff = append(buff, g.GroupID...)
	return buff
}

func (g *GetGroupCoordinatorInfoRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.GroupID = string(buff[offset : offset+ln])
	offset += ln
	return offset
}

type GetGroupCoordinatorInfoResponse struct {
	MemberID   int32
	Address    string
	GroupEpoch int
}

func (g *GetGroupCoordinatorInfoResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(g.MemberID))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Address)))
	buff = append(buff, g.Address...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.GroupEpoch))
	return buff
}

func (g *GetGroupCoordinatorInfoResponse) Deserialize(buff []byte, offset int) int {
	g.MemberID = int32(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Address = string(buff[offset : offset+ln])
	offset += ln
	g.GroupEpoch = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type TablesRegisteredNotification struct {
	Sequence      int64
	LeaderVersion int
	TableIDs      []sst.SSTableID
	Infos         []offsets.OffsetTopicInfo
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

type GenerateSequenceRequest struct {
	LeaderVersion int
	SequenceName  string
}

func (g *GenerateSequenceRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.SequenceName)))
	return append(buff, []byte(g.SequenceName)...)
}

func (g *GenerateSequenceRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.SequenceName = string(buff[offset : offset+ln])
	offset += ln
	return offset
}

type GenerateSequenceResponse struct {
	Sequence int64
}

func (g *GenerateSequenceResponse) Serialize(buff []byte) []byte {
	return binary.BigEndian.AppendUint64(buff, uint64(g.Sequence))
}

func (g *GenerateSequenceResponse) Deserialize(buff []byte, offset int) int {
	g.Sequence = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return offset
}

type GetOffsetInfoRequest struct {
	LeaderVersion       int
	GetOffsetTopicInfos []offsets.GetOffsetTopicInfo
}

func (g *GetOffsetInfoRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.LeaderVersion))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.GetOffsetTopicInfos)))
	for _, topicInfo := range g.GetOffsetTopicInfos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(topicInfo.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(topicInfo.PartitionIDs)))
		for _, partitionID := range topicInfo.PartitionIDs {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partitionID))
		}
	}
	return buff
}

func (g *GetOffsetInfoRequest) Deserialize(buff []byte, offset int) int {
	g.LeaderVersion = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	lInfos := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.GetOffsetTopicInfos = make([]offsets.GetOffsetTopicInfo, lInfos)
	for i := 0; i < lInfos; i++ {
		tInfo := &g.GetOffsetTopicInfos[i]
		tInfo.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		pInfos := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		tInfo.PartitionIDs = make([]int, pInfos)
		for j := 0; j < pInfos; j++ {
			tInfo.PartitionIDs[j] = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
		}
	}
	return offset
}

type GetOffsetInfoResponse struct {
	OffsetInfos []offsets.OffsetTopicInfo
}

func (g *GetOffsetInfoResponse) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.OffsetInfos)))
	for _, offset := range g.OffsetInfos {
		buff = binary.BigEndian.AppendUint64(buff, uint64(offset.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(offset.PartitionInfos)))
		for _, partOffset := range offset.PartitionInfos {
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.PartitionID))
			buff = binary.BigEndian.AppendUint64(buff, uint64(partOffset.Offset))
		}
	}
	return buff
}

func (g *GetOffsetInfoResponse) Deserialize(buff []byte, offset int) int {
	numOffsets := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.OffsetInfos = make([]offsets.OffsetTopicInfo, numOffsets)
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
		g.OffsetInfos[i] = offsets.OffsetTopicInfo{
			TopicID:        topicID,
			PartitionInfos: partInfos,
		}
	}
	return offset
}
