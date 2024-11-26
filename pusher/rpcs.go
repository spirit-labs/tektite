package pusher

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
)

type DirectProduceRequest struct {
	TopicProduceRequests []TopicProduceRequest
}

type TopicProduceRequest struct {
	TopicID                  int
	PartitionProduceRequests []PartitionProduceRequest
}

type PartitionProduceRequest struct {
	PartitionID int
	Batch       []byte
}

func (d *DirectProduceRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(d.TopicProduceRequests)))
	for _, req := range d.TopicProduceRequests {
		buff = binary.BigEndian.AppendUint64(buff, uint64(req.TopicID))
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(req.PartitionProduceRequests)))
		for _, preq := range req.PartitionProduceRequests {
			buff = binary.BigEndian.AppendUint64(buff, uint64(preq.PartitionID))
			buff = binary.BigEndian.AppendUint32(buff, uint32(len(preq.Batch)))
			buff = append(buff, preq.Batch...)
		}
	}
	return buff
}

func (d *DirectProduceRequest) Deserialize(buff []byte, offset int) int {
	ltr := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	d.TopicProduceRequests = make([]TopicProduceRequest, ltr)
	for i := 0; i < ltr; i++ {
		tpr := TopicProduceRequest{}
		tpr.TopicID = int(binary.BigEndian.Uint64(buff[offset:]))
		offset += 8
		lpr := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		tpr.PartitionProduceRequests = make([]PartitionProduceRequest, lpr)
		for j := 0; j < lpr; j++ {
			ppr := PartitionProduceRequest{}
			ppr.PartitionID = int(binary.BigEndian.Uint64(buff[offset:]))
			offset += 8
			lb := int(binary.BigEndian.Uint32(buff[offset:]))
			offset += 4
			ppr.Batch = common.ByteSliceCopy(buff[offset : offset+lb])
			offset += lb
			tpr.PartitionProduceRequests[j] = ppr
		}
		d.TopicProduceRequests[i] = tpr
	}
	return offset
}
