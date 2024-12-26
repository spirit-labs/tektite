package topicmeta

import (
	"encoding/binary"
	"time"
)

type TopicInfo struct {
	ID                 int
	Name               string
	PartitionCount     int
	RetentionTime      time.Duration
	UseServerTimestamp bool
}

func (t *TopicInfo) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.ID))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(t.Name)))
	buff = append(buff, t.Name...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.PartitionCount))
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.RetentionTime))
	if t.UseServerTimestamp {
		buff = append(buff, 1)
	} else {
		buff = append(buff, 0)
	}
	return buff
}

func (t *TopicInfo) Deserialize(buff []byte, offset int) int {
	t.ID = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	nl := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	t.Name = string(buff[offset : offset+nl])
	offset += nl
	t.PartitionCount = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	t.RetentionTime = time.Duration(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	t.UseServerTimestamp = buff[offset] == 1
	offset++
	return offset
}
