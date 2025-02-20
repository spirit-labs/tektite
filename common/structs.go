package common

import "encoding/binary"

type MembershipData struct {
	ClusterListenAddress string
	KafkaListenerAddress string
	Location             string
}

func (g *MembershipData) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.ClusterListenAddress)))
	buff = append(buff, g.ClusterListenAddress...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.KafkaListenerAddress)))
	buff = append(buff, g.KafkaListenerAddress...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.Location)))
	buff = append(buff, g.Location...)
	return buff
}

func (g *MembershipData) Deserialize(buff []byte, offset int) int {
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.ClusterListenAddress = string(buff[offset : offset+ln])
	offset += ln
	ln = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.KafkaListenerAddress = string(buff[offset : offset+ln])
	offset += ln
	ln = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.Location = string(buff[offset : offset+ln])
	offset += ln
	return offset
}

type DirectWriteRequest struct {
	WriterKey   string
	WriterEpoch int
	KVs         []KV
}

func (o *DirectWriteRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(o.WriterKey)))
	buff = append(buff, o.WriterKey...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(o.WriterEpoch))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(o.KVs)))
	for _, kv := range o.KVs {
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(kv.Key)))
		buff = append(buff, kv.Key...)
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(kv.Value)))
		buff = append(buff, kv.Value...)
	}
	return buff
}

func (o *DirectWriteRequest) Deserialize(buff []byte, offset int) int {
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	o.WriterKey = string(buff[offset : offset+ln])
	offset += ln
	o.WriterEpoch = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	o.KVs = make([]KV, ln)
	for i := 0; i < ln; i++ {
		lk := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		key := ByteSliceCopy(buff[offset : offset+lk])
		offset += lk
		lv := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		value := ByteSliceCopy(buff[offset : offset+lv])
		offset += lv
		o.KVs[i] = KV{
			Key:   key,
			Value: value,
		}
	}
	return offset
}
