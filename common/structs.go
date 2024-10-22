package common

import "encoding/binary"

type MembershipData struct {
	ClusterListenAddress string
	KafkaListenerAddress string
	AZInfo               string
}

func (g *MembershipData) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.ClusterListenAddress)))
	buff = append(buff, g.ClusterListenAddress...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.KafkaListenerAddress)))
	buff = append(buff, g.KafkaListenerAddress...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.AZInfo)))
	buff = append(buff, g.AZInfo...)
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
	g.AZInfo = string(buff[offset : offset+ln])
	offset += ln
	return offset
}
