package common

import "encoding/binary"

type MembershipData struct {
	ListenAddress string
	AZInfo        string
}

func (g *MembershipData) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.ListenAddress)))
	buff = append(buff, g.ListenAddress...)
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(g.AZInfo)))
	buff = append(buff, g.AZInfo...)
	return buff
}

func (g *MembershipData) Deserialize(buff []byte, offset int) int {
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.ListenAddress = string(buff[offset : offset+ln])
	offset += ln
	ln = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	g.AZInfo = string(buff[offset : offset+ln])
	offset += ln
	return offset
}
