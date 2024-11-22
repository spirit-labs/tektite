package agent

import "encoding/binary"

func uint32ToBytes(num uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)
	return buf
}
