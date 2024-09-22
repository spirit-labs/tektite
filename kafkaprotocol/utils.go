package kafkaprotocol

import "math/bits"

func sizeofUvarint(l int) int {
	if l == 0 {
		return 1
	}
	lzs := bits.LeadingZeros32(uint32(l))
	return ((38-lzs)*0b10010010010010011)>>19 + (lzs >> 5)
}
