package common

func IncBigEndianBytes(b []byte) []byte {
	b = ByteSliceCopy(b)
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return b
		}
	}
	panic("cannot increment bytes as it would cause overflow")
}
