package common

func ByteSliceCopy(byteSlice []byte) []byte {
	copied := make([]byte, len(byteSlice))
	copy(copied, byteSlice)
	return copied
}
