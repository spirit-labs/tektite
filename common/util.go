package common

import (
	"fmt"
	"unsafe"
)

func ByteSliceCopy(byteSlice []byte) []byte {
	copied := make([]byte, len(byteSlice))
	copy(copied, byteSlice)
	return copied
}

func StrPtr(s string) *string {
	return &s
}

func SafeDerefStringPtr(s *string) string {
	if s == nil {
		return fmt.Sprintf("string ptr is nil")
	}
	return *s
}

func Is64BitArch() bool {
	return unsafe.Sizeof(int(777)) == 8
}
