package common

import "unsafe"

func ByteSliceToStringZeroCopy(bs []byte) string {
	lbs := len(bs)
	if lbs == 0 {
		return ""
	}
	return unsafe.String(&bs[0], lbs)
}

func StringToByteSliceZeroCopy(str string) []byte {
	if str == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(str), len(str))
}
