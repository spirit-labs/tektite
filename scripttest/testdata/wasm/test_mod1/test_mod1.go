package main

import (
	"fmt"
	"strings"
)

// required for TinyGo to compile to Wasm
func main() {}

//export funcArgsAllTypes
func funcArgsAllTypes(intVal int64, floatVal float64, boolVal int32, decValPtr uint64, strValPtr uint64, bytesValPtr uint64, tsVal int64) (strPtr uint64) {

	bv := boolVal == 1

	decStr := ptrToString(decValPtr)

	str := ptrToString(strValPtr)

	bytes := ptrToBytes(bytesValPtr)

	msg := fmt.Sprintf("%d %.2f %t %s %s %v %d", intVal, floatVal, bv, decStr, str, bytes, tsVal)

	log(fmt.Sprintf("log from test_mod: %s", msg))

	return stringToReturnedPtr(msg)
}

//export funcIntReturn
func funcIntReturn(intVal int64) int64 {
	return intVal + 1
}

//export funcFloatReturn
func funcFloatReturn(floatVal float64) float64 {
	return floatVal + 1
}

//export funcBoolReturn
func funcBoolReturn(boolVal bool) bool {
	return !boolVal
}

//export funcDecimalReturn
func funcDecimalReturn(decValPtr uint64) uint64 {
	decValStr := ptrToString(decValPtr)
	return stringToReturnedPtr("7" + decValStr)
}

//export funcStringReturn
func funcStringReturn(strValPtr uint64) uint64 {
	str := ptrToString(strValPtr)
	return stringToReturnedPtr(strings.ToUpper(str))
}

//export funcBytesReturn
func funcBytesReturn(bytesValPtr uint64) uint64 {
	bytes := ptrToBytes(bytesValPtr)
	if bytes != nil {
		return bytesToReturnedPtr([]byte(strings.ToUpper(string(bytes))))
	} else {
		return bytesToReturnedPtr(nil)
	}
}

//export funcTimestampReturn
func funcTimestampReturn(tsVal int64) int64 {
	return tsVal + 1
}
