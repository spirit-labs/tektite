package main

import (
	"strings"
)

// this is required for TinyGo to compile to Wasm
func main() {}

//export repeatString
func repeatString(strPtr uint64, times uint64) uint64 {
	str := ptrToString(strPtr)
	var builder strings.Builder
	for i := 0; i < int(times); i++ {
		builder.WriteString(str)
	}
	return stringToReturnedPtr(builder.String())
}
