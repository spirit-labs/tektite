package common

import (
	"fmt"
	"runtime"
)

func GetAllStacks() string {
	buf := make([]byte, 1<<16)
	l := runtime.Stack(buf, true)
	s := string(buf[:l])
	return s
}

func GetCurrentStack() string {
	buf := make([]byte, 1<<16)
	l := runtime.Stack(buf, false)
	return string(buf[:l])
}

func DumpStacks() {
	fmt.Println(GetAllStacks())
}
