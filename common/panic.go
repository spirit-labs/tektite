package common

import (
	"fmt"
	"os"
	"runtime/debug"
)

func TektitePanicHandler() {
	if r := recover(); r != nil {
		fmt.Printf("Panic caught in Tektite: %v\n", r)
		debug.PrintStack()
		os.Exit(1)
	}
}
