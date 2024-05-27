package common

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
)

var runningGRs int64

var grDebug atomic.Bool
var GRStacks sync.Map
var grStackSeq uint64

func SetGRDebug(debug bool) {
	grDebug.Store(debug)
}

// Go spawns a goroutine and keeps track of the number of running GRs.
// We use this count to make sure all goroutines are shutdown cleanly before the server exits.
// In debug mode it also stores creation stacks of all running goroutines. This can be used in debugging
// goroutine leaks
func Go(f func()) {
	atomic.AddInt64(&runningGRs, 1)
	var seq uint64
	if grDebug.Load() {
		stack := GetCurrentStack()
		seq = atomic.AddUint64(&grStackSeq, 1)
		GRStacks.Store(seq, stack)
	}
	go func() {
		if grDebug.Load() {
			defer func() {
				GRStacks.Delete(seq)
			}()
		}
		defer atomic.AddInt64(&runningGRs, -1)
		f()
	}()
}

func RunningGRCount() int64 {
	return atomic.LoadInt64(&runningGRs)
}

//goland:noinspection GoUnusedExportedFunction
func DumpGRStacks() {
	log.Info("Dumping running goroutine creation stacks")
	GRStacks.Range(func(_, stack any) bool {
		log.Info(stack)
		log.Info("===============================================")
		return true
	})
	log.Info("End dump")
}
