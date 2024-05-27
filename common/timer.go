package common

import (
	log "github.com/spirit-labs/tektite/logger"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type TimerHandle struct {
	timer    *time.Timer
	stackSeq uint64
	lock     sync.Mutex
	stopped  bool
}

var activeTimersCount int64

var timerDebug atomic.Bool
var TimerStacks sync.Map
var timerStackSeq uint64

func SetTimerDebug(debug bool) {
	timerDebug.Store(debug)
}

//goland:noinspection GoUnusedExportedFunction
func ActiveTimersCount() int64 {
	return atomic.LoadInt64(&activeTimersCount)
}

// Stop stops the timer without waiting for it to complete if it's already running
func (t *TimerHandle) Stop() {
	t.timer.Stop()
}

func (t *TimerHandle) WaitComplete() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.stopped = true
}

func ScheduleTimer(delay time.Duration, randomise bool, action func()) *TimerHandle {
	atomic.AddInt64(&activeTimersCount, 1)
	if randomise {
		// The first time, we schedule random delay, to stop all timers at startup firing at same time
		delay = time.Duration(rand.Intn(int(delay)))
	}
	var handle TimerHandle
	if timerDebug.Load() {
		stack := GetCurrentStack()
		seq := atomic.AddUint64(&timerStackSeq, 1)
		TimerStacks.Store(seq, stack)
		handle.stackSeq = seq
	}
	handle.timer = time.AfterFunc(delay, func() {
		handle.lock.Lock()
		defer handle.lock.Unlock()
		if handle.stopped {
			return
		}
		if timerDebug.Load() {
			defer TimerStacks.Delete(handle.stackSeq)
		}
		action()
	})
	return &handle
}

//goland:noinspection GoUnusedExportedFunction
func DumpTimerStacks() {
	log.Info("Dumping running timer creation stacks")
	TimerStacks.Range(func(_, stack any) bool {
		log.Info(stack)
		log.Info("===============================================")
		return true
	})
	log.Info("End dump")
}
