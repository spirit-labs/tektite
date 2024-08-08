package common

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScheduleTimer(t *testing.T) {
	delay := 10 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(1)
	var lock sync.Mutex
	start := time.Now()
	var fire time.Time
	handle := ScheduleTimer(delay, false, func() {
		lock.Lock()
		defer lock.Unlock()
		fire = time.Now()
		wg.Done()
	})
	wg.Wait()
	lock.Lock()
	defer lock.Unlock()
	require.GreaterOrEqual(t, fire.Sub(start), delay)
	handle.Stop()
}

func TestTimerStopBeforeFire(t *testing.T) {
	delay := 50 * time.Millisecond
	var fired atomic.Bool
	handle := ScheduleTimer(delay, false, func() {
		fired.Store(true)
	})
	handle.Stop()
	time.Sleep(2 * delay)
	require.Equal(t, false, fired.Load())
}

func TestTimerStopFromTimerGR(t *testing.T) {
	delay := 50 * time.Millisecond
	var handle *TimerHandle
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)
	lock.Lock()
	handle = ScheduleTimer(delay, false, func() {
		wg.Wait()
		lock.Lock()
		defer lock.Unlock()
		// Stop the timer from the timer GR
		handle.Stop()
	})
	lock.Unlock()
	wg.Done()
}
