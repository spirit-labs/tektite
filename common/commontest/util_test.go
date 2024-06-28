// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commontest

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
)

func TestByteSliceToStringZeroCopy(t *testing.T) {
	b1 := []byte("string1")
	b2 := []byte("")

	s1 := common.ByteSliceToStringZeroCopy(b1)
	require.Equal(t, "string1", s1)
	s2 := common.ByteSliceToStringZeroCopy(b2)
	require.Equal(t, "", s2)
}

func TestStringToByteSliceZeroCopy(t *testing.T) {
	s1 := "string1"
	s2 := ""

	b1 := common.StringToByteSliceZeroCopy(s1)
	require.Equal(t, "string1", string(b1))
	b2 := common.StringToByteSliceZeroCopy(s2)
	require.Equal(t, "", string(b2))
}

func TestIncrementBytesBigEndian(t *testing.T) {
	incAndCheckBytes(t, []byte{0, 0, 0, 0}, []byte{0, 0, 0, 1})
	incAndCheckBytes(t, []byte{0, 0, 0, 1}, []byte{0, 0, 0, 2})
	incAndCheckBytes(t, []byte{0, 0, 0, 254}, []byte{0, 0, 0, 255})
	incAndCheckBytes(t, []byte{0, 0, 0, 255}, []byte{0, 0, 1, 0})
	incAndCheckBytes(t, []byte{0, 0, 1, 0}, []byte{0, 0, 1, 1})
	incAndCheckBytes(t, []byte{0, 0, 1, 1}, []byte{0, 0, 1, 2})
	incAndCheckBytes(t, []byte{0, 0, 1, 254}, []byte{0, 0, 1, 255})
	incAndCheckBytes(t, []byte{0, 0, 1, 255}, []byte{0, 0, 2, 0})
	incAndCheckBytes(t, []byte{0, 0, 2, 0}, []byte{0, 0, 2, 1})
	incAndCheckBytes(t, []byte{255, 255, 255, 254}, []byte{255, 255, 255, 255})
	defer func() {
		err := recover()
		if err != nil {
			require.Equal(t, "cannot increment key - all bits set: [255 255 255 255]", err)
		} else {
			// expected a panic
			t.Fail()
		}
	}()
	incAndCheckBytes(t, []byte{255, 255, 255, 255}, []byte{255, 255, 255, 255})
}

func TestIncrementBytesBigEndianNilBytes(t *testing.T) {
	defer func() {
		err := recover()
		if err != nil {
			require.Equal(t, "cannot increment empty []byte", err)
		} else {
			// expected a panic
			t.Fail()
		}
	}()
	incAndCheckBytes(t, nil, nil)
}

func TestIncrementBytesBigEndianEmptyBytes(t *testing.T) {
	defer func() {
		err := recover()
		if err != nil {
			require.Equal(t, "cannot increment empty []byte", err)
		} else {
			// expected a panic
			t.Fail()
		}
	}()
	incAndCheckBytes(t, []byte{}, nil)
}

func incAndCheckBytes(t *testing.T, bytes []byte, expected []byte) {
	t.Helper()
	res := common.IncrementBytesBigEndian(bytes)
	require.Equal(t, expected, res)
}

func TestGo(t *testing.T) {
	numRoutines := 10
	var lock sync.Mutex
	blockCount := 0
	var wg1 sync.WaitGroup
	wg1.Add(1)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	for i := 0; i < numRoutines; i++ {
		common.Go(func() {
			lock.Lock()
			blockCount++
			if blockCount == numRoutines {
				wg1.Done()
			}
			lock.Unlock()
			wg2.Wait()
		})
	}
	wg1.Wait()
	require.Equal(t, numRoutines, int(common.RunningGRCount()))
	wg2.Done()
}

func TestScheduleTimer(t *testing.T) {
	delay := 10 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(1)
	var lock sync.Mutex
	start := time.Now()
	var fire time.Time
	handle := common.ScheduleTimer(delay, false, func() {
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
	handle := common.ScheduleTimer(delay, false, func() {
		fired.Store(true)
	})
	handle.Stop()
	time.Sleep(2 * delay)
	require.Equal(t, false, fired.Load())
}

func TestTimerStopFromTimerGR(t *testing.T) {
	delay := 50 * time.Millisecond
	var handle *common.TimerHandle
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)
	lock.Lock()
	handle = common.ScheduleTimer(delay, false, func() {
		wg.Wait()
		lock.Lock()
		defer lock.Unlock()
		// Stop the timer from the timer GR
		handle.Stop()
	})
	lock.Unlock()
	wg.Done()
}
