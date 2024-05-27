/*
From https://github.com/tidwall/spinlock

The MIT License (MIT)

# Copyright (c) 2018 Josh Baker

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package common

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func testLock(threads, n int, l sync.Locker) time.Duration {
	var wg sync.WaitGroup
	wg.Add(threads)

	var count1 int
	var count2 int

	start := time.Now()
	for i := 0; i < threads; i++ {
		go func() {
			for i := 0; i < n; i++ {
				l.Lock()
				count1++
				count2 += 2
				l.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	dur := time.Since(start)
	if count1 != threads*n {
		panic("mismatch")
	}
	if count2 != threads*n*2 {
		panic("mismatch")
	}
	return dur
}

func TestSpinLock(t *testing.T) {
	fmt.Printf("[1] spinlock %4.0fms\n", testLock(1, 1000000, &SpinLock{}).Seconds()*1000)
	fmt.Printf("[1] mutex    %4.0fms\n", testLock(1, 1000000, &sync.Mutex{}).Seconds()*1000)
	fmt.Printf("[4] spinlock %4.0fms\n", testLock(4, 1000000, &SpinLock{}).Seconds()*1000)
	fmt.Printf("[4] mutex    %4.0fms\n", testLock(4, 1000000, &sync.Mutex{}).Seconds()*1000)
	fmt.Printf("[8] spinlock %4.0fms\n", testLock(8, 1000000, &SpinLock{}).Seconds()*1000)
	fmt.Printf("[8] mutex    %4.0fms\n", testLock(8, 1000000, &sync.Mutex{}).Seconds()*1000)
}
