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

package common

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync/atomic"
)

func NewCountDownFuture(initialCount int, completionFunc func(error)) *CountDownFuture {
	return &CountDownFuture{
		count:          int32(initialCount),
		completionFunc: completionFunc,
	}
}

// CountDownFuture calls the completion func when it's count reaches zero
type CountDownFuture struct {
	count          int32
	completionFunc func(error)
	errSent        atomic.Bool
}

// SetCount must not be called after CountDown has been called!
func (pf *CountDownFuture) SetCount(count int) {
	atomic.StoreInt32(&pf.count, int32(count))
}

func (pf *CountDownFuture) CountDown(err error) {
	if err != nil {
		if pf.errSent.CompareAndSwap(false, true) {
			pf.completionFunc(err)
		} else {
			log.Debugf("countdown future complete with additional error %v", err)
		}
		return
	}
	newVal := atomic.AddInt32(&pf.count, -1)
	if newVal < 0 {
		//log.Errorf("countdown future completed too many times curr stack %s prev stack %s",
		//	common.GetCurrentStack(), pf.prevStack)
		panic("countdown future completed too many times")
	}
	if newVal == 0 {
		pf.completionFunc(nil)
	}
}
