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
