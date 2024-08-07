package common

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/asl/errwrap"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"time"
)

func CallWithRetryOnUnavailable[R any](action func() (R, error), stopped func() bool) (R, error) {
	return CallWithRetryOnUnavailableWithTimeout(action, stopped, 10*time.Millisecond, time.Duration(math.MaxInt64), "failed to execute retryable operation")
}

func CallWithRetryOnUnavailableWithTimeout[R any](action func() (R, error), stopped func() bool, delay time.Duration,
	timeout time.Duration, retryMessage string) (R, error) {
	start := arista.NanoTime()
	var zeroResult R
	for !stopped() {
		r, err := action()
		if err != nil {
			var perr TektiteError
			if errwrap.As(err, &perr) {
				if perr.Code == Unavailable {
					if retryMessage != "" {
						log.Warnf(fmt.Sprintf("%s: %v", retryMessage, err))
					}
					time.Sleep(delay)
					if arista.NanoTime()-start >= uint64(timeout) {
						return zeroResult, errwrap.New("timed out waiting to execute action")
					}
					continue
				}
			}
			return zeroResult, err
		}
		return r, nil
	}
	return zeroResult, errwrap.New("retry terminated as closed")
}
