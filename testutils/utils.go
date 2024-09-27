//go:build !release

package testutils

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func WaitUntil(t *testing.T, predicate Predicate) {
	t.Helper()
	WaitUntilWithDur(t, predicate, 10*time.Second)
}

func WaitUntilWithDur(t *testing.T, predicate Predicate, timeout time.Duration) {
	t.Helper()
	complete, err := WaitUntilWithError(predicate, timeout, time.Millisecond)
	require.NoError(t, err)
	require.True(t, complete, "timed out waiting for predicate")
}

type Predicate func() (bool, error)

func WaitUntilWithError(predicate Predicate, timeout time.Duration, sleepTime time.Duration) (bool, error) {
	start := time.Now()
	for {
		complete, err := predicate()
		if err != nil {
			return false, err
		}
		if complete {
			return true, nil
		}
		time.Sleep(sleepTime)
		if time.Since(start) >= timeout {
			return false, nil
		}
	}
}
