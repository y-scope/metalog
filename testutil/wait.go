package testutil

import (
	"testing"
	"time"
)

// WaitFor polls condition every 100ms until it returns true or timeout expires.
func WaitFor(t *testing.T, timeout time.Duration, msg string, condition func() bool) {
	t.Helper()
	if !waitFor(timeout, condition) {
		t.Fatalf("timed out waiting: %s", msg)
	}
}

// waitFor polls condition every 100ms and returns true if it succeeds
// before the timeout, false otherwise.
func waitFor(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
