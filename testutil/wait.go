package testutil

import (
	"testing"
	"time"
)

// WaitFor polls condition every 100ms until it returns true or timeout expires.
func WaitFor(t *testing.T, timeout time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}
