package coordinator

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestProgressTracker_IsStalled_InitiallyFalse(t *testing.T) {
	log := zap.NewNop()
	pt := NewProgressTracker(time.Hour, log)

	// Immediately after creation, should not be stalled
	if pt.IsStalled() {
		t.Error("IsStalled() = true immediately after creation, want false")
	}
}

func TestProgressTracker_IsStalled_AfterTimeout(t *testing.T) {
	log := zap.NewNop()
	// Use a very short timeout for testing
	pt := NewProgressTracker(10*time.Millisecond, log)

	// Wait for timeout to elapse
	time.Sleep(50 * time.Millisecond)

	if !pt.IsStalled() {
		t.Error("IsStalled() = false after timeout, want true")
	}
}

func TestProgressTracker_RecordProgress_ResetsStall(t *testing.T) {
	log := zap.NewNop()
	pt := NewProgressTracker(50*time.Millisecond, log)

	// Wait most of the timeout
	time.Sleep(40 * time.Millisecond)

	// Record progress to reset the timer
	pt.RecordProgress()

	// Should not be stalled yet
	if pt.IsStalled() {
		t.Error("IsStalled() = true after RecordProgress, want false")
	}

	// Wait for the original timeout from creation (not from RecordProgress)
	// After another 50ms total from RecordProgress, should be stalled
	time.Sleep(60 * time.Millisecond)

	if !pt.IsStalled() {
		t.Error("IsStalled() = false after timeout from RecordProgress, want true")
	}
}

func TestProgressTracker_MultipleRecordProgress(t *testing.T) {
	log := zap.NewNop()
	pt := NewProgressTracker(100*time.Millisecond, log)

	// Keep recording progress multiple times
	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		pt.RecordProgress()
	}

	// Should still not be stalled because we kept recording
	if pt.IsStalled() {
		t.Error("IsStalled() = true with continuous progress, want false")
	}
}

func TestProgressTracker_ZeroTimeout(t *testing.T) {
	log := zap.NewNop()
	pt := NewProgressTracker(0, log)

	// With zero timeout, should immediately be stalled
	// (since any elapsed time > 0)
	time.Sleep(time.Millisecond)
	if !pt.IsStalled() {
		t.Error("IsStalled() = false with zero timeout, want true")
	}
}
