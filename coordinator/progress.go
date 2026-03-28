// Package coordinator provides shared types for coordinator-level functionality
// including [ProgressTracker] for stall detection and [TableRegistration] for
// runtime table provisioning via gRPC.
package coordinator

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ProgressTracker detects stalled coordinator loops by tracking last-progress timestamps.
type ProgressTracker struct {
	log               *zap.Logger
	lastProgressNanos atomic.Int64
	stallTimeout      time.Duration
}

// NewProgressTracker creates a ProgressTracker.
func NewProgressTracker(stallTimeout time.Duration, log *zap.Logger) *ProgressTracker {
	pt := &ProgressTracker{stallTimeout: stallTimeout, log: log}
	pt.RecordProgress()
	return pt
}

// RecordProgress updates the last-progress timestamp to now.
func (pt *ProgressTracker) RecordProgress() {
	pt.lastProgressNanos.Store(time.Now().UnixNano())
}

// IsStalled returns true if no progress has been recorded for longer than the timeout.
func (pt *ProgressTracker) IsStalled() bool {
	last := pt.lastProgressNanos.Load()
	return time.Since(time.Unix(0, last)) > pt.stallTimeout
}
