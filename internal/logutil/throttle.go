// Package logutil provides logging utilities for periodic loops.
package logutil

import (
	"time"

	"go.uber.org/zap"
)

// FailureLogger throttles repeated failure logs in periodic loops.
//
// On first failure: logs immediately at Warn level.
// While failing: repeats the warning every [interval] (default 1 minute).
// On recovery: logs once at Info level.
//
// This avoids both extremes — logging every tick (noisy) and logging only
// the first failure (silent during prolonged outages).
type FailureLogger struct {
	log      *zap.Logger
	interval time.Duration
	lastLog  time.Time
	failing  bool
}

// NewFailureLogger creates a FailureLogger that repeats warnings at the given interval.
func NewFailureLogger(log *zap.Logger, interval time.Duration) *FailureLogger {
	return &FailureLogger{
		log:      log,
		interval: interval,
	}
}

// Fail records a failure. Logs at Warn level on first failure and then
// at most once per interval while the failure persists.
func (f *FailureLogger) Fail(msg string, fields ...zap.Field) {
	now := time.Now()
	if !f.failing || now.Sub(f.lastLog) >= f.interval {
		f.log.Warn(msg, fields...)
		f.lastLog = now
	}
	f.failing = true
}

// OK records a success. If the previous state was failing, logs recovery
// at Info level.
func (f *FailureLogger) OK() {
	if f.failing {
		f.log.Info("recovered")
		f.failing = false
	}
}
