package logutil

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestFailureLogger_FirstFailureLogsImmediately(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	fl := NewFailureLogger(zap.New(core), time.Minute)

	fl.Fail("something broke")

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log, got %d", logs.Len())
	}
	if logs.All()[0].Message != "something broke" {
		t.Errorf("message = %q, want %q", logs.All()[0].Message, "something broke")
	}
}

func TestFailureLogger_ThrottlesRepeatedFailures(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	fl := NewFailureLogger(zap.New(core), time.Minute)

	fl.Fail("broken")
	fl.Fail("broken")
	fl.Fail("broken")

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log (throttled), got %d", logs.Len())
	}
}

func TestFailureLogger_RepeatsAfterInterval(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	fl := NewFailureLogger(zap.New(core), 10*time.Millisecond)

	fl.Fail("broken")
	time.Sleep(15 * time.Millisecond)
	fl.Fail("broken")

	if logs.Len() != 2 {
		t.Fatalf("expected 2 logs (after interval), got %d", logs.Len())
	}
}

func TestFailureLogger_RecoveryLogsOnce(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	fl := NewFailureLogger(zap.New(core), time.Minute)

	fl.Fail("broken")
	fl.OK()
	fl.OK() // second OK should not log

	// 1 Warn + 1 Info
	if logs.Len() != 2 {
		t.Fatalf("expected 2 logs (fail + recover), got %d", logs.Len())
	}
	if logs.All()[1].Message != "recovered" {
		t.Errorf("recovery message = %q, want %q", logs.All()[1].Message, "recovered")
	}
}

func TestFailureLogger_NoLogOnOKWithoutFailure(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	fl := NewFailureLogger(zap.New(core), time.Minute)

	fl.OK()

	if logs.Len() != 0 {
		t.Fatalf("expected 0 logs, got %d", logs.Len())
	}
}
