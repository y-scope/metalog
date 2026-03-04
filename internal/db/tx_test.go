package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestWithDeadlockRetry_Success(t *testing.T) {
	calls := 0
	err := WithDeadlockRetry(context.Background(), 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("called %d times, want 1", calls)
	}
}

func TestWithDeadlockRetry_RetriesOnDeadlock(t *testing.T) {
	calls := 0
	err := WithDeadlockRetry(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return &mysql.MySQLError{Number: 1213, Message: "Deadlock"}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("called %d times, want 3", calls)
	}
}

func TestWithDeadlockRetry_ExhaustsRetries(t *testing.T) {
	calls := 0
	err := WithDeadlockRetry(context.Background(), 2, func() error {
		calls++
		return &mysql.MySQLError{Number: 1213, Message: "Deadlock"}
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !IsDeadlock(err) {
		t.Errorf("expected deadlock error, got: %v", err)
	}
	// maxRetries=2: attempts 0, 1, 2 => 3 calls total
	if calls != 3 {
		t.Errorf("called %d times, want 3", calls)
	}
}

func TestWithDeadlockRetry_NonDeadlockError(t *testing.T) {
	nonDeadlock := fmt.Errorf("connection refused")
	calls := 0
	err := WithDeadlockRetry(context.Background(), 5, func() error {
		calls++
		return nonDeadlock
	})
	if err != nonDeadlock {
		t.Errorf("expected original error, got: %v", err)
	}
	if calls != 1 {
		t.Errorf("should not retry non-deadlock errors, called %d times", calls)
	}
}

func TestWithDeadlockRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := WithDeadlockRetry(ctx, 5, func() error {
		return &mysql.MySQLError{Number: 1213, Message: "Deadlock"}
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}
