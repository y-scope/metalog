package db

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/y-scope/metalog/config"
)

// WithTx executes fn inside a database transaction. If fn returns an error,
// the transaction is rolled back; otherwise it is committed.
func WithTx(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// WithDeadlockRetry wraps fn in a retry loop that retries on MySQL deadlocks
// (1213) and lock wait timeouts (1205). Both are transient lock contention
// errors that can resolve on retry.
// Uses random jitter in [DefaultDeadlockMinBackoff, DefaultDeadlockMaxBackoff] on each retry.
func WithDeadlockRetry(ctx context.Context, maxRetries int, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if (!IsDeadlock(err) && !IsLockWaitTimeout(err)) || attempt >= maxRetries {
			return err
		}
		jitter := config.DefaultDeadlockMinBackoff +
			time.Duration(rand.Int63n(int64(config.DefaultDeadlockMaxBackoff-config.DefaultDeadlockMinBackoff)))
		timer := time.NewTimer(jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
