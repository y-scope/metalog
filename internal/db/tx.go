package db

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/y-scope/metalog/internal/config"
)

// Querier is the common interface for *sql.DB and *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// WithTx executes fn inside a database transaction. If fn returns an error,
// the transaction is rolled back; otherwise it is committed.
func WithTx(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() // no-op after successful commit
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// WithDeadlockRetry wraps fn in a retry loop that retries on MySQL deadlocks.
// Uses random jitter in [DefaultDeadlockMinBackoff, DefaultDeadlockMaxBackoff] on each retry.
func WithDeadlockRetry(ctx context.Context, maxRetries int, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !IsDeadlock(err) || attempt >= maxRetries {
			return err
		}
		jitter := config.DefaultDeadlockMinBackoff +
			time.Duration(rand.Int63n(int64(config.DefaultDeadlockMaxBackoff-config.DefaultDeadlockMinBackoff)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitter):
		}
	}
}
