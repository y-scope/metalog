package metastore

import (
	"context"
	"database/sql"
	"fmt"
)

// AdvisoryLock wraps MySQL GET_LOCK/RELEASE_LOCK on a dedicated connection.
// The lock is connection-scoped: it is automatically released if the connection
// drops, preventing deadlocks from crashed processes.
type AdvisoryLock struct {
	conn *sql.Conn
	name string
	held bool
}

// AcquireAdvisoryLock obtains a named advisory lock with a timeout.
// Uses db.Conn() to get a dedicated connection (required for lock affinity).
func AcquireAdvisoryLock(ctx context.Context, db *sql.DB, name string, timeoutSeconds int) (*AdvisoryLock, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("advisory lock conn: %w", err)
	}

	var result sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSeconds).Scan(&result)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("GET_LOCK(%s): %w", name, err)
	}

	if !result.Valid || result.Int64 != 1 {
		conn.Close()
		return nil, fmt.Errorf("GET_LOCK(%s): timeout or error (result=%v)", name, result)
	}

	return &AdvisoryLock{conn: conn, name: name, held: true}, nil
}

// Release releases the advisory lock and closes the connection.
func (l *AdvisoryLock) Release(ctx context.Context) error {
	if !l.held {
		return nil
	}
	defer l.conn.Close()
	l.held = false

	var result sql.NullInt64
	err := l.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&result)
	if err != nil {
		return fmt.Errorf("RELEASE_LOCK(%s): %w", l.name, err)
	}
	return nil
}
