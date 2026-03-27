package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestWithTx_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := WithTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), "INSERT INTO test VALUES (1)")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithTx_FnError_Rollback(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectBegin()
	mock.ExpectRollback()

	fnErr := fmt.Errorf("fn failed")
	err := WithTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		return fnErr
	})
	if err != fnErr {
		t.Errorf("expected fnErr, got %v", err)
	}
}

func TestWithTx_BeginError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectBegin().WillReturnError(fmt.Errorf("begin failed"))

	err := WithTx(context.Background(), db, nil, func(tx *sql.Tx) error { return nil })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWithTx_CommitError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(fmt.Errorf("commit failed"))

	err := WithTx(context.Background(), db, nil, func(tx *sql.Tx) error { return nil })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWithDeadlockRetry_RetriesOnLockWaitTimeout(t *testing.T) {
	calls := 0
	err := WithDeadlockRetry(context.Background(), 3, func() error {
		calls++
		if calls < 2 {
			return &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("called %d times, want 2", calls)
	}
}
