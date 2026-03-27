package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
)

func TestLockMode(t *testing.T) {
	if lockMode(true) != "SHARED" {
		t.Error("MariaDB should use SHARED")
	}
	if lockMode(false) != "NONE" {
		t.Error("MySQL should use NONE")
	}
}

func TestEvolver_AddColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	e := NewEvolver(db, true, zap.NewNop())
	if err := e.AddColumn(context.Background(), "test_table", "dim_f01", "VARCHAR(255)"); err != nil {
		t.Fatal(err)
	}
}

func TestEvolver_AddColumn_InvalidTable(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	e := NewEvolver(db, false, zap.NewNop())
	if err := e.AddColumn(context.Background(), "DROP TABLE;--", "col", "INT"); err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestEvolver_AddColumn_InvalidColumn(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	e := NewEvolver(db, false, zap.NewNop())
	if err := e.AddColumn(context.Background(), "test_table", "BAD COL", "INT"); err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestEvolver_AddColumn_ExecError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectExec("ALTER TABLE").WillReturnError(fmt.Errorf("table locked"))

	e := NewEvolver(db, false, zap.NewNop())
	if err := e.AddColumn(context.Background(), "test_table", "dim_f01", "VARCHAR(255)"); err == nil {
		t.Fatal("expected error")
	}
}

func TestSchemaSQL_NotEmpty(t *testing.T) {
	if len(SchemaSQL) == 0 {
		t.Fatal("SchemaSQL should not be empty")
	}
}
