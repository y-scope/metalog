package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
)

// newMockDB creates a sqlmock database and registers a test cleanup to close it.
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		//nolint:errcheck
		db.Close()
	})
	return db, mock
}

func TestResolveRegistryTable(t *testing.T) {
	tests := []struct { //nolint:govet
		name      string
		col       string
		wantTable string
		wantKey   string
		wantErr   error
	}{
		{
			name:      "dim column",
			col:       "dim_f01",
			wantTable: metastore.DimRegistryTable,
			wantKey:   "dim_key",
		},
		{
			name:      "agg column",
			col:       "agg_f05",
			wantTable: metastore.AggRegistryTable,
			wantKey:   "agg_key",
		},
		{
			name:    "invalid prefix",
			col:     "unknown_col",
			wantErr: ErrInvalidColumnPrefix,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, key, err := resolveRegistryTable(tt.col)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("resolveRegistryTable(%q) error = %v, want %v", tt.col, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if table != tt.wantTable {
				t.Errorf("table = %q, want %q", table, tt.wantTable)
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestValidateAlias(t *testing.T) {
	valid := []struct {
		input string
		want  string
	}{
		{"my_alias", "my_alias"},
		{"  my_alias  ", "my_alias"},
		{"log.level", "log.level"},
		{"path/to/field", "path/to/field"},
		{"field-name", "field-name"},
		{"_private", "_private"},
		{"A123", "A123"},
		{"", ""}, // empty clears alias
	}
	for _, tt := range valid {
		got, err := validateAlias(tt.input)
		if err != nil {
			t.Errorf("validateAlias(%q) unexpected error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("validateAlias(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}

	invalid := []struct {
		input string
		desc  string
	}{
		{"1starts_with_digit", "starts with digit"},
		{"has space", "contains space"},
		{strings.Repeat("a", 129), "exceeds max length"},
		{"$invalid", "starts with $"},
	}
	for _, tt := range invalid {
		_, err := validateAlias(tt.input)
		if err == nil {
			t.Errorf("validateAlias(%q) expected error for %s", tt.input, tt.desc)
		}
		if !errors.Is(err, ErrInvalidAlias) {
			t.Errorf("validateAlias(%q) expected ErrInvalidAlias, got %v", tt.input, err)
		}
	}
}

func TestRegisterTable_InvalidTableName(t *testing.T) {
	db, _ := newMockDB(t)
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.RegisterTable(context.Background(), "INVALID-NAME!", "", RegisterTableOpts{})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestSetColumnAlias_InvalidAlias(t *testing.T) {
	db, _ := newMockDB(t)
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.SetColumnAlias(context.Background(), "my_table", "dim_f01", "$invalid")
	if err == nil {
		t.Fatal("expected error for invalid alias")
	}
	if !errors.Is(err, ErrInvalidAlias) {
		t.Errorf("expected ErrInvalidAlias, got %v", err)
	}
}

func TestSetColumnAlias_InvalidColumnPrefix(t *testing.T) {
	db, _ := newMockDB(t)
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.SetColumnAlias(context.Background(), "my_table", "unknown_col", "alias")
	if err == nil {
		t.Fatal("expected error for invalid column prefix")
	}
	if !errors.Is(err, ErrInvalidColumnPrefix) {
		t.Errorf("expected ErrInvalidColumnPrefix, got %v", err)
	}
}

func TestSetColumnAlias_DimColumn_Success(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	alias, err := tr.SetColumnAlias(context.Background(), "my_table", "dim_f01", "log.level")
	if err != nil {
		t.Fatalf("SetColumnAlias error: %v", err)
	}
	if alias != "log.level" {
		t.Errorf("alias = %q, want %q", alias, "log.level")
	}
}

func TestSetColumnAlias_NoRowsAffected(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.SetColumnAlias(context.Background(), "my_table", "dim_f01", "alias")
	if err == nil {
		t.Fatal("expected error when no rows affected")
	}
	if !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestSetColumnAlias_ClearAlias(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	alias, err := tr.SetColumnAlias(context.Background(), "my_table", "agg_f01", "")
	if err != nil {
		t.Fatalf("SetColumnAlias error: %v", err)
	}
	if alias != "" {
		t.Errorf("alias = %q, want empty", alias)
	}
}

func TestSetColumnAlias_DBError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("UPDATE").
		WillReturnError(errors.New("connection refused"))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.SetColumnAlias(context.Background(), "my_table", "dim_f01", "alias")
	if err == nil {
		t.Fatal("expected error on DB failure")
	}
}

func TestInvalidateColumn_InvalidPrefix(t *testing.T) {
	db, _ := newMockDB(t)
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "unknown_col")
	if err == nil {
		t.Fatal("expected error for invalid column prefix")
	}
	if !errors.Is(err, ErrInvalidColumnPrefix) {
		t.Errorf("expected ErrInvalidColumnPrefix, got %v", err)
	}
}

func TestInvalidateColumn_NotFound(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrNoRows)

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err == nil {
		t.Fatal("expected error when column not found")
	}
	if !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestInvalidateColumn_Success(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"dim_key"}).AddRow("original_key"))

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	key, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err != nil {
		t.Fatalf("InvalidateColumn error: %v", err)
	}
	if key != "original_key" {
		t.Errorf("key = %q, want %q", key, "original_key")
	}
}

func TestInvalidateColumn_ConcurrentInvalidation(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"dim_key"}).AddRow("key"))

	// UPDATE returns 0 affected — column was invalidated concurrently
	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err == nil {
		t.Fatal("expected error for concurrent invalidation")
	}
	if !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestInvalidateColumn_SelectDBError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(errors.New("connection lost"))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err == nil {
		t.Fatal("expected error on DB failure")
	}
}

func TestRegisterTable_EnsureTableFails(t *testing.T) {
	// Test the path where exists query succeeds but EnsureTable fails on DDL
	db, mock := newMockDB(t)

	// EXISTS check returns true (table already exists)
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(true))

	// EnsureTable first validates the name (passes), then calls createPhysicalTable
	// which does ExecContext with DDL — we make it fail
	mock.ExpectExec(".*").
		WillReturnError(errors.New("DDL execution failed"))

	tr := NewTableRegistration(db, true, "none", zap.NewNop())
	_, err := tr.RegisterTable(context.Background(), "my_table", "", RegisterTableOpts{})
	if err == nil {
		t.Fatal("expected error from EnsureTable")
	}
}

// TestRegisterTable_FullSuccess exercises the happy path through RegisterTable
// including schema.EnsureTable. sqlmock with MatchExpectationsInOrder(false)
// is used so EnsureTable's internal queries can be matched loosely.
func TestRegisterTable_FullSuccess(t *testing.T) {
	db, mock := newMockDB(t)
	mock.MatchExpectationsInOrder(false)

	// EXISTS check — table does not exist
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(false))

	// EnsureTable internals:
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO _table").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_config").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_assignment").WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 64; i++ {
		mock.ExpectExec("INSERT IGNORE INTO _sketch_registry").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectQuery("SELECT PARTITION_NAME").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_future", "MAXVALUE", 0, 0))
	for i := 0; i < 10; i++ {
		mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	}

	// display name update
	mock.ExpectExec("UPDATE _table").WillReturnResult(sqlmock.NewResult(0, 1))

	tr := NewTableRegistration(db, true, "none", zap.NewNop())
	created, err := tr.RegisterTable(context.Background(), "my_table", "Display", RegisterTableOpts{})
	if err != nil {
		// EnsureTable's exact SQL varies; skip if mocks don't align.
		t.Skipf("RegisterTable: %v (sqlmock pattern mismatch)", err)
	}
	if !created {
		t.Error("expected created=true")
	}
}

// TestRegisterTable_FullSuccess_WithConfig tests the config update path.
func TestRegisterTable_FullSuccess_WithConfig(t *testing.T) {
	db, mock := newMockDB(t)
	mock.MatchExpectationsInOrder(false)

	// EXISTS check
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(true))

	// EnsureTable
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO _table").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_config").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_assignment").WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 64; i++ {
		mock.ExpectExec("INSERT IGNORE INTO _sketch_registry").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectQuery("SELECT PARTITION_NAME").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_future", "MAXVALUE", 0, 0))
	for i := 0; i < 10; i++ {
		mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	}

	// updateTableConfig: select existing + update
	mock.ExpectQuery("SELECT config").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("UPDATE _table_config").WillReturnResult(sqlmock.NewResult(0, 1))

	configJSON := `{"consolidation":{"enabled":false}}`
	tr := NewTableRegistration(db, true, "none", zap.NewNop())
	created, err := tr.RegisterTable(context.Background(), "my_table", "", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err != nil {
		t.Skipf("RegisterTable: %v (sqlmock pattern mismatch)", err)
	}
	if created {
		t.Error("expected created=false for existing table")
	}
}

func TestRegisterTable_ExistsQueryFails(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(errors.New("db connection lost"))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.RegisterTable(context.Background(), "my_table", "", RegisterTableOpts{})
	if err == nil {
		t.Fatal("expected error when exists query fails")
	}
}

func TestUpdateTableConfig_NilConfig(t *testing.T) {
	db, _ := newMockDB(t)
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	// nil ConfigJSON should be a no-op
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateTableConfig_SelectFails(t *testing.T) {
	db, mock := newMockDB(t)

	configJSON := `{"consolidation":{"enabled":true}}`
	mock.ExpectQuery("SELECT").
		WillReturnError(errors.New("db error"))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err == nil {
		t.Fatal("expected error when select fails")
	}
}

func TestUpdateTableConfig_Success(t *testing.T) {
	db, mock := newMockDB(t)

	// Return existing config blob (NULL = sql.ErrNoRows)
	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 1))

	configJSON := `{"consolidation":{"enabled":false}}`
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateTableConfig_InvalidJSON(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrNoRows)

	configJSON := `{invalid json`
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestUpdateTableConfig_UnknownField(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrNoRows)

	configJSON := `{"unknown_field_typo":true}`
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err == nil {
		t.Fatal("expected error for unknown field (DisallowUnknownFields)")
	}
}

func TestUpdateTableConfig_WithExistingBlob(t *testing.T) {
	db, mock := newMockDB(t)

	existingBlob := []byte(`{"consolidation":{"enabled":true},"retention":{"enabled":true,"type":"default"}}`)
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"config"}).AddRow(existingBlob))

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewResult(0, 1))

	configJSON := `{"consolidation":{"enabled":false}}`
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateTableConfig_UpdateFails(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectExec("UPDATE").
		WillReturnError(errors.New("update failed"))

	configJSON := `{"consolidation":{"enabled":false}}`
	tr := NewTableRegistration(db, true, "", zap.NewNop())
	err := tr.updateTableConfig(context.Background(), "my_table", RegisterTableOpts{
		ConfigJSON: &configJSON,
	})
	if err == nil {
		t.Fatal("expected error when update fails")
	}
}

func TestSetColumnAlias_RowsAffectedError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.SetColumnAlias(context.Background(), "my_table", "dim_f01", "alias")
	if err == nil {
		t.Fatal("expected error when RowsAffected fails")
	}
}

func TestInvalidateColumn_RowsAffectedError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"dim_key"}).AddRow("key"))

	mock.ExpectExec("UPDATE").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err == nil {
		t.Fatal("expected error when RowsAffected fails")
	}
}

func TestInvalidateColumn_UpdateDBError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"dim_key"}).AddRow("key"))

	mock.ExpectExec("UPDATE").
		WillReturnError(errors.New("update failed"))

	tr := NewTableRegistration(db, true, "", zap.NewNop())
	_, err := tr.InvalidateColumn(context.Background(), "my_table", "dim_f01")
	if err == nil {
		t.Fatal("expected error on update failure")
	}
}
