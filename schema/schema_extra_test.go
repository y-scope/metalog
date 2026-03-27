package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// TableProvisioner: loadTemplateDDL, insertRegistryRows, prepopulateSketchSlots,
// createPhysicalTable, EnsureTable, sketchSlotCount
// ---------------------------------------------------------------------------

func TestLoadTemplateDDL(t *testing.T) {
	ddl, err := loadTemplateDDL()
	if err != nil {
		t.Fatalf("loadTemplateDDL() error = %v", err)
	}
	if ddl == "" {
		t.Error("DDL should not be empty")
	}
	if !strings.Contains(ddl, "_clp_template") {
		t.Error("DDL should contain _clp_template")
	}
}

func TestInsertRegistryRows(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))

	err := insertRegistryRows(context.Background(), db, "test_table")
	if err != nil {
		t.Fatalf("insertRegistryRows() error = %v", err)
	}
}

func TestPrepopulateSketchSlots(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	for i := 0; i < sketchSlotCount; i++ {
		mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	}

	err := prepopulateSketchSlots(context.Background(), db, "test_table")
	if err != nil {
		t.Fatalf("prepopulateSketchSlots() error = %v", err)
	}
}

func TestSketchSlotCount(t *testing.T) {
	if sketchSlotCount != 64 {
		t.Errorf("sketchSlotCount = %d, want 64", sketchSlotCount)
	}
}

func TestCreatePhysicalTable(t *testing.T) {
	for _, tt := range []struct {
		name, compression string
		isMariaDB         bool
	}{
		{"none", "none", true},
		{"lz4", "lz4", false},
		{"page_compressed", "", true},
		{"zlib", "zlib", false},
		{"zstd", "zstd", false},
		{"default_mysql", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			defer db.Close() //nolint:errcheck
			mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
			if err := createPhysicalTable(context.Background(), db, "test_table", tt.isMariaDB, tt.compression); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreatePhysicalTable_UnsupportedCompression(t *testing.T) {
	if err := createPhysicalTable(context.Background(), nil, "test_table", false, "invalid"); err == nil {
		t.Fatal("expected error")
	}
}

func TestEnsureTable_InvalidTableName(t *testing.T) {
	err := EnsureTable(context.Background(), nil, "DROP TABLE;--", false, "", zap.NewNop())
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

// ---------------------------------------------------------------------------
// PartitionManager
// ---------------------------------------------------------------------------

func TestPartitionManager_RunMaintenance_LockNotAcquired(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 90, zap.NewNop())

	// Advisory lock returns 0 (not acquired, timeout=0)
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(0))

	err := pm.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("error = %v (should skip gracefully)", err)
	}
}

func TestRunMaintenance_WithCleanup(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 1, 1, zap.NewNop()) // cleanupAgeDays=1

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// createLookaheadPartitions: all exist
	now := time.Now().UTC().Truncate(24 * time.Hour)
	rows := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"})
	rows.AddRow("p_floor", sql.NullString{}, int64(0), int64(0))
	for i := 0; i <= 1; i++ {
		d := now.AddDate(0, 0, i)
		name := fmt.Sprintf("p_%s", d.Format("20060102"))
		boundary := d.AddDate(0, 0, 1).UnixNano()
		rows.AddRow(name, sql.NullString{Valid: true, String: fmt.Sprintf("%d", boundary)}, int64(0), int64(0))
	}
	rows.AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0))
	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(rows)

	// cleanupOldPartitions: getExistingPartitions
	rows2 := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"})
	rows2.AddRow("p_floor", sql.NullString{}, int64(0), int64(0))
	rows2.AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0))
	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(rows2)

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	err := pm.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestPartitionManager_EnsureLookaheadPartitions_LockFailed(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 90, zap.NewNop())

	// Advisory lock fails (timeout)
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(0))

	// Still proceeds without lock — getExistingPartitions
	rows := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"})
	rows.AddRow("p_floor", sql.NullString{}, int64(0), int64(0))
	now := time.Now().UTC().Truncate(24 * time.Hour)
	for i := 0; i <= 7; i++ {
		d := now.AddDate(0, 0, i)
		name := fmt.Sprintf("p_%s", d.Format("20060102"))
		boundary := d.AddDate(0, 0, 1).UnixNano()
		rows.AddRow(name, sql.NullString{Valid: true, String: fmt.Sprintf("%d", boundary)}, int64(0), int64(0))
	}
	rows.AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0))
	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(rows)

	created, err := pm.EnsureLookaheadPartitions(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if created != 0 {
		t.Errorf("created = %d, want 0", created)
	}
}

func TestIsPartitionEmpty_False(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 90, zap.NewNop())

	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(100))

	if pm.isPartitionEmpty(context.Background(), "p_20260101") {
		t.Error("should not be empty")
	}
}

func TestIsPartitionEmpty_Error(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 90, zap.NewNop())

	mock.ExpectQuery("SELECT COUNT").WillReturnError(fmt.Errorf("query error"))

	if pm.isPartitionEmpty(context.Background(), "p_20260101") {
		t.Error("should assume non-empty on error")
	}
}

func TestCleanupOldPartitions_NoCandidates(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 90, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_floor", sql.NullString{}, int64(0), int64(0)).
			AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0)))

	err := pm.cleanupOldPartitions(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestCleanupOldPartitions_MergeNonEmpty(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 1, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_floor", sql.NullString{}, int64(0), int64(0)).
			AddRow("p_20200101", sql.NullString{Valid: true, String: "1577923200000000000"}, int64(1000), int64(4096)).
			AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0)))

	// REORGANIZE PARTITION to merge into p_floor
	mock.ExpectExec("ALTER TABLE.*REORGANIZE PARTITION").WillReturnResult(sqlmock.NewResult(0, 0))

	err := pm.cleanupOldPartitions(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestCleanupOldPartitions_NoFloor(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 1, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_20200101", sql.NullString{Valid: true, String: "1577923200000000000"}, int64(1000), int64(4096)).
			AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0)))

	err := pm.cleanupOldPartitions(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestCleanupOldPartitions_DropEmpty(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	pm := NewPartitionManager(db, "test_table", 7, 1, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "TABLE_ROWS", "DATA_LENGTH"}).
			AddRow("p_floor", sql.NullString{}, int64(0), int64(0)).
			AddRow("p_20200101", sql.NullString{Valid: true, String: "1577923200000000000"}, int64(0), int64(0)).
			AddRow("p_future", sql.NullString{Valid: true, String: "MAXVALUE"}, int64(0), int64(0)))

	// isPartitionEmpty check
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0))

	// DROP PARTITION
	mock.ExpectExec("ALTER TABLE.*DROP PARTITION").WillReturnResult(sqlmock.NewResult(0, 0))

	err := pm.cleanupOldPartitions(context.Background())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// IndexManager
// ---------------------------------------------------------------------------

func TestIndexManager_EnsureIndex_InvalidIdentifier(t *testing.T) {
	im := NewIndexManager(nil, true, zap.NewNop())

	err := im.EnsureIndex(context.Background(), "DROP TABLE;--", "dim_f01")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}

	err = im.EnsureIndex(context.Background(), "test_table", "DROP TABLE;--")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestIndexManager_DropIndex_InvalidIdentifier(t *testing.T) {
	im := NewIndexManager(nil, true, zap.NewNop())

	err := im.DropIndex(context.Background(), "DROP;--", "dim_f01")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}

	err = im.DropIndex(context.Background(), "test_table", "DROP;--")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestIndexManager_Reconcile_Empty(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	im := NewIndexManager(db, true, zap.NewNop())
	err := im.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatalf("Reconcile(nil) error = %v", err)
	}
}

func TestIndexManager_Reconcile_CreateAndDrop(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	im := NewIndexManager(db, true, zap.NewNop())

	desired := []IndexConfig{
		{Table: "test_table", Columns: []string{"dim_f01", "dim_f02"}},
	}

	// listDynamicIndexes
	mock.ExpectQuery("SELECT DISTINCT INDEX_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).
			AddRow("idx_dim_f01"). // already exists
			AddRow("idx_dim_f03"), // stale, should be dropped
	)

	// Create missing index dim_f02
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	// Drop stale index dim_f03
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	err := im.Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// Validator
// ---------------------------------------------------------------------------

func TestValidateColumns_TypeMismatch(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_NAME, COLUMN_TYPE").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE"}).
			AddRow("id", "int(11)"))

	err := v.validateColumns(context.Background(), "test_table", []columnSpec{
		{"id", "bigint"},
	})
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestValidateColumns_EmptyTable(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_NAME, COLUMN_TYPE").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE"}))

	err := v.validateColumns(context.Background(), "nonexistent", []columnSpec{
		{"id", "bigint"},
	})
	if err == nil {
		t.Fatal("expected error for empty/nonexistent table")
	}
}

func TestValidateColumns_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_NAME, COLUMN_TYPE").
		WillReturnError(fmt.Errorf("connection error"))

	err := v.validateColumns(context.Background(), "test_table", []columnSpec{
		{"id", "bigint"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateColumns_EmptyTypePrefix(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_NAME, COLUMN_TYPE").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE"}).
			AddRow("active", "tinyint(1)"))

	err := v.validateColumns(context.Background(), "test_table", []columnSpec{
		{"active", ""},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateIndexes_Missing(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT DISTINCT INDEX_NAME, NON_UNIQUE").
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "NON_UNIQUE"}).
			AddRow("PRIMARY", 0))

	err := v.validateIndexes(context.Background(), "test_table", []indexSpec{
		{"PRIMARY", false},
		{"idx_missing", false},
	})
	if err == nil {
		t.Fatal("expected error for missing index")
	}
}

func TestValidateIndexes_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT DISTINCT INDEX_NAME, NON_UNIQUE").
		WillReturnError(fmt.Errorf("db error"))

	err := v.validateIndexes(context.Background(), "test_table", []indexSpec{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateIndexes_UniquenessWarn(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT DISTINCT INDEX_NAME, NON_UNIQUE").
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "NON_UNIQUE"}).
			AddRow("idx_hash", 1)) // non-unique, but spec expects unique

	// Should not error — uniqueness mismatch is just a warning
	err := v.validateIndexes(context.Background(), "test_table", []indexSpec{
		{"idx_hash", true},
	})
	if err != nil {
		t.Fatalf("error = %v (should only warn, not fail)", err)
	}
}

func TestValidateSketchSlots_WrongCount(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE"}).AddRow("set('s01','s02')"))

	err := v.validateSketchSlots(context.Background(), "_clp_template")
	if err == nil {
		t.Fatal("expected error for wrong sketch slot count")
	}
}

func TestValidateSketchSlots_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS").
		WillReturnError(fmt.Errorf("db error"))

	err := v.validateSketchSlots(context.Background(), "_clp_template")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidatePartitioned_NotPartitioned(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_METHOD"}).
			AddRow(sql.NullString{Valid: false}))

	err := v.validatePartitioned(context.Background(), "_clp_template")
	if err == nil {
		t.Fatal("expected error for non-partitioned table")
	}
}

func TestValidatePartitioned_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_METHOD").
		WillReturnError(fmt.Errorf("db error"))

	err := v.validatePartitioned(context.Background(), "_clp_template")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate_SystemTableMissing(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	// First system table query returns empty (table doesn't exist)
	mock.ExpectQuery("SELECT COLUMN_NAME, COLUMN_TYPE").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE"}))

	err := v.Validate(context.Background())
	if err == nil {
		t.Fatal("expected error when system table has no columns")
	}
}

func TestInsertRegistryRows_TableInsertFails(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT").WillReturnError(fmt.Errorf("insert failed"))

	err := insertRegistryRows(context.Background(), db, "test_table")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestInsertRegistryRows_ConfigInsertFails(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnError(fmt.Errorf("config insert failed"))

	err := insertRegistryRows(context.Background(), db, "test_table")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestInsertRegistryRows_AssignmentInsertFails(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnError(fmt.Errorf("assignment insert failed"))

	err := insertRegistryRows(context.Background(), db, "test_table")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateSketchSlots_InsertFails(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	// First insert fails
	mock.ExpectExec("INSERT").WillReturnError(fmt.Errorf("sketch insert failed"))

	err := prepopulateSketchSlots(context.Background(), db, "test_table")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDropIndex_IgnoresNotExistError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	im := NewIndexManager(db, true, zap.NewNop())

	// MySQL error 1091 (ER_CANT_DROP_FIELD_OR_KEY) — should be silently ignored
	mock.ExpectExec("ALTER TABLE").WillReturnError(&mysql.MySQLError{Number: 1091, Message: "Can't DROP; check that it exists"})

	err := im.DropIndex(context.Background(), "test_table", "dim_f01")
	if err != nil {
		t.Fatalf("error = %v (should be ignored)", err)
	}
}

func TestGetExistingPartitions_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnError(fmt.Errorf("db error"))

	_, err := getExistingPartitions(context.Background(), db, "test_table")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateLookaheadPartitions_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT PARTITION_NAME").WillReturnError(fmt.Errorf("db error"))

	_, err := createLookaheadPartitions(context.Background(), db, "test_table", 7, zap.NewNop())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReconcile_ListIndexesError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	im := NewIndexManager(db, true, zap.NewNop())

	desired := []IndexConfig{
		{Table: "test_table", Columns: []string{"dim_f01"}},
	}

	// listDynamicIndexes fails
	mock.ExpectQuery("SELECT DISTINCT INDEX_NAME").WillReturnError(fmt.Errorf("db error"))

	// Should not error overall (logs warning and continues)
	err := im.Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestValidatePartitioned_RangeOnly(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_METHOD"}).
			AddRow(sql.NullString{Valid: true, String: "RANGE"}))

	err := v.validatePartitioned(context.Background(), "_clp_template")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestValidatePartitioned_Hash(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	v := NewBaseSchemaValidator(db, zap.NewNop())

	mock.ExpectQuery("SELECT PARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_METHOD"}).
			AddRow(sql.NullString{Valid: true, String: "HASH"}))

	err := v.validatePartitioned(context.Background(), "_clp_template")
	if err == nil {
		t.Fatal("expected error for HASH partitioning")
	}
}
