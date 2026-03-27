package metastore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
)

func newMockFileRecords(t *testing.T, tableName string, isMariaDB bool) (*FileRecords, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	fr, err := NewFileRecords(db, tableName, isMariaDB, zap.NewNop())
	if err != nil {
		t.Fatalf("NewFileRecords() error = %v", err)
	}
	return fr, mock
}

func TestNewFileRecords_InvalidTableName(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	_, err := NewFileRecords(db, "DROP TABLE;--", false, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestUpsertBatch_EmptyRecords(t *testing.T) {
	fr, _ := newMockFileRecords(t, "test_table", true)

	affected, err := fr.UpsertBatch(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

func TestUpsertBatch_SingleRecord(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectExec("INSERT INTO").
		WillReturnResult(sqlmock.NewResult(1, 1))

	records := []*FileRecord{{
		State:       StateIRBuffering,
		MinTimestamp: 1000,
		MaxTimestamp: 2000,
		ClpIRPath:   sql.NullString{Valid: true, String: "/ir/path"},
	}}

	affected, err := fr.UpsertBatch(context.Background(), records, nil, nil, nil)
	if err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}
}

func TestUpsertBatch_WithDimAndAggCols(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectExec("INSERT INTO").
		WillReturnResult(sqlmock.NewResult(1, 1))

	records := []*FileRecord{{
		State:       StateIRBuffering,
		MinTimestamp: 1000,
		ClpIRPath:   sql.NullString{Valid: true, String: "/ir/path"},
		Dims:        map[string]any{"dim_f01": "host1"},
		Aggs:        map[string]any{"agg_f01": int64(42)},
	}}

	affected, err := fr.UpsertBatch(context.Background(), records,
		[]string{"dim_f01"}, []string{"agg_f01"}, nil)
	if err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}
}

func TestFindByID_Found(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	rows := sqlmock.NewRows(cols).
		AddRow(
			int64(1),
			sql.NullString{}, sql.NullString{}, sql.NullString{Valid: true, String: "/ir"},
			sql.NullString{}, sql.NullString{}, sql.NullString{},
			"IR_BUFFERING", int64(1000), int64(2000), int64(0),
			uint32(10), sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
			uint16(30), int64(0),
		)
	mock.ExpectQuery("SELECT").
		WithArgs(int64(1)).
		WillReturnRows(rows)

	rec, err := fr.FindByID(context.Background(), 1)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if rec == nil {
		t.Fatal("FindByID() returned nil")
	}
	if rec.ID != 1 {
		t.Errorf("ID = %d, want 1", rec.ID)
	}
	if rec.State != StateIRBuffering {
		t.Errorf("State = %q, want IR_BUFFERING", rec.State)
	}
}

func TestFindByID_NotFound(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	rows := sqlmock.NewRows(cols)
	mock.ExpectQuery("SELECT").
		WithArgs(int64(999)).
		WillReturnRows(rows)

	rec, err := fr.FindByID(context.Background(), 999)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if rec != nil {
		t.Error("FindByID() should return nil for missing record")
	}
}

func TestFindByIRPath(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	rows := sqlmock.NewRows(cols).
		AddRow(
			int64(1),
			sql.NullString{}, sql.NullString{}, sql.NullString{Valid: true, String: "/ir/path"},
			sql.NullString{}, sql.NullString{}, sql.NullString{},
			"IR_CLOSED", int64(1000), int64(2000), int64(0),
			uint32(10), sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
			uint16(30), int64(0),
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	rec, err := fr.FindByIRPath(context.Background(), "/ir/path")
	if err != nil {
		t.Fatalf("FindByIRPath() error = %v", err)
	}
	if rec == nil {
		t.Fatal("returned nil")
	}
	if rec.State != StateIRClosed {
		t.Errorf("State = %q, want IR_CLOSED", rec.State)
	}
}

func TestFindByIRPath_NotFound(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))

	rec, err := fr.FindByIRPath(context.Background(), "/nonexistent")
	if err != nil {
		t.Fatalf("FindByIRPath() error = %v", err)
	}
	if rec != nil {
		t.Error("should return nil for missing")
	}
}

func TestFindByArchivePath(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	rows := sqlmock.NewRows(cols).
		AddRow(
			int64(5),
			sql.NullString{}, sql.NullString{}, sql.NullString{},
			sql.NullString{Valid: true, String: "s3"}, sql.NullString{Valid: true, String: "bucket"},
			sql.NullString{Valid: true, String: "/archive/path"},
			"ARCHIVE_CLOSED", int64(1000), int64(2000), int64(3000),
			uint32(100), sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{Valid: true, Int64: 5000},
			uint16(90), int64(0),
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	rec, err := fr.FindByArchivePath(context.Background(), "/archive/path")
	if err != nil {
		t.Fatalf("FindByArchivePath() error = %v", err)
	}
	if rec == nil {
		t.Fatal("returned nil")
	}
	if rec.ID != 5 {
		t.Errorf("ID = %d, want 5", rec.ID)
	}
}

func TestGetCurrentStates_Empty(t *testing.T) {
	fr, _ := newMockFileRecords(t, "test_table", true)

	states, err := fr.GetCurrentStates(context.Background(), nil)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(states) != 0 {
		t.Errorf("expected empty map, got %d entries", len(states))
	}
}

func TestGetCurrentStates_WithPaths(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	rows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_BUFFERING").
		AddRow("/ir/2", "IR_CLOSED")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	states, err := fr.GetCurrentStates(context.Background(), []string{"/ir/1", "/ir/2"})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 states, got %d", len(states))
	}
	if states["/ir/1"] != StateIRBuffering {
		t.Errorf("state for /ir/1 = %q", states["/ir/1"])
	}
}

func TestTransitionExpiredToPurging(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	// IR_CLOSED -> IR_PURGING
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 5))
	// ARCHIVE_CLOSED -> ARCHIVE_PURGING
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 3))

	affected, err := fr.TransitionExpiredToPurging(context.Background(), 9999999)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if affected != 8 {
		t.Errorf("affected = %d, want 8", affected)
	}
}

func TestDeleteExpiredFiles_NoExpired(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectCommit()

	result, err := fr.DeleteExpiredFiles(context.Background(), 9999999)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if result.DeletedCount != 0 {
		t.Errorf("deleted = %d, want 0", result.DeletedCount)
	}
}

func TestDeleteExpiredFiles_WithExpired(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	rows := sqlmock.NewRows(cols).
		AddRow(
			sql.NullString{Valid: true, String: "fs"},
			sql.NullString{Valid: true, String: "/bucket"},
			sql.NullString{Valid: true, String: "/ir/file1"},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			[]byte{0x01, 0x02},
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := fr.DeleteExpiredFiles(context.Background(), 9999999)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if result.DeletedCount != 1 {
		t.Errorf("deleted = %d, want 1", result.DeletedCount)
	}
	if len(result.IRPaths) != 1 {
		t.Errorf("IR paths = %d, want 1", len(result.IRPaths))
	}
}

func TestUpdateState_EmptyPaths(t *testing.T) {
	fr, _ := newMockFileRecords(t, "test_table", true)

	affected, err := fr.UpdateState(context.Background(), nil, StateIRClosed)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

func TestUpdateState_ValidTransition(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	// getCurrentStatesInTx
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_BUFFERING")
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	// Update
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	affected, err := fr.UpdateState(context.Background(), []string{"/ir/1"}, StateIRClosed)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}
}

func TestUpdateState_InvalidTransition(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_PURGING")
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	mock.ExpectRollback()

	_, err := fr.UpdateState(context.Background(), []string{"/ir/1"}, StateIRClosed)
	if err == nil {
		t.Fatal("expected error for invalid state transition")
	}
}

func TestMarkArchiveClosed_EmptyPaths(t *testing.T) {
	fr, _ := newMockFileRecords(t, "test_table", true)

	affected, err := fr.MarkArchiveClosed(context.Background(), nil, "/archive", "s3", "bucket", 1000, 2000)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

func TestMarkArchiveClosed_InvalidTransition(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_BUFFERING")
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	mock.ExpectRollback()

	_, err := fr.MarkArchiveClosed(context.Background(),
		[]string{"/ir/1"}, "/archive", "s3", "bucket", 1000, 2000)
	if err == nil {
		t.Fatal("expected error for invalid transition from IR_BUFFERING to ARCHIVE_CLOSED")
	}
}

func TestPromoteStuckBuffering(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 3))

	affected, err := fr.PromoteStuckBuffering(context.Background(), 1000000)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if affected != 3 {
		t.Errorf("affected = %d, want 3", affected)
	}
}

func TestFindConsolidationPending(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	cols := baseSelectCols()
	rows := sqlmock.NewRows(cols).
		AddRow(
			int64(1),
			sql.NullString{}, sql.NullString{}, sql.NullString{Valid: true, String: "/ir/1"},
			sql.NullString{}, sql.NullString{}, sql.NullString{},
			"IR_ARCHIVE_CONSOLIDATION_PENDING", int64(1000), int64(2000), int64(0),
			uint32(10), sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
			uint16(30), int64(0),
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	records, err := fr.FindConsolidationPending(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].State != StateIRArchiveConsolidationPending {
		t.Errorf("state = %q", records[0].State)
	}
}

func TestFindConsolidationPending_WithMappings(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	baseCols := baseSelectCols()
	allCols := append(baseCols, "dim_f01", "agg_f01")
	rows := sqlmock.NewRows(allCols).
		AddRow(
			int64(1),
			sql.NullString{}, sql.NullString{}, sql.NullString{Valid: true, String: "/ir/1"},
			sql.NullString{}, sql.NullString{}, sql.NullString{},
			"IR_ARCHIVE_CONSOLIDATION_PENDING", int64(1000), int64(2000), int64(0),
			uint32(10), sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
			uint16(30), int64(0),
			sql.NullString{Valid: true, String: "host1"},
			sql.NullString{Valid: true, String: "42"},
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	dimMappings := []ColumnMapping{{PhysicalCol: "dim_f01", LogicalKey: "host"}}
	aggMappings := []ColumnMapping{{PhysicalCol: "agg_f01", LogicalKey: "count"}}

	records, err := fr.FindConsolidationPending(context.Background(), dimMappings, aggMappings)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Dims["host"] != "host1" {
		t.Errorf("dim host = %v", records[0].Dims["host"])
	}
	if records[0].Aggs["count"] != "42" {
		t.Errorf("agg count = %v", records[0].Aggs["count"])
	}
}
func TestMarkArchiveClosed_Success(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	// getCurrentStatesInTx
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_ARCHIVE_CONSOLIDATION_PENDING").
		AddRow("/ir/2", "IR_ARCHIVE_CONSOLIDATION_PENDING")
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	// Update
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	affected, err := fr.MarkArchiveClosed(context.Background(),
		[]string{"/ir/1", "/ir/2"}, "/archive/path", "s3", "bucket", 5000, 1234567)
	if err != nil {
		t.Fatalf("MarkArchiveClosed() error = %v", err)
	}
	if affected != 2 {
		t.Errorf("affected = %d, want 2", affected)
	}
}

func TestMarkArchiveClosed_MissingFile(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	// getCurrentStatesInTx - only one of two paths found
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"}).
		AddRow("/ir/1", "IR_ARCHIVE_CONSOLIDATION_PENDING")
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	// Update - only 1 found
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	affected, err := fr.MarkArchiveClosed(context.Background(),
		[]string{"/ir/1", "/ir/missing"}, "/archive/path", "s3", "bucket", 5000, 1234567)
	if err != nil {
		t.Fatalf("MarkArchiveClosed() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}
}

func TestMarkArchiveClosed_AllMissing(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	stateRows := sqlmock.NewRows([]string{"clp_ir_path", "state"})
	mock.ExpectQuery("SELECT").WillReturnRows(stateRows)
	mock.ExpectCommit()

	affected, err := fr.MarkArchiveClosed(context.Background(),
		[]string{"/ir/missing"}, "/archive/path", "s3", "bucket", 5000, 1234567)
	if err != nil {
		t.Fatalf("MarkArchiveClosed() error = %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

func TestAdvisoryLock_AcquireAndRelease(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectQuery("SELECT GET_LOCK").
		WithArgs("test_lock", 0).
		WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(int64(1)))

	mock.ExpectQuery("SELECT RELEASE_LOCK").
		WithArgs("test_lock").
		WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(int64(1)))

	lock, err := AcquireAdvisoryLock(context.Background(), db, "test_lock", 0)
	if err != nil {
		t.Fatalf("AcquireAdvisoryLock() error = %v", err)
	}
	if lock == nil {
		t.Fatal("returned nil lock")
	}

	err = lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	// Second release should be no-op
	err = lock.Release(context.Background())
	if err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
}

func TestAdvisoryLock_AcquireFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup

	mock.ExpectQuery("SELECT GET_LOCK").
		WithArgs("test_lock", 0).
		WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(int64(0)))

	_, err := AcquireAdvisoryLock(context.Background(), db, "test_lock", 0)
	if err == nil {
		t.Fatal("expected error when lock not acquired")
	}
}

func TestDeleteExpiredFiles_WithArchivePaths(t *testing.T) {
	fr, mock := newMockFileRecords(t, "test_table", true)

	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	rows := sqlmock.NewRows(cols).
		AddRow(
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{Valid: true, String: "s3"},
			sql.NullString{Valid: true, String: "my-bucket"},
			sql.NullString{Valid: true, String: "/archive/file"},
			[]byte{0x01, 0x02},
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := fr.DeleteExpiredFiles(context.Background(), 9999999)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(result.ArchivePaths) != 1 {
		t.Errorf("archive paths = %d, want 1", len(result.ArchivePaths))
	}
	if result.ArchivePaths[0].Backend != "s3" {
		t.Errorf("backend = %q, want s3", result.ArchivePaths[0].Backend)
	}
}

func TestBuildGuardedUpsertSQL_InvalidColumnName(t *testing.T) {
	_, _, _, err := BuildGuardedUpsertSQL("test_table", []string{"DROP TABLE;--"}, nil, nil, 1, false)
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}
