package retention

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/storage"
)

// mockBackend implements storage.Backend for testing deleteStoragePaths.
type mockBackend struct {
	errOnKey string // if non-empty, return an error when deleting this key
	deleted  []string
	mu       sync.Mutex
}

func (m *mockBackend) Get(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) Put(_ context.Context, _, _ string, _ io.Reader, _ int64) error {
	return errors.New("not implemented")
}

func (m *mockBackend) Delete(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errOnKey != "" && key == m.errOnKey {
		return errors.New("delete failed")
	}
	m.deleted = append(m.deleted, bucket+"/"+key)
	return nil
}

func (m *mockBackend) Exists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func TestDeleteStoragePaths_NilRegistry(t *testing.T) {
	s := &defaultStrategy{
		storageRegistry: nil,
		log:             zap.NewNop(),
		deleteRate:      1000,
	}
	// Should not panic with nil registry.
	s.deleteStoragePaths(context.Background(), []metastore.StoragePath{
		{Backend: "s3", Bucket: "b", Path: "/p"},
	})
}

func TestDeleteStoragePaths_EmptyPaths(t *testing.T) {
	s := &defaultStrategy{
		storageRegistry: storage.NewRegistry(),
		log:             zap.NewNop(),
		deleteRate:      1000,
	}
	// Should not panic with empty paths.
	s.deleteStoragePaths(context.Background(), nil)
	s.deleteStoragePaths(context.Background(), []metastore.StoragePath{})
}

func TestDeleteStoragePaths_SkipsEmptyBackendOrPath(t *testing.T) {
	reg := storage.NewRegistry()
	mb := &mockBackend{}
	reg.Register("s3", mb)

	s := &defaultStrategy{
		storageRegistry: reg,
		log:             zap.NewNop(),
		deleteRate:      100000, // high rate to avoid delays
	}
	s.deleteStoragePaths(context.Background(), []metastore.StoragePath{
		{Backend: "", Bucket: "b", Path: "/p"},
		{Backend: "s3", Bucket: "b", Path: ""},
		{Backend: "s3", Bucket: "b", Path: "/valid"},
	})

	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.deleted) != 1 {
		t.Errorf("expected 1 deletion, got %d: %v", len(mb.deleted), mb.deleted)
	}
}

func TestDeleteStoragePaths_UnknownBackend(t *testing.T) {
	reg := storage.NewRegistry()
	s := &defaultStrategy{
		storageRegistry: reg,
		log:             zap.NewNop(),
		deleteRate:      100000,
	}
	// Should not panic; logs warning but continues.
	s.deleteStoragePaths(context.Background(), []metastore.StoragePath{
		{Backend: "unknown", Bucket: "b", Path: "/p"},
	})
}

func TestDeleteStoragePaths_DeleteError(t *testing.T) {
	reg := storage.NewRegistry()
	mb := &mockBackend{errOnKey: "/fail"}
	reg.Register("s3", mb)

	s := &defaultStrategy{
		storageRegistry: reg,
		log:             zap.NewNop(),
		deleteRate:      100000,
	}
	// Should not return error — deletion errors are logged but not propagated.
	s.deleteStoragePaths(context.Background(), []metastore.StoragePath{
		{Backend: "s3", Bucket: "b", Path: "/fail"},
		{Backend: "s3", Bucket: "b", Path: "/ok"},
	})

	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.deleted) != 1 || mb.deleted[0] != "b//ok" {
		t.Errorf("expected 1 successful deletion of /ok, got %v", mb.deleted)
	}
}

func TestDeleteStoragePaths_ContextCancel(t *testing.T) {
	reg := storage.NewRegistry()
	mb := &mockBackend{}
	reg.Register("s3", mb)

	s := &defaultStrategy{
		storageRegistry: reg,
		log:             zap.NewNop(),
		deleteRate:      1, // very slow — 1 per second
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create many paths but cancel shortly after start.
	paths := make([]metastore.StoragePath, 10)
	for i := range paths {
		paths[i] = metastore.StoragePath{Backend: "s3", Bucket: "b", Path: "/p"}
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	s.deleteStoragePaths(ctx, paths)

	mb.mu.Lock()
	defer mb.mu.Unlock()
	// Should have deleted at most a few before context was canceled.
	if len(mb.deleted) >= len(paths) {
		t.Errorf("expected fewer than %d deletions due to context cancel, got %d", len(paths), len(mb.deleted))
	}
}

func TestNewDefaultStrategy_FailureLogIntervalDefault(t *testing.T) {
	// When FailureLogInterval is 0, newDefaultStrategy should default to 60s.
	// Will fail on NewFileRecords (nil DB) — we're just testing the code path
	// doesn't panic before that point.
	_, err := newDefaultStrategy(Deps{
		DB:        nil,
		TableName: "test_table",
		Log:       zap.NewNop(),
		IsMariaDB: true,
	})
	if err == nil {
		t.Log("newDefaultStrategy unexpectedly succeeded with nil DB")
	}
}

func TestNewDefaultStrategy_CustomFailureLogInterval(t *testing.T) {
	_, err := newDefaultStrategy(Deps{
		DB:                 nil,
		TableName:          "test_table",
		Log:                zap.NewNop(),
		IsMariaDB:          true,
		FailureLogInterval: 5 * time.Second,
	})
	_ = err // expected to fail on NewFileRecords
}

// newMockStrategy creates a defaultStrategy backed by sqlmock for unit tests.
func newMockStrategy(t *testing.T) (*defaultStrategy, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	fr, err := metastore.NewFileRecords(db, "test_table", true, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	reg := storage.NewRegistry()
	return &defaultStrategy{
		fileRecs:           fr,
		storageRegistry:    reg,
		log:                zap.NewNop(),
		interval:           50 * time.Millisecond,
		failureLogInterval: time.Hour,
		deleteRate:         100000,
	}, mock
}

func TestRunOnce_NoExpired(t *testing.T) {
	s, mock := newMockStrategy(t)

	// TransitionExpiredToPurging: 2 UPDATEs (IR + archive)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))

	// DeleteExpiredFiles: BEGIN + SELECT + COMMIT
	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectCommit()

	err := s.runOnce(context.Background())
	if err != nil {
		t.Fatalf("runOnce error: %v", err)
	}
}

func TestRunOnce_WithTransitions(t *testing.T) {
	s, mock := newMockStrategy(t)

	// TransitionExpiredToPurging returns some transitions
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 2))

	// DeleteExpiredFiles: BEGIN + SELECT (returns rows) + DELETE + COMMIT
	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	rows := sqlmock.NewRows(cols).
		AddRow(
			sql.NullString{Valid: true, String: "s3"},
			sql.NullString{Valid: true, String: "logs"},
			sql.NullString{Valid: true, String: "/data/file.ir"},
			sql.NullString{Valid: false},
			sql.NullString{Valid: false},
			sql.NullString{Valid: false},
			[]byte("hash1"),
		)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := s.runOnce(context.Background())
	if err != nil {
		t.Fatalf("runOnce error: %v", err)
	}
}

func TestRunOnce_TransitionError(t *testing.T) {
	s, mock := newMockStrategy(t)

	// First UPDATE fails
	mock.ExpectExec("UPDATE").WillReturnError(errors.New("db error"))

	err := s.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from runOnce")
	}
}

func TestRunOnce_DeleteError(t *testing.T) {
	s, mock := newMockStrategy(t)

	// Transitions succeed
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))

	// DeleteExpiredFiles fails
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query error"))
	mock.ExpectRollback()

	err := s.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from runOnce")
	}
}

func TestRun_TicksAndHandlesError(t *testing.T) {
	s, mock := newMockStrategy(t)
	s.interval = 20 * time.Millisecond

	// First tick: runOnce succeeds (no expired)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	cols := []string{
		"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
		"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
		"clp_ir_path_hash",
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectCommit()

	// Second tick: runOnce fails
	mock.ExpectExec("UPDATE").WillReturnError(errors.New("db down"))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	// Let it run for a couple ticks
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestNewDefaultStrategy_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	s, err := newDefaultStrategy(Deps{
		DB:                 db,
		TableName:          "valid_table",
		Log:                zap.NewNop(),
		IsMariaDB:          true,
		FailureLogInterval: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("newDefaultStrategy error: %v", err)
	}
	if s == nil {
		t.Fatal("strategy is nil")
	}

	ds, ok := s.(*defaultStrategy)
	if !ok {
		t.Fatal("expected *defaultStrategy")
	}
	if ds.failureLogInterval != 30*time.Second {
		t.Errorf("failureLogInterval = %v, want 30s", ds.failureLogInterval)
	}
	if ds.deleteRate != defaultDeleteRate {
		t.Errorf("deleteRate = %d, want %d", ds.deleteRate, defaultDeleteRate)
	}
}

func TestNewDefaultStrategy_ZeroFailureInterval(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	s, err := newDefaultStrategy(Deps{
		DB:                 db,
		TableName:          "valid_table",
		Log:                zap.NewNop(),
		IsMariaDB:          true,
		FailureLogInterval: 0,
	})
	if err != nil {
		t.Fatalf("newDefaultStrategy error: %v", err)
	}
	ds, ok := s.(*defaultStrategy)
	if !ok {
		t.Fatal("expected *defaultStrategy")
	}
	if ds.failureLogInterval != 60*time.Second {
		t.Errorf("failureLogInterval = %v, want 60s (default)", ds.failureLogInterval)
	}
}

func TestNewDefaultStrategy_InvalidTableName(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	_, err = newDefaultStrategy(Deps{
		DB:        db,
		TableName: "invalid table!",
		Log:       zap.NewNop(),
		IsMariaDB: true,
	})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

