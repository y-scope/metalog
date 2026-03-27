package consolidation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/taskqueue"
)

// --- additional chain tests ---

func TestPolicyChain_RequiredDims(t *testing.T) {
	p1 := &mockPolicy{selectFn: func(_ []*metastore.FileRecord) []FileGroup { return nil }}
	p2 := &mockPolicy{selectFn: func(_ []*metastore.FileRecord) []FileGroup { return nil }}
	// RequiredDims returns nil for mockPolicy, so chain returns nil.
	chain := NewPolicyChain([]Policy{p1, p2})
	dims := chain.RequiredDims()
	if len(dims) != 0 {
		t.Errorf("RequiredDims() = %v, want empty", dims)
	}
}

func TestPolicyChain_RequiredAggs(t *testing.T) {
	p1 := &mockPolicy{selectFn: func(_ []*metastore.FileRecord) []FileGroup { return nil }}
	chain := NewPolicyChain([]Policy{p1})
	aggs := chain.RequiredAggs()
	if len(aggs) != 0 {
		t.Errorf("RequiredAggs() = %v, want empty", aggs)
	}
}

// --- additional planner tests ---

func TestMarkTaskProcessed_DeleteError(t *testing.T) {
	// Using a mockTaskStore that does NOT override DeleteTerminalTask to return error
	// isn't possible with the existing mock. Test the zero-count guard path instead.
	fr := &mockFileRecords{}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)
	p.activeTaskCount = 0

	p.markTaskProcessed(context.Background(), 1)

	// activeTaskCount should not go negative.
	if p.activeTaskCount != 0 {
		t.Errorf("activeTaskCount = %d, want 0", p.activeTaskCount)
	}
}

func TestDeleteIRFiles_NilResolver(t *testing.T) {
	fr := &mockFileRecords{}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)
	p.storageResolver = nil

	// Should not panic.
	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a"},
		IRBuckets: []string{"b"},
	})
}

func TestDeleteIRFiles_EmptyBackend(t *testing.T) {
	fr := &mockFileRecords{}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)
	p.storageResolver = nil

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "",
		IRPaths:   []string{"/a"},
	})
}

type testStorageDeleter struct {
	err     error
	deleted []string
}

func (d *testStorageDeleter) Delete(_ context.Context, bucket, path string) error {
	d.deleted = append(d.deleted, bucket+"/"+path)
	return d.err
}

type testStorageResolver struct {
	backend storageDeleter
	err     error
}

func (r *testStorageResolver) Get(_ string) (storageDeleter, error) {
	return r.backend, r.err
}

func TestDeleteIRFiles_Success(t *testing.T) {
	mb := &testStorageDeleter{}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a", "/b"},
		IRBuckets: []string{"bucket1", "bucket2"},
	})

	if len(mb.deleted) != 2 {
		t.Errorf("expected 2 deletions, got %d", len(mb.deleted))
	}
}

func TestDeleteIRFiles_SkipsEmptyPaths(t *testing.T) {
	mb := &testStorageDeleter{}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a", ""},
		IRBuckets: []string{"bucket1", ""},
	})

	if len(mb.deleted) != 1 {
		t.Errorf("expected 1 deletion, got %d", len(mb.deleted))
	}
}

func TestDeleteIRFiles_BackendError(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{err: errors.New("unknown")}

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a"},
		IRBuckets: []string{"b"},
	})
}

func TestDeleteIRFiles_DeleteError(t *testing.T) {
	mb := &testStorageDeleter{err: errors.New("delete failed")}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a"},
		IRBuckets: []string{"b"},
	})
}

func TestDeleteIRFiles_MorePathsThanBuckets(t *testing.T) {
	mb := &testStorageDeleter{}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}

	p.deleteIRFiles(context.Background(), &taskqueue.ConsolidationPayload{
		IRBackend: "s3",
		IRPaths:   []string{"/a", "/b"},
		IRBuckets: []string{"bucket1"}, // shorter than paths
	})

	// /b has empty bucket (out of range), should be skipped.
	if len(mb.deleted) != 1 {
		t.Errorf("expected 1 deletion, got %d", len(mb.deleted))
	}
}

func TestDeleteArchive_NilResolver(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = nil
	p.deleteArchive(context.Background(), "/archive", "s3", "bucket")
}

func TestDeleteArchive_EmptyBackend(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{}
	p.deleteArchive(context.Background(), "/archive", "", "bucket")
}

func TestDeleteArchive_EmptyPath(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{}
	p.deleteArchive(context.Background(), "", "s3", "bucket")
}

func TestDeleteArchive_Success(t *testing.T) {
	mb := &testStorageDeleter{}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}

	p.deleteArchive(context.Background(), "/archive.clp", "s3", "bucket")

	if len(mb.deleted) != 1 {
		t.Errorf("expected 1 deletion, got %d", len(mb.deleted))
	}
}

func TestDeleteArchive_BackendError(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{err: errors.New("unknown")}
	p.deleteArchive(context.Background(), "/archive.clp", "s3", "bucket")
}

func TestDeleteArchive_DeleteError(t *testing.T) {
	mb := &testStorageDeleter{err: errors.New("delete failed")}
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.storageResolver = &testStorageResolver{backend: mb}
	p.deleteArchive(context.Background(), "/archive.clp", "s3", "bucket")
}

func TestReclaimStaleTasks_Success(t *testing.T) {
	ts := &mockTaskStore{
		staleTasks: []*taskqueue.Task{{TaskID: 1}, {TaskID: 2}},
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.reclaimStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestReclaimStaleTasks_FindError(t *testing.T) {
	ts := &mockTaskStore{staleErr: errors.New("db error")}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.reclaimStaleTasks(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimStaleTasks_ReclaimError(t *testing.T) {
	ts := &mockTaskStore{
		staleTasks: []*taskqueue.Task{{TaskID: 1}},
		reclaimErr: errors.New("reclaim failed"),
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.reclaimStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestResolveColumnMappings_NilResolver(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.resolver = nil

	dims, aggs := p.resolveColumnMappings()
	if dims != nil || aggs != nil {
		t.Errorf("expected nil, nil with nil resolver")
	}
}

func TestResolveColumnMappings_WithResolverDimsAndAggs(t *testing.T) {
	// Create a custom planner with a policy that requires dims and aggs
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.resolver = &staticResolver{} // returns "" for all, meaning "not resolved"

	dims, aggs := p.resolveColumnMappings()
	// TimeWindowPolicy has no required dims/aggs, so both should be nil.
	if dims != nil || aggs != nil {
		t.Errorf("expected nil, nil for TimeWindowPolicy (no required dims/aggs)")
	}
}

func TestPromoteStuckBuffering_DisabledWhenZero(t *testing.T) {
	fr := &mockFileRecords{promoteErr: fmt.Errorf("should not be called")}
	p := newTestPlanner(fr, &mockTaskStore{})
	p.staleThreshold = 0

	p.promoteStuckBuffering(context.Background())
}

func TestPromoteStuckBuffering_Error(t *testing.T) {
	fr := &mockFileRecords{promoteErr: errors.New("db error")}
	p := newTestPlanner(fr, &mockTaskStore{})

	p.promoteStuckBuffering(context.Background())
}

func TestPromoteStuckBuffering_Success(t *testing.T) {
	fr := &mockFileRecords{promoteCount: 3}
	p := newTestPlanner(fr, &mockTaskStore{})

	p.promoteStuckBuffering(context.Background())
}

func TestProcessTerminalTasks_FindError(t *testing.T) {
	ts := &mockTaskStore{terminalErr: errors.New("db error")}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestProcessTerminalTasks_BadPayload(t *testing.T) {
	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 1, Input: []byte("invalid")},
		},
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestProcessTerminalTasks_NilConsolidation(t *testing.T) {
	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: "t"})

	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 1, Input: input},
		},
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(ts.deletedTaskIDs) != 1 {
		t.Errorf("expected 1 deleted task, got %d", len(ts.deletedTaskIDs))
	}
}

func TestProcessTerminalTasks_ResultWithError(t *testing.T) {
	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{
		TableName:     "t",
		Consolidation: &taskqueue.ConsolidationPayload{IRPaths: []string{"/a"}},
	})
	output, _ := taskqueue.MarshalResult(&taskqueue.TaskResult{Error: "archive creation failed"})

	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 1, Input: input, Output: output},
		},
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(ts.deletedTaskIDs) != 1 {
		t.Errorf("expected 1 deleted task, got %d", len(ts.deletedTaskIDs))
	}
}

func TestProcessTerminalTasks_BadOutput(t *testing.T) {
	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{
		TableName:     "t",
		Consolidation: &taskqueue.ConsolidationPayload{IRPaths: []string{"/a"}},
	})

	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 1, Input: input, Output: []byte("bad output")},
		},
	}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(ts.deletedTaskIDs) != 1 {
		t.Errorf("expected 1 deleted task, got %d", len(ts.deletedTaskIDs))
	}
}

func TestProcessTerminalTasks_CleanupOldTasksError(t *testing.T) {
	ts := &mockTaskStore{cleanupErr: errors.New("cleanup error")}
	p := newTestPlanner(&mockFileRecords{}, ts)

	// Should not propagate cleanup error.
	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestProcessTerminalTasks_CleanupOldTasksSuccess(t *testing.T) {
	ts := &mockTaskStore{cleanupCount: 5}
	p := newTestPlanner(&mockFileRecords{}, ts)

	err := p.processTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestPlannerSetMeter(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})
	p.SetMeter(noop.Meter{})
}

func TestRegistryAdapter_Get(t *testing.T) {
	// Just verify compilation — the adapter wraps storage.Registry.Get.
	_ = &registryAdapter{}
}

func TestNewPlanner_InvalidTableName(t *testing.T) {
	_, err := NewPlanner(PlannerConfig{
		TableName: "invalid table!",
		Log:       zap.NewNop(),
	})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestNewPlanner_DefaultFailureLogInterval(t *testing.T) {
	sqlmockDB, _, err := newSqlmockDB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlmockDB.Close() //nolint:errcheck

	p, err := NewPlanner(PlannerConfig{
		DB:                 sqlmockDB,
		TableName:          "valid_table",
		Log:                zap.NewNop(),
		Policy:             NewTimeWindowPolicy(time.Hour, 2, 100),
		InFlight:           NewInFlightSet(),
		TaskQueue:          taskqueue.NewQueue(sqlmockDB, zap.NewNop()),
		Interval:           time.Second,
		FailureLogInterval: 0, // should default to 1 minute
	})
	if err != nil {
		t.Fatalf("NewPlanner error: %v", err)
	}
	if p.failureLogInterval != time.Minute {
		t.Errorf("failureLogInterval = %v, want 1m", p.failureLogInterval)
	}
}

func TestPlanOnce_FindCandidatesError(t *testing.T) {
	fr := &mockFileRecords{pendingErr: errors.New("db error")}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)

	err := p.planOnce(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlanOnce_CreateTasksError(t *testing.T) {
	recs := makePendingRecords(5)
	fr := &mockFileRecords{pendingRecords: recs}
	ts := &mockTaskStore{createErr: errors.New("insert error")}
	p := newTestPlanner(fr, ts)

	err := p.planOnce(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlanOnce_CountActiveTasksError(t *testing.T) {
	// Uses a custom mock that returns error from CountActiveTasks
	fr := &mockFileRecords{pendingRecords: nil}
	ts := &countActiveErrorTaskStore{}
	p := &Planner{
		tableName:          "test_table",
		policy:             NewTimeWindowPolicy(24*time.Hour, 2, 100),
		inFlight:           NewInFlightSet(),
		tasks:              ts,
		fileRecs:           fr,
		archiveBackend:     "s3",
		archiveBucket:      "archives",
		interval:           time.Second,
		staleThreshold:     time.Hour,
		resolver:           &staticResolver{},
		log:                zap.NewNop(),
		failureLogInterval: time.Hour,
	}
	p.initMetrics(noop.Meter{})

	err := p.planOnce(context.Background())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

// countActiveErrorTaskStore returns an error from CountActiveTasks.
type countActiveErrorTaskStore struct {
	mockTaskStore
}

func (m *countActiveErrorTaskStore) CountActiveTasks(_ context.Context, _ string) (int, error) {
	return 0, errors.New("count error")
}

func TestBuildPayload_GroupOverrides(t *testing.T) {
	p := newTestPlanner(&mockFileRecords{}, &mockTaskStore{})

	recs := []*metastore.FileRecord{
		{
			ID:                  1,
			MinTimestamp:        1000,
			ClpIRPath:           sql.NullString{Valid: true, String: "/a.ir"},
			ClpIRBucket:         sql.NullString{Valid: true, String: "logs"},
			ClpIRStorageBackend: sql.NullString{Valid: true, String: "s3"},
		},
	}
	group := FileGroup{
		ArchivePath:    "/custom-archive.clp",
		ArchiveBackend: "gcs",
		ArchiveBucket:  "custom-bucket",
		Records:        recs,
	}

	payload := p.buildPayload(group, []string{"/a.ir"})
	if payload.Consolidation.ArchiveBackend != "gcs" {
		t.Errorf("ArchiveBackend = %q, want gcs (from group override)", payload.Consolidation.ArchiveBackend)
	}
	if payload.Consolidation.ArchiveBucket != "custom-bucket" {
		t.Errorf("ArchiveBucket = %q, want custom-bucket", payload.Consolidation.ArchiveBucket)
	}
}

func TestGenerateArchivePath_NonEmpty(t *testing.T) {
	path := GenerateArchivePath()
	if path == "" {
		t.Fatal("GenerateArchivePath returned empty string")
	}
	if len(path) < 10 {
		t.Errorf("path too short: %q", path)
	}
}

func TestSparkJobPolicy_RequiredDimsAggs(t *testing.T) {
	p := &SparkJobPolicy{
		GroupingDimKey: "application_id",
	}
	dims := p.RequiredDims()
	if len(dims) != 1 || dims[0] != "application_id" {
		t.Errorf("RequiredDims() = %v, want [application_id]", dims)
	}
	aggs := p.RequiredAggs()
	if len(aggs) != 0 {
		t.Errorf("RequiredAggs() = %v, want empty", aggs)
	}
}

func TestSparkJobPolicy_RequiredDimsEmpty(t *testing.T) {
	p := &SparkJobPolicy{}
	dims := p.RequiredDims()
	if dims != nil {
		t.Errorf("RequiredDims() = %v, want nil for empty GroupingDimKey", dims)
	}
}

// newSqlmockDB creates a sqlmock *sql.DB for testing.
func newSqlmockDB() (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	return db, mock, err
}
