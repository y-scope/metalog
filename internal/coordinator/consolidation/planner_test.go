package consolidation

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/taskqueue"
)

// --- Mocks ---

type mockFileRecords struct {
	pendingRecords []*metastore.FileRecord
	pendingErr     error
	promoteCount   int64
	promoteErr     error
	closedCalls    int
	closedErr      error
}

func (m *mockFileRecords) FindConsolidationPending(_ context.Context, _, _ []metastore.ColumnMapping) ([]*metastore.FileRecord, error) {
	return m.pendingRecords, m.pendingErr
}
func (m *mockFileRecords) PromoteStuckBuffering(_ context.Context, _ int64) (int64, error) {
	return m.promoteCount, m.promoteErr
}
func (m *mockFileRecords) MarkArchiveClosed(_ context.Context, _ []string, _, _, _ string, _, _ int64) error {
	m.closedCalls++
	return m.closedErr
}

type mockTaskStore struct {
	createdTasks   [][]byte
	createErr      error
	nextTaskID     int64
	terminalTasks  []taskqueue.TerminalTask
	terminalErr    error
	deletedTaskIDs []int64
	staleTasks     []*taskqueue.Task
	staleErr       error
	reclaimErr     error
	cleanupCount   int64
	cleanupErr     error
}

func (m *mockTaskStore) CountActiveTasks(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (m *mockTaskStore) CreateTasks(_ context.Context, _ string, _ uint8, inputs [][]byte) (int64, error) {
	m.createdTasks = append(m.createdTasks, inputs...)
	m.nextTaskID += int64(len(inputs))
	return int64(len(inputs)), m.createErr
}
func (m *mockTaskStore) CleanupOldTasks(_ context.Context, _ string, _ time.Duration) (int64, error) {
	return m.cleanupCount, m.cleanupErr
}
func (m *mockTaskStore) FindStaleTasks(_ context.Context, _ string, _ time.Duration) ([]*taskqueue.Task, error) {
	return m.staleTasks, m.staleErr
}
func (m *mockTaskStore) ReclaimTask(_ context.Context, _ int64) error {
	return m.reclaimErr
}
func (m *mockTaskStore) FindTerminalTasks(_ context.Context, _ string, _ int) ([]taskqueue.TerminalTask, error) {
	return m.terminalTasks, m.terminalErr
}
func (m *mockTaskStore) DeleteTerminalTask(_ context.Context, taskID int64) error {
	m.deletedTaskIDs = append(m.deletedTaskIDs, taskID)
	return nil
}

type staticResolver struct{}

func (s *staticResolver) ResolveDim(_ string) string                  { return "" }
func (s *staticResolver) ResolveAgg(_, _, _ string) string            { return "" }

// --- Helpers ---

func newTestPlanner(fr fileRecordStore, ts taskStore) *Planner {
	return &Planner{
		tableName:       "test_table",
		policy:          NewTimeWindowPolicy(24*time.Hour, 2, 100),
		inFlight:        NewInFlightSet(),
		tasks:           ts,
		fileRecs:        fr,
		archiveBackend:  "s3",
		archiveBucket:   "archives",
		interval:        time.Second,
		staleThreshold:  time.Hour,
		resolver:        &staticResolver{},
		log:             zap.NewNop(),
	}
}

func makePendingRecords(n int) []*metastore.FileRecord {
	records := make([]*metastore.FileRecord, n)
	baseTs := int64(1704067200000000000) // 2024-01-01
	for i := range records {
		records[i] = &metastore.FileRecord{
			ID:                  int64(i + 1),
			MinTimestamp:        baseTs + int64(i)*1000000,
			MaxTimestamp:        baseTs + int64(i)*1000000 + 500000,
			ClpIRStorageBackend: sql.NullString{String: "minio", Valid: true},
			ClpIRBucket:         sql.NullString{String: "logs", Valid: true},
			ClpIRPath:           sql.NullString{String: fmt.Sprintf("/data/file_%d.ir", i), Valid: true},
			State:               metastore.StateIRArchiveConsolidationPending,
		}
	}
	return records
}

// --- Tests ---

func TestPlanOnce_BackpressureSkipsTaskCreation(t *testing.T) {
	fr := &mockFileRecords{}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)
	p.activeTaskCount = 110 // >= maxBackpressureDepth (100)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ts.createdTasks) != 0 {
		t.Errorf("created %d tasks, want 0 (backpressure should skip)", len(ts.createdTasks))
	}
}

func TestPlanOnce_NoCandidatesNoTasks(t *testing.T) {
	fr := &mockFileRecords{pendingRecords: nil}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ts.createdTasks) != 0 {
		t.Errorf("created %d tasks, want 0 (no candidates)", len(ts.createdTasks))
	}
}

func TestPlanOnce_BelowMinFilesNoTasks(t *testing.T) {
	// Only 1 file, but policy requires min 2 per group.
	fr := &mockFileRecords{pendingRecords: makePendingRecords(1)}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ts.createdTasks) != 0 {
		t.Errorf("created %d tasks, want 0 (below minFiles)", len(ts.createdTasks))
	}
}

func TestPlanOnce_CreatesTaskForEligibleGroup(t *testing.T) {
	fr := &mockFileRecords{pendingRecords: makePendingRecords(5)}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ts.createdTasks) != 1 {
		t.Fatalf("created %d tasks, want 1", len(ts.createdTasks))
	}

	// Verify payload structure.
	payload, err := taskqueue.UnmarshalPayload(ts.createdTasks[0])
	if err != nil {
		t.Fatal(err)
	}
	if payload.TableName != "test_table" {
		t.Errorf("TableName = %q, want test_table", payload.TableName)
	}
	if payload.Consolidation == nil {
		t.Fatal("Consolidation is nil")
	}
	cons := payload.Consolidation
	if len(cons.IRPaths) != 5 {
		t.Errorf("IRPaths = %d, want 5", len(cons.IRPaths))
	}
	if len(cons.FileIDs) != 5 {
		t.Errorf("FileIDs = %d, want 5", len(cons.FileIDs))
	}
	if cons.ArchiveBackend != "s3" {
		t.Errorf("ArchiveBackend = %q, want s3", cons.ArchiveBackend)
	}
	if cons.ArchiveBucket != "archives" {
		t.Errorf("ArchiveBucket = %q, want archives", cons.ArchiveBucket)
	}
	if cons.ArchivePath == "" {
		t.Error("ArchivePath is empty, want UUIDv7-based path")
	}
	if len(cons.ArchivePath) < 10 || cons.ArchivePath[len(cons.ArchivePath)-8:] != ".clp.zst" {
		t.Errorf("ArchivePath = %q, want *.clp.zst suffix", cons.ArchivePath)
	}
}

func TestPlanOnce_InFlightDedup(t *testing.T) {
	records := makePendingRecords(3)
	fr := &mockFileRecords{pendingRecords: records}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)

	// Pre-add paths to in-flight set.
	paths := make([]string, len(records))
	for i, r := range records {
		paths[i] = r.ClpIRPath.String
	}
	p.inFlight.TryAdd(paths)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ts.createdTasks) != 0 {
		t.Errorf("created %d tasks, want 0 (paths already in-flight)", len(ts.createdTasks))
	}
}

func TestPlanOnce_StaleThresholdZeroSkipsPromotion(t *testing.T) {
	fr := &mockFileRecords{pendingRecords: nil}
	ts := &mockTaskStore{}
	p := newTestPlanner(fr, ts)
	p.staleThreshold = 0 // disabled

	// If promotion were called, promoteCount would be non-zero. Since
	// threshold is 0, it should be skipped entirely.
	fr.promoteErr = fmt.Errorf("should not be called")

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPlanOnce_ProcessCompletedTasksClearsInFlight(t *testing.T) {
	cons := &taskqueue.ConsolidationPayload{
		IRPaths:        []string{"/data/a.ir", "/data/b.ir"},
		ArchiveBackend: "s3",
		ArchiveBucket:  "archives",
		ArchivePath:    "test.clp.zst",
	}
	payload := &taskqueue.TaskPayload{
		TableName:     "test_table",
		Consolidation: cons,
	}
	input, _ := taskqueue.MarshalPayload(payload)

	result := &taskqueue.TaskResult{
		ArchivePath:      "test.clp.zst",
		ArchiveSizeBytes: 1024,
	}
	output, _ := taskqueue.MarshalResult(result)

	fr := &mockFileRecords{pendingRecords: nil}
	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 42, Input: input, Output: output},
		},
	}
	p := newTestPlanner(fr, ts)
	p.activeTaskCount = 1 // one active task to be processed

	// Pre-add paths to in-flight.
	p.inFlight.TryAdd(cons.IRPaths)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// In-flight should be cleared after processing.
	if p.inFlight.TryAdd(cons.IRPaths) != true {
		t.Error("in-flight paths not cleared after processing completed task")
	}

	// Task should be deleted.
	if len(ts.deletedTaskIDs) != 1 || ts.deletedTaskIDs[0] != 42 {
		t.Errorf("deleted task IDs = %v, want [42]", ts.deletedTaskIDs)
	}

	// MarkArchiveClosed should be called.
	if fr.closedCalls != 1 {
		t.Errorf("MarkArchiveClosed called %d times, want 1", fr.closedCalls)
	}

	// Active task count should be decremented.
	if p.activeTaskCount != 0 {
		t.Errorf("activeTaskCount = %d, want 0", p.activeTaskCount)
	}
}

func TestPlanOnce_FailedTaskFreesInFlight(t *testing.T) {
	cons := &taskqueue.ConsolidationPayload{
		IRPaths: []string{"/data/failed.ir"},
	}
	payload := &taskqueue.TaskPayload{
		TableName:     "test_table",
		Consolidation: cons,
	}
	input, _ := taskqueue.MarshalPayload(payload)

	fr := &mockFileRecords{pendingRecords: nil}
	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 99, Input: input, Output: nil}, // nil output = failed/dead-letter
		},
	}
	p := newTestPlanner(fr, ts)
	p.activeTaskCount = 1
	p.inFlight.TryAdd(cons.IRPaths)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// In-flight should be freed.
	if p.inFlight.TryAdd(cons.IRPaths) != true {
		t.Error("in-flight paths not freed after failed task")
	}

	// MarkArchiveClosed should NOT be called.
	if fr.closedCalls != 0 {
		t.Errorf("MarkArchiveClosed called %d times, want 0 (failed task)", fr.closedCalls)
	}

	// Active task count should be decremented.
	if p.activeTaskCount != 0 {
		t.Errorf("activeTaskCount = %d, want 0", p.activeTaskCount)
	}
}

func TestPlanOnce_MarkArchiveClosedFailureKeepsInFlight(t *testing.T) {
	cons := &taskqueue.ConsolidationPayload{
		IRPaths:        []string{"/data/retry.ir"},
		ArchiveBackend: "s3",
		ArchiveBucket:  "archives",
		ArchivePath:    "retry.clp.zst",
	}
	payload := &taskqueue.TaskPayload{
		TableName:     "test_table",
		Consolidation: cons,
	}
	input, _ := taskqueue.MarshalPayload(payload)

	result := &taskqueue.TaskResult{
		ArchivePath:      "retry.clp.zst",
		ArchiveSizeBytes: 1024,
	}
	output, _ := taskqueue.MarshalResult(result)

	fr := &mockFileRecords{
		closedErr: fmt.Errorf("simulated DB error"),
	}
	ts := &mockTaskStore{
		terminalTasks: []taskqueue.TerminalTask{
			{TaskID: 55, Input: input, Output: output},
		},
	}
	p := newTestPlanner(fr, ts)
	p.activeTaskCount = 1
	p.inFlight.TryAdd(cons.IRPaths)

	if err := p.planOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// In-flight should NOT be freed (task will be retried next cycle).
	if p.inFlight.TryAdd(cons.IRPaths) {
		t.Error("in-flight paths should still be held after MarkArchiveClosed failure")
	}

	// Task should NOT be deleted.
	if len(ts.deletedTaskIDs) != 0 {
		t.Errorf("deleted task IDs = %v, want [] (should not delete on MarkArchiveClosed failure)", ts.deletedTaskIDs)
	}

	// Active task count should NOT be decremented.
	if p.activeTaskCount != 1 {
		t.Errorf("activeTaskCount = %d, want 1 (should not decrement on MarkArchiveClosed failure)", p.activeTaskCount)
	}
}

