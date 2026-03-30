//go:build integration

// Package pipeline runs end-to-end tests using the real clp-s binary.
// Requires clp-s in $PATH. IR test data is generated using clp-ffi-go (KV-IR).
package pipeline

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/consolidation"
	"github.com/y-scope/metalog/taskqueue"
	"github.com/y-scope/metalog/testutil"
	"github.com/y-scope/metalog/worker"
	"github.com/y-scope/metalog/storage"
)

const (
	e2eTable       = "test_e2e"
	irBucket       = "ir-files"
	archBucket     = "archives"
	irFileCount    = 3
	eventsPerFile  = 50
)

// TestConsolidationPipeline runs the complete consolidation pipeline with real
// clp-s compression and generated KV-IR data:
//
//	Phase 0: Generate IR files → upload to MinIO → insert DB records
//	Phase 1: Planner groups files → creates consolidation task
//	Phase 2: Worker downloads IR → clp-s compresses → uploads archive
//	Phase 3: Planner finalizes → ARCHIVE_CLOSED → deletes source IR
func TestConsolidationPipeline(t *testing.T) {
	// --- Prerequisite: clp-s must be available ---
	clpBinary, err := exec.LookPath("clp-s")
	if err != nil {
		t.Skip("clp-s not found in $PATH; skipping e2e test")
	}
	t.Logf("using clp-s: %s", clpBinary)

	// --- Setup infrastructure ---
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, e2eTable)

	mio := testutil.SetupMinIOWithBucket(t)
	defer mio.Teardown(t)
	mio.CreateBucket(t, irBucket)
	mio.CreateBucket(t, archBucket)

	ctx := context.Background()
	log := zap.NewNop()

	reg := storage.NewRegistry()
	reg.Register(testutil.StorageBackendName, mio.Backend)
	tq := taskqueue.NewQueue(mc.DB, log)

	// =====================================================================
	// Phase 0: Generate IR files, upload to MinIO, insert DB records
	// =====================================================================
	irDir := t.TempDir()
	irLocalPaths, err := generateIRFiles(irDir, irFileCount, eventsPerFile)
	if err != nil {
		t.Fatalf("phase 0: generate IR files: %v", err)
	}
	t.Logf("phase 0: generated %d IR files in %s", len(irLocalPaths), irDir)

	baseTs := int64(1704067200000000000) // 2024-01-01
	irKeys := make([]string, len(irLocalPaths))

	for i, localPath := range irLocalPaths {
		irKey := fmt.Sprintf("ir/%s", filepath.Base(localPath))
		irKeys[i] = irKey

		data, err := os.ReadFile(localPath)
		if err != nil {
			t.Fatalf("phase 0: read IR file %s: %v", localPath, err)
		}
		mio.PutObject(t, irBucket, irKey, data)

		query, args, sqlErr := sq.Insert("`"+e2eTable+"`").
			Columns("min_timestamp", "max_timestamp", "clp_ir_path",
				"clp_ir_storage_backend", "clp_ir_bucket",
				"state", "record_count", "retention_days", "expires_at").
			Values(
				baseTs+int64(i)*1_000_000_000,
				baseTs+int64(i)*1_000_000_000+500_000_000,
				irKey,
				testutil.StorageBackendName,
				irBucket,
				"IR_ARCHIVE_CONSOLIDATION_PENDING",
				eventsPerFile, 30, 0,
			).ToSql()
		if sqlErr != nil {
			t.Fatalf("phase 0: build insert SQL: %v", sqlErr)
		}
		if _, err := mc.DB.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("phase 0: insert file %d: %v", i, err)
		}
	}

	// Validate phase 0.
	var pendingCount int
	if err := mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+e2eTable+"` WHERE state = 'IR_ARCHIVE_CONSOLIDATION_PENDING'").
		Scan(&pendingCount); err != nil {
		t.Fatalf("phase 0: query pending count: %v", err)
	}
	if pendingCount != irFileCount {
		t.Fatalf("phase 0: pending files = %d, want %d", pendingCount, irFileCount)
	}
	for _, irKey := range irKeys {
		if !mio.ObjectExists(t, irBucket, irKey) {
			t.Fatalf("phase 0: IR file %q not found in MinIO", irKey)
		}
	}
	t.Logf("phase 0 passed: %d files uploaded and pending", irFileCount)

	// =====================================================================
	// Phase 1: Planner creates consolidation tasks
	// =====================================================================
	planner := newE2EPlanner(t, mc, reg, tq, log)

	plannerCtx, plannerCancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)

	testutil.WaitFor(t, 30*time.Second, "phase 1: planner to create tasks", func() bool {
		counts, err := tq.GetTaskCounts(ctx, e2eTable)
		return err == nil && counts.Pending >= 1
	})
	plannerCancel()

	// Validate phase 1: task exists with correct structure.
	counts, err := tq.GetTaskCounts(ctx, e2eTable)
	if err != nil {
		t.Fatalf("phase 1: get task counts: %v", err)
	}
	if counts.Pending < 1 {
		t.Fatalf("phase 1: pending tasks = %d, want >= 1", counts.Pending)
	}

	tasks, err := tq.ClaimTasks(ctx, e2eTable, "validator", 1)
	if err != nil || len(tasks) < 1 {
		t.Fatalf("phase 1: claim task for validation: %v (tasks=%d)", err, len(tasks))
	}
	task := tasks[0]
	if task.Version != taskqueue.TaskPayloadVersion {
		t.Fatalf("phase 1: task version = %d, want %d", task.Version, taskqueue.TaskPayloadVersion)
	}
	payload, err := taskqueue.UnmarshalPayload(task.Input)
	if err != nil {
		t.Fatalf("phase 1: unmarshal payload: %v", err)
	}
	if payload.Consolidation == nil {
		t.Fatalf("phase 1: payload.Consolidation is nil")
	}
	cons := payload.Consolidation
	if len(cons.IRPaths) != irFileCount {
		t.Fatalf("phase 1: IRPaths = %d, want %d", len(cons.IRPaths), irFileCount)
	}
	if cons.ArchivePath == "" {
		t.Fatalf("phase 1: ArchivePath is empty")
	}
	if cons.ArchiveBackend != testutil.StorageBackendName {
		t.Fatalf("phase 1: ArchiveBackend = %q, want %q", cons.ArchiveBackend, testutil.StorageBackendName)
	}
	if cons.ArchiveBucket != archBucket {
		t.Fatalf("phase 1: ArchiveBucket = %q, want %q", cons.ArchiveBucket, archBucket)
	}

	// Unclaim so worker can pick it up.
	if err := tq.ReclaimTask(ctx, task.TaskID); err != nil {
		t.Fatalf("phase 1: reclaim validated task: %v", err)
	}
	t.Logf("phase 1 passed: task created with %d IR paths, archive %q", len(cons.IRPaths), cons.ArchivePath)

	// =====================================================================
	// Phase 2: Worker compresses with real clp-s
	// =====================================================================
	compressor := storage.NewClpCompressor(clpBinary, 5*time.Minute, log)
	archiveCreator := storage.NewArchiveCreator(reg, compressor, log)

	pf := worker.NewPrefetcher(tq, "e2e-worker", 10, log)
	core := worker.NewCore(tq, archiveCreator, pf, log)

	workerCtx, workerCancel := context.WithCancel(ctx)
	go pf.Run(workerCtx)
	go core.Run(workerCtx)

	testutil.WaitFor(t, 2*time.Minute, "phase 2: worker to complete tasks", func() bool {
		counts, err := tq.GetTaskCounts(ctx, e2eTable)
		return err == nil && counts.Completed >= 1
	})
	workerCancel()
	<-pf.Done()

	// Validate phase 2: task completed, archive exists.
	counts, err = tq.GetTaskCounts(ctx, e2eTable)
	if err != nil {
		t.Fatalf("phase 2: get task counts: %v", err)
	}
	if counts.Completed < 1 {
		t.Fatalf("phase 2: completed tasks = %d, want >= 1", counts.Completed)
	}
	if !mio.ObjectExists(t, archBucket, cons.ArchivePath) {
		t.Fatalf("phase 2: archive %q not found in MinIO bucket %q", cons.ArchivePath, archBucket)
	}
	t.Logf("phase 2 passed: archive exists at %s/%s", archBucket, cons.ArchivePath)

	// =====================================================================
	// Phase 3: Planner finalizes
	// =====================================================================
	planner2 := newE2EPlanner(t, mc, reg, tq, log)

	plannerCtx2, plannerCancel2 := context.WithCancel(ctx)
	go planner2.Run(plannerCtx2)

	// Wait for full cycle: files closed + task row deleted.
	testutil.WaitFor(t, 30*time.Second, "phase 3: planner to finalize", func() bool {
		var closedCount int
		if err := mc.DB.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM `"+e2eTable+"` WHERE state = 'ARCHIVE_CLOSED'").
			Scan(&closedCount); err != nil || closedCount != irFileCount {
			return false
		}
		counts, err := tq.GetTaskCounts(ctx, e2eTable)
		if err != nil {
			return false
		}
		return counts.Pending+counts.Processing+counts.Completed+counts.Failed == 0
	})
	plannerCancel2()

	// Validate phase 3: full cleanup.
	var closedCount int
	if err := mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+e2eTable+"` WHERE state = 'ARCHIVE_CLOSED'").
		Scan(&closedCount); err != nil {
		t.Fatalf("phase 3: query closed count: %v", err)
	}
	if closedCount != irFileCount {
		t.Fatalf("phase 3: ARCHIVE_CLOSED = %d, want %d", closedCount, irFileCount)
	}

	var archivePathCount int
	if err := mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+e2eTable+"` WHERE clp_archive_path IS NOT NULL AND clp_archive_path != ''").
		Scan(&archivePathCount); err != nil {
		t.Fatalf("phase 3: query archive path count: %v", err)
	}
	if archivePathCount != irFileCount {
		t.Fatalf("phase 3: files with archive path = %d, want %d", archivePathCount, irFileCount)
	}

	for _, irKey := range irKeys {
		if mio.ObjectExists(t, irBucket, irKey) {
			t.Errorf("phase 3: source IR %q still exists in MinIO", irKey)
		}
	}

	if !mio.ObjectExists(t, archBucket, cons.ArchivePath) {
		t.Errorf("phase 3: archive %q missing after finalization", cons.ArchivePath)
	}

	counts, err = tq.GetTaskCounts(ctx, e2eTable)
	if err != nil {
		t.Fatalf("phase 3: get task counts: %v", err)
	}
	if total := counts.Pending + counts.Processing + counts.Completed + counts.Failed; total != 0 {
		t.Fatalf("phase 3: remaining tasks = %d, want 0", total)
	}

	t.Logf("phase 3 passed: %d files ARCHIVE_CLOSED, source IR deleted, tasks cleaned", irFileCount)
	t.Log("e2e test passed: full pipeline with generated KV-IR data and real clp-s compression")
}

// --- Helpers ---

func newE2EPlanner(t *testing.T, mc *testutil.DBContainer, reg *storage.Registry, tq *taskqueue.Queue, log *zap.Logger) *consolidation.Planner {
	t.Helper()
	planner, err := consolidation.NewPlanner(consolidation.PlannerConfig{
		DB:                 mc.DB,
		TableName:          e2eTable,
		IsMariaDB:          true,
		Policy:             consolidation.NewTimeWindowPolicy(24*time.Hour, 2, 100),
		InFlight:           consolidation.NewInFlightSet(),
		TaskQueue:          tq,
		StorageRegistry:    reg,
		ArchiveBackend:     testutil.StorageBackendName,
		ArchiveBucket:      archBucket,
		Interval:           500 * time.Millisecond,
		FailureLogInterval: 1 * time.Minute,
		StaleThreshold:     60 * time.Minute,
		Log:                log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return planner
}
