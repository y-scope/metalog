//go:build integration

package consolidation_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/coordinator/consolidation"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/testutil"
	"github.com/y-scope/metalog/internal/worker"
	"github.com/y-scope/metalog/storage"
)

const pipelineTable = "test_pipeline"

// pipelineEnv holds the shared test infrastructure for end-to-end pipeline tests.
type pipelineEnv struct {
	db  *sql.DB
	mc  *testutil.MariaDBContainer
	mio *testutil.MinIOContainer
	log *zap.Logger
}

func setupPipelineEnv(t *testing.T) *pipelineEnv {
	t.Helper()

	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, pipelineTable)

	mio := testutil.SetupMinIOWithBucket(t)

	return &pipelineEnv{
		db:  mc.DB,
		mc:  mc,
		mio: mio,
		log: zap.NewNop(),
	}
}

func (env *pipelineEnv) teardown(t *testing.T) {
	t.Helper()
	env.mc.Teardown(t)
	env.mio.Teardown(t)
}

// insertPendingFiles inserts file records in CONSOLIDATION_PENDING state
// and uploads dummy IR files to MinIO. Returns the IR paths.
func (env *pipelineEnv) insertPendingFiles(t *testing.T, count int) []string {
	t.Helper()
	ctx := context.Background()
	baseTs := int64(1704067200000000000) // 2024-01-01

	irPaths := make([]string, count)
	for i := 0; i < count; i++ {
		irPath := "/data/pipeline_" + string(rune('a'+i)) + ".ir"
		irPaths[i] = irPath

		// Insert file record into DB.
		query, args, _ := sq.Insert("`"+pipelineTable+"`").
			Columns("min_timestamp", "max_timestamp", "clp_ir_path",
				"clp_ir_storage_backend", "clp_ir_bucket",
				"state", "record_count", "retention_days", "expires_at").
			Values(
				baseTs+int64(i)*1000000,
				baseTs+int64(i)*1000000+500000,
				irPath,
				testutil.StorageBackendName,
				testutil.TestBucket,
				"IR_ARCHIVE_CONSOLIDATION_PENDING",
				10, 30, 0,
			).ToSql()
		_, err := env.db.ExecContext(ctx, query, args...)
		if err != nil {
			t.Fatalf("insert file %d: %v", i, err)
		}

		// Upload dummy IR file to MinIO.
		env.mio.PutObject(t, testutil.TestBucket, irPath, []byte("dummy-ir-content"))
	}

	return irPaths
}

// newPlanner creates a planner and task queue wired to the test environment.
func (env *pipelineEnv) newPlanner(t *testing.T) (*consolidation.Planner, *taskqueue.Queue) {
	t.Helper()
	inFlight := consolidation.NewInFlightSet()
	policy := consolidation.NewTimeWindowPolicy(24*time.Hour, 2, 100)
	tq := taskqueue.NewQueue(env.db, env.log)
	reg := env.mio.Registry(testutil.StorageBackendName)

	planner, err := consolidation.NewPlanner(consolidation.PlannerConfig{
		DB:                 env.db,
		TableName:          pipelineTable,
		IsMariaDB:          true,
		Policy:             policy,
		InFlight:           inFlight,
		TaskQueue:          tq,
		StorageRegistry:    reg,
		ArchiveBackend:     testutil.StorageBackendName,
		ArchiveBucket:      testutil.TestBucket,
		Interval:           1 * time.Second,
		FailureLogInterval: 1 * time.Minute,
		StaleThreshold:     60 * time.Minute,
		Log:                env.log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return planner, tq
}

// TestPipeline_EndToEnd tests the full consolidation lifecycle:
// insert pending files + IR objects → planner creates task →
// worker downloads IR, compresses, uploads archive, completes task →
// planner finalizes (marks ARCHIVE_CLOSED, deletes source IR, cleans task row).
func TestPipeline_EndToEnd(t *testing.T) {
	env := setupPipelineEnv(t)
	defer env.teardown(t)
	ctx := context.Background()

	irPaths := env.insertPendingFiles(t, 3)
	planner, tq := env.newPlanner(t)

	// Run planner to create tasks.
	plannerCtx, cancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)
	time.Sleep(3 * time.Second)
	cancel()

	// Verify task was created.
	counts, err := tq.GetTaskCounts(ctx, pipelineTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending < 1 {
		t.Fatalf("pending tasks = %d, want >= 1", counts.Pending)
	}

	// Run worker with ConcatCompressor (no real clp-s needed).
	reg := env.mio.Registry(testutil.StorageBackendName)
	archiveCreator := storage.NewArchiveCreator(reg, &testutil.ConcatCompressor{}, env.log)

	pf := worker.NewPrefetcher(tq, "test-worker", 10, env.log)
	core := worker.NewCore(tq, archiveCreator, pf, env.log)

	workerCtx, workerCancel := context.WithCancel(ctx)
	go pf.Run(workerCtx)
	go core.Run(workerCtx)
	time.Sleep(5 * time.Second)
	workerCancel()

	// Wait for prefetcher to finish.
	<-pf.Done()

	// Verify task is completed.
	counts, err = tq.GetTaskCounts(ctx, pipelineTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Completed < 1 {
		t.Fatalf("completed tasks = %d, want >= 1", counts.Completed)
	}

	// Run planner again to finalize — processCompletedTasks should:
	// 1. Mark files ARCHIVE_CLOSED
	// 2. Delete source IR files from MinIO
	// 3. Delete the task row
	planner2, tq2 := env.newPlanner(t)
	plannerCtx2, cancel2 := context.WithCancel(ctx)
	go planner2.Run(plannerCtx2)
	time.Sleep(3 * time.Second)
	cancel2()

	// Verify files are now ARCHIVE_CLOSED.
	var closedCount int
	err = env.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+pipelineTable+"` WHERE state = 'ARCHIVE_CLOSED'").
		Scan(&closedCount)
	if err != nil {
		t.Fatal(err)
	}
	if closedCount != 3 {
		t.Errorf("ARCHIVE_CLOSED files = %d, want 3", closedCount)
	}

	// Verify archive exists in MinIO.
	var archivePath string
	err = env.db.QueryRowContext(ctx,
		"SELECT clp_archive_path FROM `"+pipelineTable+"` WHERE clp_archive_path IS NOT NULL AND clp_archive_path != '' LIMIT 1").
		Scan(&archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if archivePath == "" {
		t.Fatal("archive path not set on file record")
	}
	if !env.mio.ObjectExists(t, testutil.TestBucket, archivePath) {
		t.Errorf("archive %q not found in MinIO", archivePath)
	}

	// Verify source IR files were deleted from MinIO.
	for _, path := range irPaths {
		if env.mio.ObjectExists(t, testutil.TestBucket, path) {
			t.Errorf("source IR %q still exists in MinIO (should be deleted after finalization)", path)
		}
	}

	// Verify task row was deleted.
	counts, err = tq2.GetTaskCounts(ctx, pipelineTable)
	if err != nil {
		t.Fatal(err)
	}
	totalTasks := counts.Pending + counts.Processing + counts.Completed + counts.Failed
	if totalTasks != 0 {
		t.Errorf("remaining tasks = %d, want 0 (task should be deleted after finalization)", totalTasks)
	}
}
