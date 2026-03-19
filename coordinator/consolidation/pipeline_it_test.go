//go:build integration

package consolidation_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/consolidation"
	"github.com/y-scope/metalog/taskqueue"
	"github.com/y-scope/metalog/pkg/testutil"
	"github.com/y-scope/metalog/worker"
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
		irPath := fmt.Sprintf("/data/pipeline_%d.ir", i)
		irPaths[i] = irPath

		query, args, err := sq.Insert("`"+pipelineTable+"`").
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
		if err != nil {
			t.Fatalf("build insert SQL: %v", err)
		}
		if _, err := env.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("insert file %d: %v", i, err)
		}

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
		Interval:           500 * time.Millisecond,
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

	// --- Setup: insert 3 pending files with IR objects in MinIO ---
	irPaths := env.insertPendingFiles(t, 3)

	// --- Phase 1: Planner creates tasks ---
	planner, tq := env.newPlanner(t)
	plannerCtx, plannerCancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)

	testutil.WaitFor(t, 15*time.Second, "planner to create tasks", func() bool {
		counts, err := tq.GetTaskCounts(ctx, pipelineTable)
		return err == nil && counts.Pending >= 1
	})
	plannerCancel()

	// --- Phase 2: Worker processes tasks ---
	reg := env.mio.Registry(testutil.StorageBackendName)
	archiveCreator := storage.NewArchiveCreator(reg, &testutil.ConcatCompressor{}, env.log)

	pf := worker.NewPrefetcher(tq, "test-worker", 10, env.log)
	core := worker.NewCore(tq, archiveCreator, pf, env.log)

	workerCtx, workerCancel := context.WithCancel(ctx)
	go pf.Run(workerCtx)
	go core.Run(workerCtx)

	testutil.WaitFor(t, 15*time.Second, "worker to complete tasks", func() bool {
		counts, err := tq.GetTaskCounts(ctx, pipelineTable)
		return err == nil && counts.Completed >= 1
	})
	workerCancel()
	<-pf.Done()

	// --- Phase 3: Planner finalizes ---
	planner2, tq2 := env.newPlanner(t)
	plannerCtx2, plannerCancel2 := context.WithCancel(ctx)
	go planner2.Run(plannerCtx2)

	// Wait for full cleanup: state transition, IR deletion, and task row removal.
	// The planner performs these in separate loop iterations, so we must wait for
	// all three before canceling.
	testutil.WaitFor(t, 30*time.Second, "planner to finalize (ARCHIVE_CLOSED + IR deleted + tasks cleaned)", func() bool {
		var closedCount int
		err := env.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM `"+pipelineTable+"` WHERE state = 'ARCHIVE_CLOSED'").
			Scan(&closedCount)
		if err != nil || closedCount != 3 {
			return false
		}

		// Check IR files are deleted
		for _, path := range irPaths {
			if env.mio.ObjectExists(t, testutil.TestBucket, path) {
				return false
			}
		}

		// Check task rows are cleaned up
		counts, err := tq2.GetTaskCounts(ctx, pipelineTable)
		if err != nil {
			return false
		}
		total := counts.Pending + counts.Processing + counts.Completed + counts.Failed
		return total == 0
	})
	plannerCancel2()

	// --- Verify final state ---
	var closedCount int
	err := env.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+pipelineTable+"` WHERE state = 'ARCHIVE_CLOSED'").
		Scan(&closedCount)
	if err != nil {
		t.Fatal(err)
	}
	if closedCount != 3 {
		t.Errorf("ARCHIVE_CLOSED files = %d, want 3", closedCount)
	}

	var archivePath string
	err = env.db.QueryRowContext(ctx,
		"SELECT clp_archive_path FROM `"+pipelineTable+"` WHERE clp_archive_path IS NOT NULL AND clp_archive_path != '' LIMIT 1").
		Scan(&archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if !env.mio.ObjectExists(t, testutil.TestBucket, archivePath) {
		t.Errorf("archive %q not found in MinIO", archivePath)
	}
}
