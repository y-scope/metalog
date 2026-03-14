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
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/testutil"
)

const pipelineTable = "test_pipeline"

// pipelineEnv holds the shared test infrastructure for end-to-end pipeline tests.
type pipelineEnv struct {
	db  *sql.DB
	mc  *testutil.MariaDBContainer
	mio *testutil.MinIOContainer
	fr  *metastore.FileRecords
	log *zap.Logger
}

func setupPipelineEnv(t *testing.T) *pipelineEnv {
	t.Helper()

	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, pipelineTable)

	mio := testutil.SetupMinIOWithBucket(t)

	log := zap.NewNop()
	fr, err := metastore.NewFileRecords(mc.DB, pipelineTable, true, log)
	if err != nil {
		mc.Teardown(t)
		mio.Teardown(t)
		t.Fatal(err)
	}

	return &pipelineEnv{
		db:  mc.DB,
		mc:  mc,
		mio: mio,
		fr:  fr,
		log: log,
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

	planner, err := consolidation.NewPlanner(
		env.db, pipelineTable, true, policy, inFlight, tq,
		nil, // resolver — no dynamic columns needed
		reg,
		testutil.StorageBackendName, testutil.TestBucket,
		1*time.Second, 1*time.Minute, 60*time.Minute,
		env.log,
	)
	if err != nil {
		t.Fatal(err)
	}
	return planner, tq
}

// --- Stage 1: Verify test infrastructure setup ---

func TestPipeline_SetupAndInsert(t *testing.T) {
	env := setupPipelineEnv(t)
	defer env.teardown(t)
	ctx := context.Background()

	irPaths := env.insertPendingFiles(t, 3)

	// Verify file records exist in DB with correct state.
	pending, err := env.fr.FindConsolidationPending(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending files = %d, want 3", len(pending))
	}
	for _, rec := range pending {
		if rec.State != metastore.StateIRArchiveConsolidationPending {
			t.Errorf("file state = %q, want IR_ARCHIVE_CONSOLIDATION_PENDING", rec.State)
		}
	}

	// Verify IR files exist in MinIO.
	for _, path := range irPaths {
		if !env.mio.ObjectExists(t, testutil.TestBucket, path) {
			t.Errorf("IR file %q not found in MinIO", path)
		}
	}
}

// --- Stage 2: Planner creates tasks from pending files ---

func TestPipeline_PlannerCreatesTask(t *testing.T) {
	env := setupPipelineEnv(t)
	defer env.teardown(t)
	ctx := context.Background()

	irPaths := env.insertPendingFiles(t, 3)
	planner, tq := env.newPlanner(t)

	// Run planner for a couple cycles.
	plannerCtx, cancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)
	time.Sleep(3 * time.Second)
	cancel()

	// Verify tasks were created.
	counts, err := tq.GetTaskCounts(ctx, pipelineTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending < 1 {
		t.Fatalf("pending tasks = %d, want >= 1", counts.Pending)
	}

	// Claim the task and verify the payload.
	tasks, err := tq.ClaimTasks(ctx, pipelineTable, "test-worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) < 1 {
		t.Fatal("no tasks claimed")
	}

	task := tasks[0]
	if task.Version != taskqueue.TaskPayloadVersion {
		t.Errorf("task.Version = %d, want %d", task.Version, taskqueue.TaskPayloadVersion)
	}

	payload, err := taskqueue.UnmarshalPayload(task.Input)
	if err != nil {
		t.Fatal(err)
	}
	if payload.TableName != pipelineTable {
		t.Errorf("TableName = %q, want %q", payload.TableName, pipelineTable)
	}

	cons := payload.Consolidation
	if cons == nil {
		t.Fatal("Consolidation is nil")
	}
	if len(cons.IRPaths) == 0 {
		t.Fatal("IRPaths is empty")
	}
	// Verify all claimed IR paths are from our inserted files.
	irPathSet := make(map[string]bool, len(irPaths))
	for _, p := range irPaths {
		irPathSet[p] = true
	}
	for _, p := range cons.IRPaths {
		if !irPathSet[p] {
			t.Errorf("unexpected IR path in payload: %q", p)
		}
	}
	if cons.ArchiveBackend != testutil.StorageBackendName {
		t.Errorf("ArchiveBackend = %q, want %q", cons.ArchiveBackend, testutil.StorageBackendName)
	}
	if cons.ArchiveBucket != testutil.TestBucket {
		t.Errorf("ArchiveBucket = %q, want %q", cons.ArchiveBucket, testutil.TestBucket)
	}
	if cons.ArchivePath == "" {
		t.Error("ArchivePath is empty")
	}
	if cons.IRBackend != testutil.StorageBackendName {
		t.Errorf("IRBackend = %q, want %q", cons.IRBackend, testutil.StorageBackendName)
	}
}
