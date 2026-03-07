//go:build integration

package consolidation_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/coordinator/consolidation"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/testutil"
)

const plannerTable = "test_planner"

func setupPlannerIT(t *testing.T) (*testutil.MariaDBContainer, *consolidation.Planner, *taskqueue.Queue) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, plannerTable)

	log := zap.NewNop()
	inFlight := consolidation.NewInFlightSet()
	policy := consolidation.NewTimeWindowPolicy(24*time.Hour, 2, 100)
	taskQueue := taskqueue.NewQueue(mc.DB, log)

	planner, err := consolidation.NewPlanner(
		mc.DB, plannerTable, policy, inFlight, taskQueue,
		nil, "", "",
		1*time.Second, log,
	)
	if err != nil {
		mc.Teardown(t)
		t.Fatal(err)
	}
	return mc, planner, taskQueue
}

func insertConsolidationPendingFiles(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		_, err := db.ExecContext(context.Background(),
			"INSERT INTO `"+plannerTable+"` (min_timestamp, max_timestamp, clp_ir_path, clp_ir_storage_backend, clp_ir_bucket, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			int64(1704067200000000000)+int64(i)*1000000,
			int64(1704067200000000000)+int64(i)*1000000+500000,
			"/data/consolidation_"+string(rune('a'+i))+".ir",
			"minio",
			"logs",
			"IR_ARCHIVE_CONSOLIDATION_PENDING",
			10,
			30,
		)
		if err != nil {
			t.Fatalf("insert consolidation file %d: %v", i, err)
		}
	}
}

func TestPlanner_RunCreatesTasksForPendingFiles(t *testing.T) {
	mc, planner, taskQueue := setupPlannerIT(t)
	defer mc.Teardown(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Insert files in CONSOLIDATION_PENDING state
	insertConsolidationPendingFiles(t, mc.DB, 5)

	// Run planner once via a short-lived context
	go planner.Run(ctx)
	time.Sleep(2 * time.Second)
	cancel()

	// Check that tasks were created (use fresh context since ctx is cancelled)
	counts, err := taskQueue.GetTaskCounts(context.Background(), plannerTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending < 1 {
		t.Errorf("pending tasks = %d, want >= 1", counts.Pending)
	}
}

func TestPlanner_NoTasksForInsufficientFiles(t *testing.T) {
	mc, _, taskQueue := setupPlannerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert only 1 file (below MinFilesPerGroup = 2)
	insertConsolidationPendingFiles(t, mc.DB, 1)

	// Create a planner manually and run one cycle
	log := zap.NewNop()
	inFlight := consolidation.NewInFlightSet()
	policy := consolidation.NewTimeWindowPolicy(24*time.Hour, 2, 100)

	planner, err := consolidation.NewPlanner(
		mc.DB, plannerTable, policy, inFlight, taskQueue,
		nil, "", "",
		1*time.Second, log,
	)
	if err != nil {
		t.Fatal(err)
	}

	plannerCtx, cancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)
	time.Sleep(2 * time.Second)
	cancel()

	counts, err := taskQueue.GetTaskCounts(ctx, plannerTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 0 {
		t.Errorf("pending tasks = %d, want 0 (not enough files for a group)", counts.Pending)
	}
}

func TestPlanner_RunContextCancel(t *testing.T) {
	mc, planner, _ := setupPlannerIT(t)
	defer mc.Teardown(t)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		planner.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not stop after context cancel")
	}
}
