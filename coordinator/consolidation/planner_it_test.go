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

	planner, err := consolidation.NewPlanner(consolidation.PlannerConfig{
		DB:                 mc.DB,
		TableName:          plannerTable,
		IsMariaDB:          true,
		Policy:             policy,
		InFlight:           inFlight,
		TaskQueue:          taskQueue,
		Interval:           500 * time.Millisecond,
		FailureLogInterval: 1 * time.Minute,
		StaleThreshold:     60 * time.Minute,
		Log:                log,
	})
	if err != nil {
		mc.Teardown(t)
		t.Fatal(err)
	}
	return mc, planner, taskQueue
}

func insertConsolidationPendingFiles(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	baseTs := int64(1704067200000000000)
	for i := 0; i < count; i++ {
		query, args, err := sq.Insert("`"+plannerTable+"`").
			Columns("min_timestamp", "max_timestamp", "clp_ir_path",
				"clp_ir_storage_backend", "clp_ir_bucket",
				"state", "record_count", "retention_days", "expires_at").
			Values(
				baseTs+int64(i)*1000000,
				baseTs+int64(i)*1000000+500000,
				fmt.Sprintf("/data/consolidation_%d.ir", i),
				"minio", "logs",
				"IR_ARCHIVE_CONSOLIDATION_PENDING",
				10, 30, 0,
			).ToSql()
		if err != nil {
			t.Fatalf("build insert SQL: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
			t.Fatalf("insert consolidation file %d: %v", i, err)
		}
	}
}

func TestPlanner_RunCreatesTasksForPendingFiles(t *testing.T) {
	mc, planner, taskQueue := setupPlannerIT(t)
	defer mc.Teardown(t)
	ctx, cancel := context.WithCancel(context.Background())

	insertConsolidationPendingFiles(t, mc.DB, 5)

	go planner.Run(ctx)

	testutil.WaitFor(t, 15*time.Second, "planner to create tasks", func() bool {
		counts, err := taskQueue.GetTaskCounts(context.Background(), plannerTable)
		return err == nil && counts.Pending >= 1
	})
	cancel()
}

func TestPlanner_NoTasksForInsufficientFiles(t *testing.T) {
	mc, planner, taskQueue := setupPlannerIT(t)
	defer mc.Teardown(t)

	// Insert only 1 file (below MinFiles = 2).
	insertConsolidationPendingFiles(t, mc.DB, 1)

	// Run planner for a few cycles — should not create any tasks.
	plannerCtx, cancel := context.WithCancel(context.Background())
	go planner.Run(plannerCtx)

	// Give planner time to run at least 2 cycles (interval = 500ms).
	time.Sleep(1500 * time.Millisecond)
	cancel()

	counts, err := taskQueue.GetTaskCounts(context.Background(), plannerTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 0 {
		t.Errorf("pending tasks = %d, want 0 (not enough files for a group)", counts.Pending)
	}
}

func TestPlanner_PromotesStuckBufferingFiles(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, plannerTable)
	ctx := context.Background()

	log := zap.NewNop()
	inFlight := consolidation.NewInFlightSet()
	policy := consolidation.NewTimeWindowPolicy(24*time.Hour, 2, 100)
	taskQueue := taskqueue.NewQueue(mc.DB, log)

	// Use a very short stale threshold (1 nanosecond) so all files qualify immediately.
	planner, err := consolidation.NewPlanner(consolidation.PlannerConfig{
		DB:                 mc.DB,
		TableName:          plannerTable,
		IsMariaDB:          true,
		Policy:             policy,
		InFlight:           inFlight,
		TaskQueue:          taskQueue,
		Interval:           500 * time.Millisecond,
		FailureLogInterval: 1 * time.Minute,
		StaleThreshold:     1 * time.Nanosecond,
		Log:                log,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert stuck IR_ARCHIVE_BUFFERING files with old timestamps.
	baseTs := int64(1704067200000000000)
	for i := 0; i < 5; i++ {
		query, args, sqlErr := sq.Insert("`"+plannerTable+"`").
			Columns("min_timestamp", "max_timestamp", "clp_ir_path",
				"clp_ir_storage_backend", "clp_ir_bucket",
				"state", "record_count", "retention_days", "expires_at").
			Values(
				baseTs+int64(i)*1000000,
				baseTs+int64(i)*1000000+500000,
				fmt.Sprintf("/data/stuck_%d.ir", i),
				"minio", "logs",
				"IR_ARCHIVE_BUFFERING",
				10, 30, 0,
			).ToSql()
		if sqlErr != nil {
			t.Fatalf("build insert SQL: %v", sqlErr)
		}
		if _, err := mc.DB.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("insert stuck file %d: %v", i, err)
		}
	}

	plannerCtx, cancel := context.WithCancel(ctx)
	go planner.Run(plannerCtx)

	// Wait for the full pipeline: promote stuck files AND create tasks.
	// We must not cancel before task creation completes — the planner promotes
	// files and creates tasks in the same planOnce() call, so cancelling after
	// promotion but before task creation causes a race.
	testutil.WaitFor(t, 15*time.Second, "planner to create tasks from stuck files", func() bool {
		counts, err := taskQueue.GetTaskCounts(context.Background(), plannerTable)
		return err == nil && counts.Pending >= 1
	})
	cancel()

	// Verify all files were promoted out of BUFFERING.
	var bufferingCount int
	if err := mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+plannerTable+"` WHERE state = 'IR_ARCHIVE_BUFFERING'").
		Scan(&bufferingCount); err != nil {
		t.Fatal(err)
	}
	if bufferingCount != 0 {
		t.Errorf("buffering count = %d, want 0", bufferingCount)
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
