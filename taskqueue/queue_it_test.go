//go:build integration

package taskqueue_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/taskqueue"
	"github.com/y-scope/metalog/pkg/testutil"
)

const testTable = "clp_spark"

func setupTaskQueueIT(t *testing.T) (*testutil.MariaDBContainer, *taskqueue.Queue) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, testTable)

	log := zap.NewNop()
	tq := taskqueue.NewQueue(mc.DB, log)
	return mc, tq
}

// mustCreateTask creates a task with a minimal payload, returning the task ID.
func mustCreateTask(t *testing.T, tq *taskqueue.Queue, ctx context.Context) int64 {
	t.Helper()
	input, err := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	id, err := tq.CreateTask(ctx, testTable, taskqueue.TaskPayloadVersion, input)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	return id
}

// mustScanState queries the state of a task by ID.
func mustScanState(t *testing.T, mc *testutil.MariaDBContainer, ctx context.Context, taskID int64) string {
	t.Helper()
	var state string
	err := mc.DB.QueryRowContext(ctx,
		"SELECT state FROM _task_queue WHERE task_id = ?", taskID).Scan(&state)
	if err != nil {
		t.Fatalf("query task %d state: %v", taskID, err)
	}
	return state
}

// mustScanCount queries the count of tasks matching the given query.
func mustScanCount(t *testing.T, mc *testutil.MariaDBContainer, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var count int
	err := mc.DB.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	return count
}

func TestQueue_CreateAndClaim(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	payload := &taskqueue.TaskPayload{
		TableName: testTable,
		Consolidation: &taskqueue.ConsolidationPayload{
			IRPaths:   []string{"/data/file1.ir"},
			IRBackend: "minio",
		},
	}
	input, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		t.Fatal(err)
	}

	taskID, err := tq.CreateTask(ctx, testTable, taskqueue.TaskPayloadVersion, input)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if taskID <= 0 {
		t.Errorf("CreateTask() returned taskID=%d, want > 0", taskID)
	}

	tasks, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10)
	if err != nil {
		t.Fatalf("ClaimTasks() error = %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("ClaimTasks() returned %d tasks, want 1", len(tasks))
	}
	if tasks[0].TaskID != taskID {
		t.Errorf("claimed task ID = %d, want %d", tasks[0].TaskID, taskID)
	}
	if tasks[0].State != taskqueue.TaskStateProcessing {
		t.Errorf("claimed task state = %q, want %q", tasks[0].State, taskqueue.TaskStateProcessing)
	}
}

func TestQueue_ClaimTasks_EmptyQueue(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)

	tasks, err := tq.ClaimTasks(context.Background(), testTable, "worker-1", 10)
	if err != nil {
		t.Fatalf("ClaimTasks() error = %v", err)
	}
	if tasks != nil {
		t.Errorf("ClaimTasks() on empty queue returned %d tasks, want nil", len(tasks))
	}
}

func TestQueue_CompleteTask(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	taskID := mustCreateTask(t, tq, ctx)
	if _, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10); err != nil {
		t.Fatal(err)
	}

	result := &taskqueue.TaskResult{
		ArchivePath:      "/data/archive.clp",
		ArchiveSizeBytes: 1024,
		CreatedAt:        time.Now().UnixNano(),
	}
	output, err := taskqueue.MarshalResult(result)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := tq.CompleteTask(ctx, taskID, output)
	if err != nil {
		t.Fatalf("CompleteTask() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("CompleteTask() affected = %d, want 1", affected)
	}

	state := mustScanState(t, mc, ctx, taskID)
	if state != "completed" {
		t.Errorf("task state = %q, want completed", state)
	}
}

func TestQueue_FailTask(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	taskID := mustCreateTask(t, tq, ctx)
	if _, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10); err != nil {
		t.Fatal(err)
	}

	affected, err := tq.FailTask(ctx, taskID)
	if err != nil {
		t.Fatalf("FailTask() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("FailTask() affected = %d, want 1", affected)
	}

	state := mustScanState(t, mc, ctx, taskID)
	if state != "failed" {
		t.Errorf("task state = %q, want failed", state)
	}
}

func TestQueue_ClaimTasks_BatchSize(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		mustCreateTask(t, tq, ctx)
	}

	tasks, err := tq.ClaimTasks(ctx, testTable, "worker-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Errorf("ClaimTasks(batchSize=3) returned %d tasks, want 3", len(tasks))
	}

	tasks2, err := tq.ClaimTasks(ctx, testTable, "worker-2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks2) != 2 {
		t.Errorf("second ClaimTasks() returned %d tasks, want 2", len(tasks2))
	}
}

func TestQueue_GetTaskCounts(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		mustCreateTask(t, tq, ctx)
	}
	if _, err := tq.ClaimTasks(ctx, testTable, "worker-1", 1); err != nil {
		t.Fatal(err)
	}

	counts, err := tq.GetTaskCounts(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 2 {
		t.Errorf("Pending = %d, want 2", counts.Pending)
	}
	if counts.Processing != 1 {
		t.Errorf("Processing = %d, want 1", counts.Processing)
	}
}

func TestQueue_ReclaimTask(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	taskID := mustCreateTask(t, tq, ctx)
	if _, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10); err != nil {
		t.Fatal(err)
	}

	if err := tq.ReclaimTask(ctx, taskID); err != nil {
		t.Fatalf("ReclaimTask() error = %v", err)
	}

	state := mustScanState(t, mc, ctx, taskID)
	if state != "timed_out" {
		t.Errorf("original task state = %q, want timed_out", state)
	}

	newCount := mustScanCount(t, mc, ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ? AND state = 'pending'", testTable)
	if newCount != 1 {
		t.Errorf("pending count after reclaim = %d, want 1", newCount)
	}
}

func TestQueue_ReclaimTask_DeadLetter(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	taskID := mustCreateTask(t, tq, ctx)
	if _, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10); err != nil {
		t.Fatal(err)
	}

	// Set retry_count >= max retries so reclaim triggers dead_letter.
	if _, err := mc.DB.ExecContext(ctx, "UPDATE _task_queue SET retry_count = ? WHERE task_id = ?",
		taskqueue.DefaultMaxRetries, taskID); err != nil {
		t.Fatal(err)
	}
	if err := tq.ReclaimTask(ctx, taskID); err != nil {
		t.Fatalf("ReclaimTask() error = %v", err)
	}

	state := mustScanState(t, mc, ctx, taskID)
	if state != "dead_letter" {
		t.Errorf("task state = %q, want dead_letter", state)
	}

	pendingCount := mustScanCount(t, mc, ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ? AND state = 'pending'", testTable)
	if pendingCount != 0 {
		t.Errorf("pending count = %d, want 0 (should not re-enqueue dead letter)", pendingCount)
	}
}

func TestQueue_DeleteAllTasks(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		mustCreateTask(t, tq, ctx)
	}

	deleted, err := tq.DeleteAllTasks(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 5 {
		t.Errorf("DeleteAllTasks() = %d, want 5", deleted)
	}

	count := mustScanCount(t, mc, ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ?", testTable)
	if count != 0 {
		t.Errorf("remaining tasks = %d, want 0", count)
	}
}
