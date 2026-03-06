//go:build integration

package taskqueue_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/testutil"
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

func TestQueue_CreateAndClaim(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Create a task
	payload := &taskqueue.TaskPayload{
		TableName: testTable,
		IRPaths:   []string{"/data/file1.ir"},
		IRBackend: "minio",
	}
	input, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		t.Fatal(err)
	}

	taskID, err := tq.CreateTask(ctx, testTable, input)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if taskID <= 0 {
		t.Errorf("CreateTask() returned taskID=%d, want > 0", taskID)
	}

	// Claim task
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
	ctx := context.Background()

	tasks, err := tq.ClaimTasks(ctx, testTable, "worker-1", 10)
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

	// Create and claim
	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
	taskID, _ := tq.CreateTask(ctx, testTable, input)
	tasks, _ := tq.ClaimTasks(ctx, testTable, "worker-1", 10)
	if len(tasks) != 1 {
		t.Fatal("expected 1 claimed task")
	}

	// Complete with output
	result := &taskqueue.TaskResult{
		ArchivePath:      "/data/archive.clp",
		ArchiveSizeBytes: 1024,
		CreatedAt:        time.Now().UnixNano(),
	}
	output, _ := taskqueue.MarshalResult(result)
	affected, err := tq.CompleteTask(ctx, taskID, output)
	if err != nil {
		t.Fatalf("CompleteTask() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("CompleteTask() affected = %d, want 1", affected)
	}

	// Verify state
	var state string
	mc.DB.QueryRowContext(ctx,
		"SELECT state FROM _task_queue WHERE task_id = ?", taskID).Scan(&state)
	if state != "completed" {
		t.Errorf("task state = %q, want completed", state)
	}
}

func TestQueue_FailTask(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
	taskID, _ := tq.CreateTask(ctx, testTable, input)
	tq.ClaimTasks(ctx, testTable, "worker-1", 10)

	affected, err := tq.FailTask(ctx, taskID)
	if err != nil {
		t.Fatalf("FailTask() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("FailTask() affected = %d, want 1", affected)
	}

	var state string
	mc.DB.QueryRowContext(ctx,
		"SELECT state FROM _task_queue WHERE task_id = ?", taskID).Scan(&state)
	if state != "failed" {
		t.Errorf("task state = %q, want failed", state)
	}
}

func TestQueue_ClaimTasks_BatchSize(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Create 5 tasks
	for i := 0; i < 5; i++ {
		input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
		_, err := tq.CreateTask(ctx, testTable, input)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Claim with batch size 3
	tasks, err := tq.ClaimTasks(ctx, testTable, "worker-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Errorf("ClaimTasks(batchSize=3) returned %d tasks, want 3", len(tasks))
	}

	// Claim remaining
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

	// Create 3 tasks
	for i := 0; i < 3; i++ {
		input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
		tq.CreateTask(ctx, testTable, input)
	}

	// Claim 1
	tq.ClaimTasks(ctx, testTable, "worker-1", 1)

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

	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
	taskID, _ := tq.CreateTask(ctx, testTable, input)
	tq.ClaimTasks(ctx, testTable, "worker-1", 10)

	// Reclaim the task (retry_count=0, below max retries)
	err := tq.ReclaimTask(ctx, taskID, 0)
	if err != nil {
		t.Fatalf("ReclaimTask() error = %v", err)
	}

	// Original task should be timed_out
	var state string
	mc.DB.QueryRowContext(ctx,
		"SELECT state FROM _task_queue WHERE task_id = ?", taskID).Scan(&state)
	if state != "timed_out" {
		t.Errorf("original task state = %q, want timed_out", state)
	}

	// A new pending task should exist
	var newCount int
	mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ? AND state = 'pending'",
		testTable).Scan(&newCount)
	if newCount != 1 {
		t.Errorf("pending count after reclaim = %d, want 1", newCount)
	}
}

func TestQueue_ReclaimTask_DeadLetter(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
	taskID, _ := tq.CreateTask(ctx, testTable, input)
	tq.ClaimTasks(ctx, testTable, "worker-1", 10)

	// Reclaim with retry_count >= max retries (3)
	err := tq.ReclaimTask(ctx, taskID, 3)
	if err != nil {
		t.Fatalf("ReclaimTask() error = %v", err)
	}

	// Should be dead_letter, no new pending task
	var state string
	mc.DB.QueryRowContext(ctx,
		"SELECT state FROM _task_queue WHERE task_id = ?", taskID).Scan(&state)
	if state != "dead_letter" {
		t.Errorf("task state = %q, want dead_letter", state)
	}

	var pendingCount int
	mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ? AND state = 'pending'",
		testTable).Scan(&pendingCount)
	if pendingCount != 0 {
		t.Errorf("pending count = %d, want 0 (should not re-enqueue dead letter)", pendingCount)
	}
}

func TestQueue_DeleteAllTasks(t *testing.T) {
	mc, tq := setupTaskQueueIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		input, _ := taskqueue.MarshalPayload(&taskqueue.TaskPayload{TableName: testTable})
		tq.CreateTask(ctx, testTable, input)
	}

	deleted, err := tq.DeleteAllTasks(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 5 {
		t.Errorf("DeleteAllTasks() = %d, want 5", deleted)
	}

	var count int
	mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM _task_queue WHERE table_name = ?", testTable).Scan(&count)
	if count != 0 {
		t.Errorf("remaining tasks = %d, want 0", count)
	}
}
