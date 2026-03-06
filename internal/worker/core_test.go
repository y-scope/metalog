package worker

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/taskqueue"
)

type mockTaskQueue struct {
	completedIDs []int64
	failedIDs    []int64
	lastOutput   []byte
}

func (m *mockTaskQueue) CompleteTask(_ context.Context, taskID int64, output []byte) (int64, error) {
	m.completedIDs = append(m.completedIDs, taskID)
	m.lastOutput = output
	return 1, nil
}

func (m *mockTaskQueue) FailTask(_ context.Context, taskID int64) (int64, error) {
	m.failedIDs = append(m.failedIDs, taskID)
	return 1, nil
}

type mockArchiveCreator struct {
	calls     int
	failNext  bool
	sizeBytes int64
}

func (m *mockArchiveCreator) CreateArchive(_ context.Context, _, _ string, _ []string, _, _, _ string) (int64, error) {
	m.calls++
	if m.failNext {
		return 0, context.DeadlineExceeded
	}
	return m.sizeBytes, nil
}

func makeTask(t *testing.T, taskID int64, payload *taskqueue.TaskPayload) *taskqueue.Task {
	t.Helper()
	input, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &taskqueue.Task{TaskID: taskID, Input: input}
}

func validPayload() *taskqueue.TaskPayload {
	return &taskqueue.TaskPayload{
		TableName:      "test_table",
		IRPaths:        []string{"path/to/file1.ir"},
		IRBuckets:      []string{"test-bucket"},
		IRBackend:      "minio",
		ArchivePath:    "/archive/out.clp",
		ArchiveBucket:  "archive-bucket",
		ArchiveBackend: "s3",
	}
}

func TestExecuteTask_Success(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{sizeBytes: 42000}
	tasks := make(chan *taskqueue.Task, 1)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	task := makeTask(t, 1, validPayload())
	core.executeTask(context.Background(), task)

	if ac.calls != 1 {
		t.Errorf("CreateArchive called %d times, want 1", ac.calls)
	}
	if len(tq.completedIDs) != 1 || tq.completedIDs[0] != 1 {
		t.Errorf("expected task 1 completed, got %v", tq.completedIDs)
	}
	if len(tq.failedIDs) != 0 {
		t.Errorf("expected no failed tasks, got %v", tq.failedIDs)
	}

	// Verify the result was serialized correctly
	result, err := taskqueue.UnmarshalResult(tq.lastOutput)
	if err != nil {
		t.Fatal(err)
	}
	if result.ArchiveSizeBytes != 42000 {
		t.Errorf("ArchiveSizeBytes = %d, want 42000", result.ArchiveSizeBytes)
	}
	if result.ArchivePath != "/archive/out.clp" {
		t.Errorf("ArchivePath = %q", result.ArchivePath)
	}
}

func TestExecuteTask_ArchiveCreationFailure(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{failNext: true}
	tasks := make(chan *taskqueue.Task, 1)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	task := makeTask(t, 2, validPayload())
	core.executeTask(context.Background(), task)

	// Archive failure completes with error result, doesn't fail the task
	if len(tq.completedIDs) != 1 || tq.completedIDs[0] != 2 {
		t.Errorf("expected task 2 completed (with error), got completed=%v", tq.completedIDs)
	}
	if len(tq.failedIDs) != 0 {
		t.Errorf("expected no failed tasks, got %v", tq.failedIDs)
	}

	result, err := taskqueue.UnmarshalResult(tq.lastOutput)
	if err != nil {
		t.Fatal(err)
	}
	if result.Error == "" {
		t.Error("expected error in result")
	}
}

func TestExecuteTask_InvalidPayload(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{}
	tasks := make(chan *taskqueue.Task, 1)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	task := &taskqueue.Task{TaskID: 3, Input: []byte("garbage")}
	core.executeTask(context.Background(), task)

	if ac.calls != 0 {
		t.Error("should not call CreateArchive on bad payload")
	}
	if len(tq.failedIDs) != 1 || tq.failedIDs[0] != 3 {
		t.Errorf("expected task 3 failed, got %v", tq.failedIDs)
	}
}

func TestExecuteTask_EmptyIRBuckets(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{}
	tasks := make(chan *taskqueue.Task, 1)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	payload := validPayload()
	payload.IRBuckets = nil // empty buckets
	task := makeTask(t, 4, payload)
	core.executeTask(context.Background(), task)

	if ac.calls != 0 {
		t.Error("should not call CreateArchive with empty buckets")
	}
	if len(tq.failedIDs) != 1 || tq.failedIDs[0] != 4 {
		t.Errorf("expected task 4 failed, got %v", tq.failedIDs)
	}
}

func TestCore_Run_ProcessesTasksUntilChannelClose(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{sizeBytes: 100}
	tasks := make(chan *taskqueue.Task, 5)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	for i := int64(1); i <= 3; i++ {
		tasks <- makeTask(t, i, validPayload())
	}
	close(tasks)

	core.Run(context.Background())

	if ac.calls != 3 {
		t.Errorf("CreateArchive called %d times, want 3", ac.calls)
	}
	if len(tq.completedIDs) != 3 {
		t.Errorf("completed %d tasks, want 3", len(tq.completedIDs))
	}
}

func TestCore_Run_StopsOnContextCancel(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{sizeBytes: 100}
	tasks := make(chan *taskqueue.Task, 1)
	pf := &Prefetcher{tasks: tasks}

	core := NewCore(tq, ac, pf, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		core.Run(ctx)
	}()

	cancel()
	close(tasks)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}
