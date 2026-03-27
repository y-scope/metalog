package worker

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/taskqueue"
)

func TestExecuteTask_VersionMismatch(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	task := &taskqueue.Task{TaskID: 10, Version: 99, Input: []byte("data")}
	core.executeTask(context.Background(), task)

	if ac.calls != 0 {
		t.Error("should not call CreateArchive on version mismatch")
	}
	if len(tq.failedIDs) != 1 || tq.failedIDs[0] != 10 {
		t.Errorf("expected task 10 failed, got %v", tq.failedIDs)
	}
}

func TestExecuteTask_NilConsolidation(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	payload := &taskqueue.TaskPayload{TableName: "t"}
	task := makeTask(t, 11, payload)
	core.executeTask(context.Background(), task)

	if ac.calls != 0 {
		t.Error("should not call CreateArchive with nil consolidation")
	}
	if len(tq.failedIDs) != 1 || tq.failedIDs[0] != 11 {
		t.Errorf("expected task 11 failed, got %v", tq.failedIDs)
	}
}

func TestExecuteTask_EmptyIRPaths(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	payload := validPayload()
	payload.Consolidation.IRPaths = nil
	task := makeTask(t, 12, payload)
	core.executeTask(context.Background(), task)

	if ac.calls != 0 {
		t.Error("should not call CreateArchive with empty IR paths")
	}
	if len(tq.failedIDs) != 1 || tq.failedIDs[0] != 12 {
		t.Errorf("expected task 12 failed, got %v", tq.failedIDs)
	}
}

func TestSetMeter(t *testing.T) {
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(&mockTaskQueue{}, &mockArchiveCreator{}, pf, zap.NewNop())
	core.SetMeter(noop.Meter{})
}

type claimableTaskQueue struct {
	mockTaskQueue
}

func (c *claimableTaskQueue) ClaimTasks(_ context.Context, _ string, _ string, _ int) ([]*taskqueue.Task, error) {
	return nil, nil
}

func TestPrefetcher_SetMeter(t *testing.T) {
	tq := &claimableTaskQueue{}
	pf := NewPrefetcher(tq, "worker-1", 10, zap.NewNop())
	pf.SetMeter(noop.Meter{})
}

// failingTaskQueue fails CompleteTask/FailTask calls to test error paths.
type failingTaskQueue struct {
	completeErr  error
	failErr      error
	completedIDs []int64
	failedIDs    []int64
}

func (f *failingTaskQueue) CompleteTask(_ context.Context, taskID int64, _ []byte) (int64, error) {
	f.completedIDs = append(f.completedIDs, taskID)
	return 0, f.completeErr
}
func (f *failingTaskQueue) FailTask(_ context.Context, taskID int64) (int64, error) {
	f.failedIDs = append(f.failedIDs, taskID)
	return 0, f.failErr
}

func TestExecuteTask_FailTaskError(t *testing.T) {
	tq := &failingTaskQueue{failErr: errors.New("fail error")}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	// Version mismatch triggers FailTask.
	task := &taskqueue.Task{TaskID: 20, Version: 99, Input: []byte("data")}
	core.executeTask(context.Background(), task)

	if len(tq.failedIDs) != 1 {
		t.Errorf("expected 1 FailTask call, got %d", len(tq.failedIDs))
	}
}

func TestExecuteTask_CompleteTaskError(t *testing.T) {
	tq := &failingTaskQueue{completeErr: errors.New("complete error")}
	ac := &mockArchiveCreator{sizeBytes: 100}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	task := makeTask(t, 21, validPayload())
	core.executeTask(context.Background(), task)

	if len(tq.completedIDs) != 1 {
		t.Errorf("expected 1 CompleteTask call, got %d", len(tq.completedIDs))
	}
}

func TestExecuteTask_ArchiveFailureWithCompleteError(t *testing.T) {
	tq := &failingTaskQueue{completeErr: errors.New("complete error")}
	ac := &mockArchiveCreator{failNext: true}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	task := makeTask(t, 22, validPayload())
	core.executeTask(context.Background(), task)

	// CompleteTask is called with the error result.
	if len(tq.completedIDs) != 1 {
		t.Errorf("expected 1 CompleteTask call, got %d", len(tq.completedIDs))
	}
}

func TestExecuteTask_ArchiveFailureWithDeleteError(t *testing.T) {
	tq := &mockTaskQueue{}
	ac := &failingArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	task := makeTask(t, 23, validPayload())
	core.executeTask(context.Background(), task)

	if len(tq.completedIDs) != 1 {
		t.Errorf("expected 1 CompleteTask call, got %d", len(tq.completedIDs))
	}
}

type failingArchiveCreator struct{}

func (f *failingArchiveCreator) CreateArchive(_ context.Context, _ string, _ []string, _ []string, _, _, _ string) (int64, error) {
	return 0, errors.New("archive error")
}
func (f *failingArchiveCreator) DeleteArchive(_ context.Context, _, _, _ string) error {
	return errors.New("delete error")
}

func TestExecuteTask_FailTaskErrorOnBadPayload(t *testing.T) {
	tq := &failingTaskQueue{failErr: errors.New("fail error")}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	task := &taskqueue.Task{TaskID: 30, Version: taskqueue.TaskPayloadVersion, Input: []byte("garbage")}
	core.executeTask(context.Background(), task)

	if len(tq.failedIDs) != 1 {
		t.Errorf("expected 1 FailTask call, got %d", len(tq.failedIDs))
	}
}

func TestExecuteTask_FailTaskErrorOnNilConsolidation(t *testing.T) {
	tq := &failingTaskQueue{failErr: errors.New("fail error")}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	payload := &taskqueue.TaskPayload{TableName: "t"}
	task := makeTask(t, 31, payload)
	core.executeTask(context.Background(), task)

	if len(tq.failedIDs) != 1 {
		t.Errorf("expected 1 FailTask call, got %d", len(tq.failedIDs))
	}
}

func TestExecuteTask_FailTaskErrorOnEmptyBuckets(t *testing.T) {
	tq := &failingTaskQueue{failErr: errors.New("fail error")}
	ac := &mockArchiveCreator{}
	pf := &Prefetcher{tasks: make(chan *taskqueue.Task, 1)}
	core := NewCore(tq, ac, pf, zap.NewNop())

	payload := validPayload()
	payload.Consolidation.IRBuckets = nil
	task := makeTask(t, 32, payload)
	core.executeTask(context.Background(), task)

	if len(tq.failedIDs) != 1 {
		t.Errorf("expected 1 FailTask call, got %d", len(tq.failedIDs))
	}
}
