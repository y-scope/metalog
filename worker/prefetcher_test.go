package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/taskqueue"
)

// mockClaimer is a test mock for TaskClaimer.
type mockClaimer struct {
	claimFunc func(ctx context.Context, tableName, workerID string, batchSize int) ([]*taskqueue.Task, error)
	calls     atomic.Int64
}

func (m *mockClaimer) ClaimTasks(ctx context.Context, tableName, workerID string, batchSize int) ([]*taskqueue.Task, error) {
	m.calls.Add(1)
	return m.claimFunc(ctx, tableName, workerID, batchSize)
}

func TestPrefetcher_Run_FeedsTasksToChannel(t *testing.T) {
	tasks := []*taskqueue.Task{
		{TaskID: 1},
		{TaskID: 2},
		{TaskID: 3},
	}
	callCount := atomic.Int64{}
	mock := &mockClaimer{
		claimFunc: func(ctx context.Context, _, _ string, _ int) ([]*taskqueue.Task, error) {
			n := callCount.Add(1)
			if n == 1 {
				return tasks, nil
			}
			// Block until context cancelled (no more tasks)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	pf := NewPrefetcher(mock, "test-worker", 10, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())

	go pf.Run(ctx)

	var received []*taskqueue.Task
	timeout := time.After(2 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case task := <-pf.Tasks():
			received = append(received, task)
		case <-timeout:
			t.Fatalf("timed out waiting for task %d", i)
		}
	}
	cancel()
	<-pf.Done()

	if len(received) != 3 {
		t.Errorf("received %d tasks, want 3", len(received))
	}
	for i, task := range received {
		if task.TaskID != tasks[i].TaskID {
			t.Errorf("task[%d].TaskID = %d, want %d", i, task.TaskID, tasks[i].TaskID)
		}
	}
}

func TestPrefetcher_Run_RetriesOnError(t *testing.T) {
	callCount := atomic.Int64{}
	mock := &mockClaimer{
		claimFunc: func(ctx context.Context, _, _ string, _ int) ([]*taskqueue.Task, error) {
			n := callCount.Add(1)
			if n <= 2 {
				return nil, fmt.Errorf("db connection failed")
			}
			// Return one task on third call
			if n == 3 {
				return []*taskqueue.Task{{TaskID: 99}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	pf := NewPrefetcher(mock, "test-worker", 10, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())

	go pf.Run(ctx)

	select {
	case task := <-pf.Tasks():
		if task.TaskID != 99 {
			t.Errorf("TaskID = %d, want 99", task.TaskID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task after retries")
	}

	cancel()
	<-pf.Done()

	if callCount.Load() < 3 {
		t.Errorf("expected at least 3 claim calls, got %d", callCount.Load())
	}
}

func TestPrefetcher_Run_BackoffOnEmpty(t *testing.T) {
	callCount := atomic.Int64{}
	mock := &mockClaimer{
		claimFunc: func(ctx context.Context, _, _ string, _ int) ([]*taskqueue.Task, error) {
			callCount.Add(1)
			return nil, nil // empty — should trigger backoff
		},
	}

	pf := NewPrefetcher(mock, "test-worker", 10, zap.NewNop())
	// DefaultWorkerPollInterval is 2s; allow enough time for at least 2 polls
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go pf.Run(ctx)
	<-pf.Done()

	// With 2s initial backoff and 5s timeout, expect at least 2 empty polls
	if callCount.Load() < 2 {
		t.Errorf("expected at least 2 calls with backoff, got %d", callCount.Load())
	}
}

func TestPrefetcher_Run_ContextCancel_AbandonsTasks(t *testing.T) {
	mock := &mockClaimer{
		claimFunc: func(ctx context.Context, _, _ string, _ int) ([]*taskqueue.Task, error) {
			// Return more tasks than channel buffer can hold
			tasks := make([]*taskqueue.Task, 100)
			for i := range tasks {
				tasks[i] = &taskqueue.Task{TaskID: int64(i)}
			}
			return tasks, nil
		},
	}

	// Small channel buffer so it fills up quickly
	pf := NewPrefetcher(mock, "test-worker", 2, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())

	go pf.Run(ctx)

	// Read one task to let Run start dispatching
	select {
	case <-pf.Tasks():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first task")
	}

	// Cancel while tasks are still being dispatched
	cancel()

	select {
	case <-pf.Done():
		// Run exited cleanly after cancel
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}

func TestPrefetcher_Run_ClosesChannelsOnExit(t *testing.T) {
	mock := &mockClaimer{
		claimFunc: func(ctx context.Context, _, _ string, _ int) ([]*taskqueue.Task, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	pf := NewPrefetcher(mock, "test-worker", 10, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())

	go pf.Run(ctx)
	cancel()

	// Both channels should close
	select {
	case <-pf.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel not closed")
	}

	// Tasks channel should also be closed
	select {
	case _, ok := <-pf.Tasks():
		if ok {
			t.Error("expected Tasks channel to be closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Tasks channel not closed")
	}
}

// --- sleep helper tests ---

func TestSleep_ReturnsAfterDuration(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	sleep(ctx, 50*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Errorf("sleep returned too early: %v", elapsed)
	}
}

func TestSleep_ReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	start := time.Now()
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	sleep(ctx, 10*time.Second)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("sleep took too long after cancel: %v", elapsed)
	}
}
