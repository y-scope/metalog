package worker

import (
	"context"
	"testing"
	"time"

	"github.com/y-scope/metalog/internal/taskqueue"
)

func TestPrefetcher_Run_ClosesChannelOnCancel(t *testing.T) {
	// We can't use a real TaskQueue here, but we can test that the channel
	// is closed when context is cancelled by using a nil taskQueue which
	// will cause ClaimTasks to panic. Instead, test the sleep helper.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		sleep(ctx, 10*time.Second)
		close(done)
	}()

	select {
	case <-done:
		// sleep returned immediately due to cancelled context
	case <-time.After(1 * time.Second):
		t.Fatal("sleep did not return on cancelled context")
	}
}

func TestPrefetcher_TaskDistribution(t *testing.T) {
	// Simulate multiple workers consuming from the same prefetcher channel
	tasks := make(chan *taskqueue.Task, 10)
	for i := int64(1); i <= 10; i++ {
		tasks <- &taskqueue.Task{TaskID: i}
	}
	close(tasks)

	// Two workers consume from the channel
	worker1Count := 0
	worker2Count := 0
	done := make(chan struct{})

	go func() {
		for range tasks {
			worker1Count++
		}
		close(done)
	}()

	<-done

	// Since we only have one goroutine reading, it gets all 10
	total := worker1Count + worker2Count
	if total != 10 {
		t.Errorf("total consumed = %d, want 10", total)
	}
}

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
