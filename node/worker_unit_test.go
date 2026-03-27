package node

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/storage"
)

func TestNewWorkerUnit(t *testing.T) {
	ctx := context.Background()
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	shared := &Resources{DB: db, Log: zap.NewNop()}
	ac := &storage.ArchiveCreator{}

	u := NewWorkerUnit(ctx, 2, "node-1", shared, ac, zap.NewNop())
	if u == nil {
		t.Fatal("expected non-nil WorkerUnit")
	}
	if u.concurrency != 2 {
		t.Errorf("concurrency = %d, want 2", u.concurrency)
	}
	if u.nodeID != "node-1" {
		t.Errorf("nodeID = %q, want node-1", u.nodeID)
	}
}

func TestWorkerUnit_StartStop_CleanDrain(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	// The prefetcher will try to claim tasks — return empty results so it
	// exits cleanly. Use regexp matcher for flexible matching.
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}))

	shared := &Resources{DB: db, Log: zap.NewNop()}
	ac := &storage.ArchiveCreator{}

	u := NewWorkerUnit(ctx, 1, "node-1", shared, ac, zap.NewNop())
	u.Start()

	// Give goroutines a moment to start, then stop.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		u.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean drain succeeded.
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within timeout")
	}
}

func TestWorkerUnit_StopWithoutStart(t *testing.T) {
	ctx := context.Background()
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	shared := &Resources{DB: db, Log: zap.NewNop()}
	ac := &storage.ArchiveCreator{}

	u := NewWorkerUnit(ctx, 1, "node-1", shared, ac, zap.NewNop())

	// Stop without Start: prefetchCancel and workerCancel should not panic.
	done := make(chan struct{})
	go func() {
		u.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK — no panic
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return")
	}
}
