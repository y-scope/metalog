package metastore_test

import (
	"context"
	"testing"

	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/testutil"
)

func TestAdvisoryLock_BlocksConcurrentAcquire(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	lock1, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock", 1)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}

	// Second lock with 0 timeout should fail while first is held.
	if _, err2 := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock", 0); err2 == nil {
		t.Error("expected error for concurrent lock")
	}

	err = lock1.Release(ctx)
	if err != nil {
		t.Fatalf("lock1.Release: %v", err)
	}

	// After release, should succeed.
	lock2, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock", 1)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	err = lock2.Release(ctx)
	if err != nil {
		t.Fatalf("lock2.Release: %v", err)
	}
}

func TestAdvisoryLock_ReleaseIdempotent(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	lock, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("second release should be no-op: %v", err)
	}
}
