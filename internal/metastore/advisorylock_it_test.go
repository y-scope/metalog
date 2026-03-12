//go:build integration

package metastore_test

import (
	"context"
	"testing"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/testutil"
)

func TestAdvisoryLock_AcquireAndRelease(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	lock, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_1", 1)
	if err != nil {
		t.Fatalf("AcquireAdvisoryLock() error = %v", err)
	}

	err = lock.Release(ctx)
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestAdvisoryLock_BlocksConcurrentAcquire(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// First lock succeeds
	lock1, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_2", 1)
	if err != nil {
		t.Fatalf("first lock error = %v", err)
	}

	// Second lock with 0 timeout should fail
	_, err = metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_2", 0)
	if err == nil {
		t.Error("expected error for concurrent lock acquire")
	}

	// Release first lock
	lock1.Release(ctx)

	// Now should succeed
	lock2, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_2", 1)
	if err != nil {
		t.Fatalf("lock after release error = %v", err)
	}
	lock2.Release(ctx)
}

func TestAdvisoryLock_ReleaseIdempotent(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	lock, err := metastore.AcquireAdvisoryLock(ctx, mc.DB, "test_lock_3", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Release twice should not error
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("first release error = %v", err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("second release error = %v", err)
	}
}
