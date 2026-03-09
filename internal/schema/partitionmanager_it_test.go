//go:build integration

package schema_test

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/internal/testutil"
)

const pmTable = "test_partitions"

func setupPartitionManagerIT(t *testing.T) (*testutil.MariaDBContainer, *schema.PartitionManager) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, pmTable)

	log := zap.NewNop()
	pm := schema.NewPartitionManager(mc.DB, pmTable, 3, 90, 1000, log)
	return mc, pm
}

func TestPartitionManager_EnsureLookaheadPartitions(t *testing.T) {
	mc, pm := setupPartitionManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	created, err := pm.EnsureLookaheadPartitions(ctx)
	if err != nil {
		t.Fatalf("EnsureLookaheadPartitions() error = %v", err)
	}
	// Should create at least today + lookaheadDays (3) partitions
	// The template already has p_future, so REORGANIZE creates daily ones
	if created < 1 {
		t.Errorf("created = %d, want >= 1", created)
	}
}

func TestPartitionManager_EnsureLookaheadPartitions_Idempotent(t *testing.T) {
	mc, pm := setupPartitionManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// First call
	created1, err := pm.EnsureLookaheadPartitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Second call — should create 0 new partitions
	created2, err := pm.EnsureLookaheadPartitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created2 != 0 {
		t.Errorf("second call created %d partitions, want 0", created2)
	}

	if created1 < 1 {
		t.Errorf("first call should have created partitions, got %d", created1)
	}
}

func TestPartitionManager_RunMaintenance(t *testing.T) {
	mc, pm := setupPartitionManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// RunMaintenance acquires advisory lock and creates partitions
	err := pm.RunMaintenance(ctx)
	if err != nil {
		t.Fatalf("RunMaintenance() error = %v", err)
	}

	// Verify partitions exist
	var partCount int
	err = mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		pmTable).Scan(&partCount)
	if err != nil {
		t.Fatal(err)
	}
	// At least p_future + today's partition
	if partCount < 2 {
		t.Errorf("partition count = %d, want >= 2", partCount)
	}
}
