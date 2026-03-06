//go:build integration

package schema_test

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/internal/testutil"
)

const imTable = "test_index"

func setupIndexManagerIT(t *testing.T) (*testutil.MariaDBContainer, *schema.IndexManager) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, imTable)

	log := zap.NewNop()

	// Add a dim column so we have something to index
	_, err := mc.DB.ExecContext(context.Background(),
		"ALTER TABLE `"+imTable+"` ADD COLUMN dim_f01 VARCHAR(128) NULL")
	if err != nil {
		mc.Teardown(t)
		t.Fatalf("add dim column: %v", err)
	}

	im := schema.NewIndexManager(mc.DB, log)
	return mc, im
}

func TestIndexManager_EnsureIndex(t *testing.T) {
	mc, im := setupIndexManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	err := im.EnsureIndex(ctx, imTable, "dim_f01")
	if err != nil {
		t.Fatalf("EnsureIndex() error = %v", err)
	}

	// Verify index exists
	var count int
	err = mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
		imTable, "idx_dim_f01").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Error("index idx_dim_f01 should exist after EnsureIndex")
	}
}

func TestIndexManager_EnsureIndex_Idempotent(t *testing.T) {
	mc, im := setupIndexManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// First call
	if err := im.EnsureIndex(ctx, imTable, "dim_f01"); err != nil {
		t.Fatal(err)
	}

	// Second call — should be no-op
	if err := im.EnsureIndex(ctx, imTable, "dim_f01"); err != nil {
		t.Fatalf("second EnsureIndex() error = %v", err)
	}
}

func TestIndexManager_EnsureIndex_InvalidIdentifier(t *testing.T) {
	mc, im := setupIndexManagerIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	err := im.EnsureIndex(ctx, "valid_table", "DROP TABLE--")
	if err == nil {
		t.Error("expected error for invalid column name")
	}
}
