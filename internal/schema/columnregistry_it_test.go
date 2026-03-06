//go:build integration

package schema_test

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/internal/testutil"
)

const itTable = "test_registry"

func setupColumnRegistryIT(t *testing.T) (*testutil.MariaDBContainer, *schema.ColumnRegistry) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, itTable)

	log := zap.NewNop()
	cr, err := schema.NewColumnRegistry(context.Background(), mc.DB, itTable, log)
	if err != nil {
		mc.Teardown(t)
		t.Fatal(err)
	}
	return mc, cr
}

func TestColumnRegistry_LoadEmpty(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)

	dims := cr.ActiveDimColumns()
	if len(dims) != 0 {
		t.Errorf("ActiveDimColumns() = %d, want 0 for fresh table", len(dims))
	}

	aggs := cr.ActiveAggColumns()
	if len(aggs) != 0 {
		t.Errorf("ActiveAggColumns() = %d, want 0 for fresh table", len(aggs))
	}
}

func TestColumnRegistry_ResolveOrAllocateDim(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// First allocation
	col1, err := cr.ResolveOrAllocateDim(ctx, "application_id", "str", 128)
	if err != nil {
		t.Fatalf("ResolveOrAllocateDim() error = %v", err)
	}
	if col1 != "dim_f01" {
		t.Errorf("first dim slot = %q, want dim_f01", col1)
	}

	// Same key resolves to same column
	col1Again, err := cr.ResolveOrAllocateDim(ctx, "application_id", "str", 128)
	if err != nil {
		t.Fatal(err)
	}
	if col1Again != col1 {
		t.Errorf("same key resolved to %q, want %q", col1Again, col1)
	}

	// Different key gets next slot
	col2, err := cr.ResolveOrAllocateDim(ctx, "hostname", "str_utf8", 255)
	if err != nil {
		t.Fatal(err)
	}
	if col2 != "dim_f02" {
		t.Errorf("second dim slot = %q, want dim_f02", col2)
	}

	// Verify column exists in the physical table
	var colCount int
	err = mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
		itTable, "dim_f01").Scan(&colCount)
	if err != nil {
		t.Fatal(err)
	}
	if colCount != 1 {
		t.Errorf("dim_f01 column count = %d, want 1", colCount)
	}
}

func TestColumnRegistry_ResolveOrAllocateAgg(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	col, err := cr.ResolveOrAllocateAgg(ctx, "level", "ERROR", "EQ", "INT")
	if err != nil {
		t.Fatalf("ResolveOrAllocateAgg() error = %v", err)
	}
	if col != "agg_f01" {
		t.Errorf("first agg slot = %q, want agg_f01", col)
	}

	// Same key resolves to same column
	colAgain, err := cr.ResolveOrAllocateAgg(ctx, "level", "ERROR", "EQ", "INT")
	if err != nil {
		t.Fatal(err)
	}
	if colAgain != col {
		t.Errorf("same key resolved to %q, want %q", colAgain, col)
	}

	// Float agg gets DOUBLE column type
	col2, err := cr.ResolveOrAllocateAgg(ctx, "response_time", "", "AVG", "FLOAT")
	if err != nil {
		t.Fatal(err)
	}
	if col2 != "agg_f02" {
		t.Errorf("second agg slot = %q, want agg_f02", col2)
	}

	// Verify float detection
	floats := cr.FloatAggColumns()
	if !floats["agg_f02"] {
		t.Error("agg_f02 should be in FloatAggColumns()")
	}
	if floats["agg_f01"] {
		t.Error("agg_f01 should not be in FloatAggColumns()")
	}
}

func TestColumnRegistry_ResolveDim_NotFound(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)

	col := cr.ResolveDim("nonexistent_key")
	if col != "" {
		t.Errorf("ResolveDim(nonexistent) = %q, want empty", col)
	}
}

func TestColumnRegistry_ResolveAgg_NotFound(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)

	col := cr.ResolveAgg("nonexistent", "", "EQ")
	if col != "" {
		t.Errorf("ResolveAgg(nonexistent) = %q, want empty", col)
	}
}

func TestColumnRegistry_AllEntries(t *testing.T) {
	mc, cr := setupColumnRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	cr.ResolveOrAllocateDim(ctx, "k1", "str", 64)
	cr.ResolveOrAllocateDim(ctx, "k2", "int", 0)
	cr.ResolveOrAllocateAgg(ctx, "a1", "", "SUM", "INT")

	dimEntries := cr.AllDimEntries()
	if len(dimEntries) != 2 {
		t.Errorf("AllDimEntries() = %d, want 2", len(dimEntries))
	}

	aggEntries := cr.AllAggEntries()
	if len(aggEntries) != 1 {
		t.Errorf("AllAggEntries() = %d, want 1", len(aggEntries))
	}
}

func TestColumnRegistry_PersistsAcrossReloads(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, itTable)

	ctx := context.Background()
	log := zap.NewNop()

	// First instance: allocate columns
	cr1, err := schema.NewColumnRegistry(ctx, mc.DB, itTable, log)
	if err != nil {
		t.Fatal(err)
	}
	cr1.ResolveOrAllocateDim(ctx, "host", "str", 128)
	cr1.ResolveOrAllocateAgg(ctx, "errors", "", "SUM", "INT")

	// Second instance: should load from DB
	cr2, err := schema.NewColumnRegistry(ctx, mc.DB, itTable, log)
	if err != nil {
		t.Fatal(err)
	}

	col := cr2.ResolveDim("host")
	if col != "dim_f01" {
		t.Errorf("persisted dim = %q, want dim_f01", col)
	}

	aggCol := cr2.ResolveAgg("errors", "", "SUM")
	if aggCol != "agg_f01" {
		t.Errorf("persisted agg = %q, want agg_f01", aggCol)
	}
}
