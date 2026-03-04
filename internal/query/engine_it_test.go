//go:build integration

package query_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/query"
	"github.com/y-scope/metalog/internal/testutil"
)

const engineTable = "test_query_engine"

func setupEngineIT(t *testing.T) (*testutil.MariaDBContainer, *query.SplitQueryEngine) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, engineTable)

	log := zap.NewNop()
	engine := query.NewSplitQueryEngine(mc.DB, log)
	return mc, engine
}

func insertTestRows(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		_, err := db.ExecContext(context.Background(),
			"INSERT INTO `"+engineTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?)",
			int64(1704067200000000000)+int64(i)*1000000000,
			int64(1704067200000000000)+int64(i)*1000000000+500000000,
			fmt.Sprintf("/data/file_%03d.ir", i),
			"IR_BUFFERING",
			int64(10+i),
			30,
		)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
}

func TestSplitQueryEngine_BasicQuery(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	insertTestRows(t, mc.DB, 5)

	rows, err := engine.Query(ctx, &query.QueryParams{
		TableName: engineTable,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("returned %d rows, want 5", len(rows))
	}
}

func TestSplitQueryEngine_Pagination(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	insertTestRows(t, mc.DB, 10)

	// Page 1: first 3 rows
	page1, err := engine.Query(ctx, &query.QueryParams{
		TableName: engineTable,
		OrderBy:   []query.OrderBySpec{{Column: "min_timestamp"}},
		Limit:     3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1 = %d rows, want 3", len(page1))
	}

	// Page 2: next 3 rows using keyset cursor
	lastRow := page1[len(page1)-1]
	minTs := lastRow.Values["min_timestamp"]

	page2, err := engine.Query(ctx, &query.QueryParams{
		TableName:    engineTable,
		OrderBy:      []query.OrderBySpec{{Column: "min_timestamp"}},
		Limit:        3,
		HasCursor:    true,
		CursorValues: []any{minTs},
		CursorID:     lastRow.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Fatalf("page2 = %d rows, want 3", len(page2))
	}

	// Verify no overlap between pages
	page1IDs := make(map[int64]bool)
	for _, r := range page1 {
		page1IDs[r.ID] = true
	}
	for _, r := range page2 {
		if page1IDs[r.ID] {
			t.Errorf("page2 contains ID %d from page1 (overlap)", r.ID)
		}
	}
}

func TestSplitQueryEngine_StateFilter(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	insertTestRows(t, mc.DB, 5)

	// Change 2 rows to IR_CLOSED
	_, err := mc.DB.ExecContext(ctx,
		"UPDATE `"+engineTable+"` SET state = 'IR_CLOSED' ORDER BY id LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}

	// Query only IR_BUFFERING
	rows, err := engine.Query(ctx, &query.QueryParams{
		TableName:   engineTable,
		StateFilter: []string{"IR_BUFFERING"},
		Limit:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Errorf("filtered rows = %d, want 3", len(rows))
	}
}

func TestSplitQueryEngine_ColumnProjection(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	insertTestRows(t, mc.DB, 3)

	rows, err := engine.Query(ctx, &query.QueryParams{
		TableName: engineTable,
		Columns:   []string{"id", "min_timestamp", "state"},
		Limit:     10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	// Should only have the 3 requested columns
	for _, r := range rows {
		if len(r.Values) != 3 {
			t.Errorf("column count = %d, want 3", len(r.Values))
		}
		if _, ok := r.Values["id"]; !ok {
			t.Error("missing id column")
		}
		if _, ok := r.Values["state"]; !ok {
			t.Error("missing state column")
		}
	}
}

func TestSplitQueryEngine_EmptyTable(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	rows, err := engine.Query(ctx, &query.QueryParams{
		TableName: engineTable,
		Limit:     10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %d, want 0 for empty table", len(rows))
	}
}

func TestSplitQueryEngine_InvalidTable(t *testing.T) {
	mc, engine := setupEngineIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	_, err := engine.Query(ctx, &query.QueryParams{
		TableName: "DROP TABLE--",
		Limit:     10,
	})
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}
