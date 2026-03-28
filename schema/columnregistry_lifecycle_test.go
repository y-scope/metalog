package schema

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRefreshAliases(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}
	cr.dimByColumn["dim_f01"] = cr.dimByKey["host"]

	// Query alias columns for dims
	mock.ExpectQuery("SELECT column_name.* FROM _dim_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "alias_column"}).
			AddRow("dim_f01", "hostname"))
	mock.ExpectQuery("SELECT column_name.* FROM _agg_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "alias_column"}))

	if err := cr.RefreshAliases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cr.dimByKey["host"].AliasCol != "hostname" {
		t.Errorf("alias = %q, want hostname", cr.dimByKey["host"].AliasCol)
	}
}

func TestRecycleOnce_NoCandidates(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectQuery("SELECT column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "dim_key"}))
	mock.ExpectQuery("SELECT column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key"}))

	if err := cr.recycleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRecycleOnce_WithCandidates(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectQuery("SELECT column_name, dim_key FROM _dim_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "dim_key"}).
			AddRow("dim_f03", "old_host"))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT column_name, agg_key FROM _agg_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key"}))

	if err := cr.recycleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRecycleOnce_WithDataClearing(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	// Find INVALIDATED dim with non-null data that needs clearing
	mock.ExpectQuery("SELECT column_name, dim_key FROM _dim_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "dim_key"}).
			AddRow("dim_f03", "old_host"))
	// countNonNullRows — has data but below threshold
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(500))
	// batch UPDATE to NULL — first pass clears 500
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 500))
	// second pass clears 0 — loop exits
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	// mark AVAILABLE
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	// aggs: no candidates
	mock.ExpectQuery("SELECT column_name, agg_key FROM _agg_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key"}))

	if err := cr.recycleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshAliases_EvictsRemoved(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}
	cr.dimByColumn["dim_f01"] = cr.dimByKey["host"]
	cr.dimByKey["zone"] = &DimRegistryEntry{ColumnName: "dim_f02", DimKey: "zone"}
	cr.dimByColumn["dim_f02"] = cr.dimByKey["zone"]
	aggKey := AggCacheKey("level", "error", "EQ")
	aggEntry := &AggRegistryEntry{ColumnName: "agg_f01", AggKey: "level", AggValue: "error", AggregationType: "EQ"}
	cr.aggByKey[aggKey] = aggEntry
	cr.aggByColumn["agg_f01"] = aggEntry

	// Only host comes back — zone was invalidated
	mock.ExpectQuery("SELECT column_name.* FROM _dim_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "alias_column"}).
			AddRow("dim_f01", ""))
	// Agg with alias update
	mock.ExpectQuery("SELECT column_name.* FROM _agg_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "alias_column"}).
			AddRow("agg_f01", "error_count"))

	if err := cr.RefreshAliases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cr.ResolveDim("zone") != "" {
		t.Error("zone should be evicted")
	}
	if cr.aggByKey[aggKey].AliasCol != "error_count" {
		t.Errorf("agg alias = %q, want error_count", cr.aggByKey[aggKey].AliasCol)
	}
}

func TestRunRecycler_ContextCancelled(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	// Initial scan
	mock.ExpectQuery("SELECT column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "dim_key"}))
	mock.ExpectQuery("SELECT column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key"}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		cr.RunRecycler(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunRecycler did not exit")
	}
}
