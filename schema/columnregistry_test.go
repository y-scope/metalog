package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

func newTestRegistry() *ColumnRegistry {
	return &ColumnRegistry{
		dimByKey:    make(map[string]*DimRegistryEntry),
		dimByColumn: make(map[string]*DimRegistryEntry),
		aggByKey:    make(map[string]*AggRegistryEntry),
		aggByColumn: make(map[string]*AggRegistryEntry),
		sketchByKey: make(map[string]*SketchRegistryEntry),
		log:         zap.NewNop(),
	}
}

func newTestRegistryWithDB(db *sql.DB) *ColumnRegistry {
	cr := newTestRegistry()
	cr.db = db
	cr.tableName = "test_table"
	return cr
}

func TestAggCacheKey(t *testing.T) {
	if got := AggCacheKey("level", "error", "EQ"); got != "EQ\x00level\x00error" {
		t.Errorf("got %q", got)
	}
	if got := AggCacheKey("cpu", "", "SUM"); got != "SUM\x00cpu\x00" {
		t.Errorf("got %q", got)
	}
}

func TestParseSlotNumber(t *testing.T) {
	for _, tt := range []struct {
		col, prefix string
		want        int
	}{
		{"dim_f01", "dim_f", 1},
		{"dim_f99", "dim_f", 99},
		{"agg_f05", "agg_f", 5},
		{"dim_f", "dim_f", 0},
		{"short", "dim_f", 0},
		{"dim_f00", "dim_f", 0},
	} {
		if got := parseSlotNumber(tt.col, tt.prefix); got != tt.want {
			t.Errorf("parseSlotNumber(%q, %q) = %d, want %d", tt.col, tt.prefix, got, tt.want)
		}
	}
}

func TestDimSQLType(t *testing.T) {
	for _, tt := range []struct {
		baseType string
		want     string
		width    int
	}{
		{"str", "VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin", 256},
		{"str_utf8", "VARCHAR(256)", 100},
		{"bool", "BOOLEAN", 0},
		{"int", "BIGINT", 0},
		{"float", "DOUBLE", 0},
		{"str", "VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin", -1},
		{"unknown_type", "VARCHAR(256)", 0},
	} {
		got := dimSQLType(tt.baseType, tt.width)
		if got != tt.want {
			t.Errorf("dimSQLType(%q, %d) = %q, want %q", tt.baseType, tt.width, got, tt.want)
		}
	}
}

func TestResolveDim_Cached(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}

	if got := cr.ResolveDim("host"); got != "dim_f01" {
		t.Errorf("got %q, want dim_f01", got)
	}
	if got := cr.ResolveDim("missing"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestResolveAgg_Cached(t *testing.T) {
	cr := newTestRegistry()
	key := AggCacheKey("level", "error", "EQ")
	cr.aggByKey[key] = &AggRegistryEntry{ColumnName: "agg_f01"}

	if got := cr.ResolveAgg("level", "error", "EQ"); got != "agg_f01" {
		t.Errorf("got %q, want agg_f01", got)
	}
	if got := cr.ResolveAgg("missing", "", "SUM"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestResolveSketch_Cached(t *testing.T) {
	cr := newTestRegistry()
	cr.sketchByKey["uuid"] = &SketchRegistryEntry{SketchName: "s01"}

	if got := cr.ResolveSketch("uuid"); got == nil || got.SketchName != "s01" {
		t.Errorf("got %v", got)
	}
	if got := cr.ResolveSketch("missing"); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestSnapshot(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}
	cr.aggByKey[AggCacheKey("cpu", "", "SUM")] = &AggRegistryEntry{ColumnName: "agg_f01"}

	snap := cr.Snapshot()
	if snap.ResolveDim("host") != "dim_f01" {
		t.Error("snapshot should resolve host")
	}
	if snap.ResolveAgg("cpu", "", "SUM") != "agg_f01" {
		t.Error("snapshot should resolve cpu")
	}
	if len(snap.AllDimEntries()) != 1 {
		t.Errorf("dim entries = %d, want 1", len(snap.AllDimEntries()))
	}
	if len(snap.AllAggEntries()) != 1 {
		t.Errorf("agg entries = %d, want 1", len(snap.AllAggEntries()))
	}
	if snap.ResolveDim("missing") != "" {
		t.Error("missing dim should return empty")
	}
	if snap.ResolveAgg("missing", "", "SUM") != "" {
		t.Error("missing agg should return empty")
	}
}

func TestActiveDimColumns(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByColumn["dim_f01"] = &DimRegistryEntry{ColumnName: "dim_f01"}
	cr.dimByColumn["dim_f02"] = &DimRegistryEntry{ColumnName: "dim_f02"}

	cols := cr.ActiveDimColumns()
	if len(cols) != 2 {
		t.Errorf("got %d, want 2", len(cols))
	}
}

func TestActiveAggColumns(t *testing.T) {
	cr := newTestRegistry()
	cr.aggByColumn["agg_f01"] = &AggRegistryEntry{ColumnName: "agg_f01"}

	cols := cr.ActiveAggColumns()
	if len(cols) != 1 {
		t.Errorf("got %d, want 1", len(cols))
	}
}

func TestFloatAggColumns(t *testing.T) {
	cr := newTestRegistry()
	cr.aggByColumn["agg_f01"] = &AggRegistryEntry{ColumnName: "agg_f01", ValueType: "FLOAT"}
	cr.aggByColumn["agg_f02"] = &AggRegistryEntry{ColumnName: "agg_f02", ValueType: "INT"}

	floats := cr.FloatAggColumns()
	if !floats["agg_f01"] || floats["agg_f02"] {
		t.Errorf("got %v", floats)
	}
}

func TestEntryCount(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByKey["a"] = &DimRegistryEntry{}
	cr.aggByKey["b"] = &AggRegistryEntry{}

	if got := cr.EntryCount(); got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestSetMeter(t *testing.T) {
	cr := newTestRegistry()
	cr.SetMeter(noop.Meter{})
}

func TestNewColumnRegistry_InvalidTable(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	_, err := NewColumnRegistry(context.Background(), db, "DROP;--", false, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestNewColumnRegistry_LoadFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(context.DeadlineExceeded)

	_, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err == nil {
		t.Fatal("expected error when load fails")
	}
}

func TestEvolver_AddColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	e := NewEvolver(db, true, zap.NewNop())
	if err := e.AddColumn(context.Background(), "test_table", "dim_f01", "VARCHAR(255)"); err != nil {
		t.Fatal(err)
	}
}

func TestEvolver_AddColumn_InvalidName(t *testing.T) {
	e := NewEvolver(nil, false, zap.NewNop())
	if err := e.AddColumn(context.Background(), "DROP;--", "col", "INT"); err == nil {
		t.Fatal("expected error")
	}
	if err := e.AddColumn(context.Background(), "test_table", "BAD COL", "INT"); err == nil {
		t.Fatal("expected error")
	}
}

func TestLockMode(t *testing.T) {
	if lockMode(true) != "SHARED" {
		t.Error("MariaDB should use SHARED")
	}
	if lockMode(false) != "NONE" {
		t.Error("MySQL should use NONE")
	}
}

func TestSchemaSQL_NotEmpty(t *testing.T) {
	if len(SchemaSQL) == 0 {
		t.Fatal("SchemaSQL should not be empty")
	}
}

func TestIsDuplicateColumn(t *testing.T) {
	if isDuplicateColumn(nil) {
		t.Error("nil should not be duplicate")
	}
}

func TestNullIfEmpty(t *testing.T) {
	if nullIfEmpty("") != nil {
		t.Error("empty should be nil")
	}
	if nullIfEmpty("x") != "x" {
		t.Error("non-empty should return value")
	}
}

func TestLookupByColumn(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByColumn["dim_f01"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}
	cr.aggByColumn["agg_f01"] = &AggRegistryEntry{ColumnName: "agg_f01"}

	if cr.LookupDimByColumn("dim_f01") == nil {
		t.Error("should find dim_f01")
	}
	if cr.LookupDimByColumn("missing") != nil {
		t.Error("should return nil for missing")
	}
	if cr.LookupAggByColumn("agg_f01") == nil {
		t.Error("should find agg_f01")
	}
	if cr.LookupAggByColumn("missing") != nil {
		t.Error("should return nil for missing")
	}
}

func TestAllEntries(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByKey["a"] = &DimRegistryEntry{DimKey: "a"}
	cr.dimByKey["b"] = &DimRegistryEntry{DimKey: "b"}
	cr.aggByKey["c"] = &AggRegistryEntry{}

	if len(cr.AllDimEntries()) != 2 {
		t.Errorf("dim entries = %d, want 2", len(cr.AllDimEntries()))
	}
	if len(cr.AllAggEntries()) != 1 {
		t.Errorf("agg entries = %d, want 1", len(cr.AllAggEntries()))
	}
}

func TestNewColumnRegistry_LoadSuccess(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	// loadSlotHighWaterMarks runs first inside loadActiveEntries
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).AddRow("dim_f05"))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).AddRow("agg_f03"))
	// then dims, aggs, sketches
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}).
			AddRow("dim_f01", "str", 256, "host", nil))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
			AddRow("agg_f01", "level", "error", "EQ", "INT", nil))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"sketch_name", "sketch_key"}).
			AddRow("s01", "uuid"))

	cr, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if cr.ResolveDim("host") != "dim_f01" {
		t.Error("should resolve host")
	}
	if cr.ResolveAgg("level", "error", "EQ") != "agg_f01" {
		t.Error("should resolve level")
	}
	if cr.ResolveSketch("uuid") == nil {
		t.Error("should resolve uuid sketch")
	}
}

func TestSnapshot_NotFound(t *testing.T) {
	cr := newTestRegistry()
	snap := cr.Snapshot()
	if snap.ResolveDim("missing") != "" {
		t.Error("should return empty for missing dim")
	}
	if snap.ResolveAgg("missing", "", "SUM") != "" {
		t.Error("should return empty for missing agg")
	}
}

func TestNewColumnRegistry_HWMQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	// First HWM query fails
	mock.ExpectQuery("SELECT").WillReturnError(context.DeadlineExceeded)
	_, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewColumnRegistry_DimQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	// HWM queries succeed
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	// Dim query fails
	mock.ExpectQuery("SELECT").WillReturnError(context.DeadlineExceeded)
	_, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSnapshot_DeepCopy(t *testing.T) {
	cr := newTestRegistry()
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}
	cr.dimByColumn["dim_f01"] = cr.dimByKey["host"]
	cr.aggByKey[AggCacheKey("cpu", "", "SUM")] = &AggRegistryEntry{ColumnName: "agg_f01"}
	cr.aggByColumn["agg_f01"] = cr.aggByKey[AggCacheKey("cpu", "", "SUM")]

	snap := cr.Snapshot()

	// Mutate original — snapshot should not change
	cr.dimByKey["host"].ColumnName = "modified"
	if snap.ResolveDim("host") == "modified" {
		t.Error("snapshot should be a deep copy")
	}
}

func TestDimSQLType_LargeWidth(t *testing.T) {
	got := dimSQLType("str", 500)
	if got != "VARCHAR(500) CHARACTER SET ascii COLLATE ascii_bin" {
		t.Errorf("got %q", got)
	}
}

func TestNewColumnRegistry_AggQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	// HWM queries succeed
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	// Dim query succeeds
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}))
	// Agg query fails
	mock.ExpectQuery("SELECT").WillReturnError(context.DeadlineExceeded)
	_, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewColumnRegistry_SketchQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}))
	// Sketch query fails
	mock.ExpectQuery("SELECT").WillReturnError(context.DeadlineExceeded)
	_, err := NewColumnRegistry(context.Background(), db, "test_table", true, zap.NewNop())
	if err == nil {
		t.Fatal("expected error")
	}
}
