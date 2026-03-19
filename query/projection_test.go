package query

import (
	"testing"
)

func TestResolveProjectionColumns_Empty(t *testing.T) {
	cols, err := ResolveProjectionColumns(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cols != nil {
		t.Error("empty projection should return nil (SELECT *)")
	}
}

func TestResolveProjectionColumns_PhysicalColumns(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"dim_f01", "agg_f03"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Should include 4 system columns + 2 requested
	if len(cols) != 6 {
		t.Errorf("columns count = %d, want 6", len(cols))
	}
}

func TestResolveProjectionColumns_FileWildcard(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// All file columns (17) but system columns are deduplicated
	if len(cols) != 17 {
		t.Errorf("columns count = %d, want 17", len(cols))
	}
}

func TestResolveProjectionColumns_FileSpecific(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.record_count"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 4 system + record_count
	if len(cols) != 5 {
		t.Errorf("columns count = %d, want 5", len(cols))
	}
	if cols[4] != "record_count" {
		t.Errorf("last col = %q, want record_count", cols[4])
	}
}

func TestResolveProjectionColumns_FileUnknown(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"__FILE.nonexistent"}, nil)
	if err == nil {
		t.Error("expected error for unknown file column")
	}
}

func TestResolveProjectionColumns_UnknownColumn(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"totally_bogus"}, nil)
	if err == nil {
		t.Error("expected error for unknown column")
	}
}

func TestResolveProjectionColumns_DeduplicatesSystemColumns(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.id", "dim_f01"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	idCount := 0
	for _, c := range cols {
		if c == "id" {
			idCount++
		}
	}
	if idCount != 1 {
		t.Errorf("id appears %d times, want 1", idCount)
	}
}

func TestIsPhysicalColumn(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"dim_f01", true},
		{"dim_f99", true},
		{"agg_f01", true},
		{"agg_f123", true},
		{"dim_", false},
		{"agg_", false},
		{"dim_f", false},
		{"dim_fxx", false},
		{"id", false},
		{"state", false},
		{"", false},
		{"dim_g01", false},
	}
	for _, tt := range tests {
		if got := isPhysicalColumn(tt.name); got != tt.want {
			t.Errorf("isPhysicalColumn(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// --- Projection tests with ColumnRegistry ---

func TestResolveProjectionColumns_DimWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__DIM.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	// 4 system + 3 dims (dim_f01, dim_f02, dim_f03)
	if len(cols) != 7 {
		t.Errorf("columns count = %d, want 7", len(cols))
	}
	colSet := toSet(cols)
	if !colSet["dim_f01"] || !colSet["dim_f02"] || !colSet["dim_f03"] {
		t.Errorf("missing dim columns, got %v", cols)
	}
}

func TestResolveProjectionColumns_DimSpecific(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__DIM.region"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["dim_f01"] {
		t.Errorf("expected dim_f01 for __DIM.region, got %v", cols)
	}
}

func TestResolveProjectionColumns_DimDottedKey(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__DIM.service.name"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["dim_f02"] {
		t.Errorf("expected dim_f02 for __DIM.service.name, got %v", cols)
	}
}

func TestResolveProjectionColumns_DimUnknownKey(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := ResolveProjectionColumns([]string{"__DIM.nonexistent"}, reg)
	if err == nil {
		t.Error("expected error for unknown dim key")
	}
}

func TestResolveProjectionColumns_DimNoRegistry(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"__DIM.region"}, nil)
	if err == nil {
		t.Error("expected error for dim lookup without registry")
	}
}

func TestResolveProjectionColumns_DimWildcardNoRegistry(t *testing.T) {
	// __DIM.* with nil registry should silently produce no dim columns (not error)
	cols, err := ResolveProjectionColumns([]string{"__DIM.*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Just the 4 system columns
	if len(cols) != 4 {
		t.Errorf("columns count = %d, want 4 (system only)", len(cols))
	}
}

func TestResolveProjectionColumns_AggWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	// 4 system + 4 aggs (agg_f01..agg_f04)
	if len(cols) != 8 {
		t.Errorf("columns count = %d, want 8", len(cols))
	}
	colSet := toSet(cols)
	if !colSet["agg_f01"] || !colSet["agg_f02"] || !colSet["agg_f03"] || !colSet["agg_f04"] {
		t.Errorf("missing agg columns, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggTypedWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	// __AGG_EQ.* should return only EQ-type agg columns
	cols, err := ResolveProjectionColumns([]string{"__AGG_EQ.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f01"] {
		t.Errorf("expected agg_f01 (EQ type), got %v", cols)
	}
	if colSet["agg_f02"] {
		t.Errorf("agg_f02 (GTE type) should not be in __AGG_EQ.* result")
	}
}

func TestResolveProjectionColumns_AggTypedSpecific(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG_EQ.status_code"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f01"] {
		t.Errorf("expected agg_f01 for __AGG_EQ.status_code, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggTypedWithValue(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG_GTE.response_time.p99"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f02"] {
		t.Errorf("expected agg_f02 for __AGG_GTE.response_time.p99, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggWithoutType(t *testing.T) {
	// __AGG.<key> (no type) falls back to __AGG_EQ.<key>
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG.status_code"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f01"] {
		t.Errorf("expected agg_f01 for __AGG.status_code (EQ fallback), got %v", cols)
	}
}

func TestResolveProjectionColumns_AggUnknownKey(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := ResolveProjectionColumns([]string{"__AGG_EQ.nonexistent"}, reg)
	if err == nil {
		t.Error("expected error for unknown agg key")
	}
}

func TestResolveProjectionColumns_AggNoRegistry(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"__AGG_EQ.foo"}, nil)
	if err == nil {
		t.Error("expected error for agg lookup without registry")
	}
}

func TestResolveProjectionColumns_MixedNamespaces(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{
		"__FILE.record_count",
		"__DIM.region",
		"__AGG_EQ.status_code",
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["record_count"] {
		t.Errorf("missing record_count")
	}
	if !colSet["dim_f01"] {
		t.Errorf("missing dim_f01")
	}
	if !colSet["agg_f01"] {
		t.Errorf("missing agg_f01")
	}
}

func TestResolveProjectionColumns_AggSUMWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG_SUM.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f03"] {
		t.Errorf("expected agg_f03 (SUM type), got %v", cols)
	}
	// Should NOT include EQ, GTE, AVG types
	if colSet["agg_f01"] || colSet["agg_f02"] || colSet["agg_f04"] {
		t.Errorf("should only include SUM type, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggAVGWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG_AVG.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	colSet := toSet(cols)
	if !colSet["agg_f04"] {
		t.Errorf("expected agg_f04 (AVG type), got %v", cols)
	}
}

func toSet(cols []string) map[string]bool {
	s := make(map[string]bool, len(cols))
	for _, c := range cols {
		s[c] = true
	}
	return s
}
