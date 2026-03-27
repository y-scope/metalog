package query

import (
	"strings"
	"testing"
)

// --- ResolveColumnRef tests ---

func TestResolveColumnRef_FilePrefix(t *testing.T) {
	col, err := ResolveColumnRef("__FILE.record_count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if col != "record_count" {
		t.Errorf("got %q, want record_count", col)
	}
}

func TestResolveColumnRef_FileUnknown(t *testing.T) {
	_, err := ResolveColumnRef("__FILE.bogus", nil)
	if err == nil {
		t.Error("expected error for unknown file column")
	}
}

func TestResolveColumnRef_NoPrefix(t *testing.T) {
	col, err := ResolveColumnRef("min_timestamp", nil)
	if err != nil {
		t.Fatal(err)
	}
	if col != "min_timestamp" {
		t.Errorf("got %q, want min_timestamp", col)
	}
}

func TestResolveColumnRef_DimNoRegistry(t *testing.T) {
	_, err := ResolveColumnRef("__DIM.zone", nil)
	if err == nil {
		t.Error("expected error when registry is nil")
	}
}

func TestResolveColumnRef_DimWithRegistry(t *testing.T) {
	reg := newTestRegistry(t)
	col, err := ResolveColumnRef("__DIM.region", reg)
	if err != nil {
		t.Fatal(err)
	}
	if col != "dim_f01" {
		t.Errorf("got %q, want dim_f01", col)
	}
}

func TestResolveColumnRef_DimDottedKey(t *testing.T) {
	reg := newTestRegistry(t)
	col, err := ResolveColumnRef("__DIM.service.name", reg)
	if err != nil {
		t.Fatal(err)
	}
	if col != "dim_f02" {
		t.Errorf("got %q, want dim_f02", col)
	}
}

func TestResolveColumnRef_DimUnknownKey(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := ResolveColumnRef("__DIM.nonexistent", reg)
	if err == nil {
		t.Error("expected error for unknown dim key")
	}
}

func TestResolveColumnRef_AggTypedWithRegistry(t *testing.T) {
	reg := newTestRegistry(t)
	col, err := ResolveColumnRef("__AGG_EQ.status_code", reg)
	if err != nil {
		t.Fatal(err)
	}
	if col != "agg_f01" {
		t.Errorf("got %q, want agg_f01", col)
	}
}

func TestResolveColumnRef_AggTypedWithValue(t *testing.T) {
	reg := newTestRegistry(t)
	col, err := ResolveColumnRef("__AGG_GTE.response_time.p99", reg)
	if err != nil {
		t.Fatal(err)
	}
	if col != "agg_f02" {
		t.Errorf("got %q, want agg_f02", col)
	}
}

func TestResolveColumnRef_AggNoRegistry(t *testing.T) {
	_, err := ResolveColumnRef("__AGG_EQ.foo", nil)
	if err == nil {
		t.Error("expected error when registry is nil")
	}
}

func TestResolveColumnRef_AggWithoutType(t *testing.T) {
	// __AGG.<key> (no type prefix) falls back to __AGG_EQ.<key>
	reg := newTestRegistry(t)
	col, err := ResolveColumnRef("__AGG.status_code", reg)
	if err != nil {
		t.Fatal(err)
	}
	if col != "agg_f01" {
		t.Errorf("got %q, want agg_f01 (__AGG.x should fall back to __AGG_EQ.x)", col)
	}
}

func TestResolveColumnRef_AggUnknownKey(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := ResolveColumnRef("__AGG_EQ.nonexistent", reg)
	if err == nil {
		t.Error("expected error for unknown agg key")
	}
}

// --- RewriteFilterColumns tests ---

func TestRewriteFilterColumns_Empty(t *testing.T) {
	result, err := RewriteFilterColumns("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestRewriteFilterColumns_NoMagicPrefix(t *testing.T) {
	expr := "min_timestamp > 1000 AND state = 'IR_CLOSED'"
	result, err := RewriteFilterColumns(expr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "min_timestamp") {
		t.Errorf("expected min_timestamp in result, got %q", result)
	}
}

func TestRewriteFilterColumns_FilePrefix(t *testing.T) {
	expr := "__FILE.record_count > 100"
	result, err := RewriteFilterColumns(expr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "record_count") {
		t.Errorf("expected record_count in result, got %q", result)
	}
	if strings.Contains(result, "__FILE") {
		t.Errorf("__FILE prefix should be rewritten, got %q", result)
	}
}

func TestRewriteFilterColumns_LikeWithFilePrefix(t *testing.T) {
	expr := "__FILE.clp_ir_path LIKE '/logs/%'"
	result, err := RewriteFilterColumns(expr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "clp_ir_path") {
		t.Errorf("expected clp_ir_path in result, got %q", result)
	}
	if !strings.Contains(result, "like") && !strings.Contains(result, "LIKE") {
		t.Errorf("expected LIKE in result, got %q", result)
	}
}

func TestRewriteFilterColumns_Complex(t *testing.T) {
	expr := "__FILE.min_timestamp > 1000 AND __FILE.max_timestamp < 2000"
	result, err := RewriteFilterColumns(expr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "__FILE") {
		t.Errorf("__FILE prefix should be rewritten, got %q", result)
	}
	if !strings.Contains(result, "min_timestamp") || !strings.Contains(result, "max_timestamp") {
		t.Errorf("expected both timestamps in result, got %q", result)
	}
}

func TestRewriteFilterColumns_DimResolution(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region = 'us-east-1'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in result, got %q", result)
	}
	if strings.Contains(result, "__DIM") {
		t.Errorf("__DIM prefix should be rewritten, got %q", result)
	}
}

func TestRewriteFilterColumns_DimDottedKey(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.service.name = 'api-gateway'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f02") {
		t.Errorf("expected dim_f02 in result, got %q", result)
	}
}

func TestRewriteFilterColumns_DimDeeplyDottedKey(t *testing.T) {
	// Vitess SQL parser only supports up to 3-part names (schema.table.column).
	// 4-part names like __DIM.k8s.pod.name fail to parse, so deeply dotted
	// dim keys cannot be used directly in filter expressions. They work in
	// projection (__DIM.k8s.pod.name) because projection resolution doesn't
	// go through the SQL parser.
	reg := newTestRegistry(t)
	expr := "__DIM.k8s.pod.name = 'my-pod'"
	_, err := RewriteFilterColumns(expr, reg)
	if err == nil {
		t.Error("expected parse error for 4-part dotted name (vitess parser limitation)")
	}
}

func TestRewriteFilterColumns_AggResolution(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__AGG_EQ.status_code = 200"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "agg_f01") {
		t.Errorf("expected agg_f01 in result, got %q", result)
	}
}

func TestRewriteFilterColumns_AggWithValue(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__AGG_GTE.response_time.p99 > 500"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "agg_f02") {
		t.Errorf("expected agg_f02 in result, got %q", result)
	}
}

func TestRewriteFilterColumns_MixedDimAndFile(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region = 'us-east-1' AND __FILE.min_timestamp > 1000"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01, got %q", result)
	}
	if !strings.Contains(result, "min_timestamp") {
		t.Errorf("expected min_timestamp, got %q", result)
	}
	if strings.Contains(result, "__DIM") || strings.Contains(result, "__FILE") {
		t.Errorf("prefixes should be rewritten, got %q", result)
	}
}

func TestRewriteFilterColumns_MixedDimAggFile(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region = 'us-east-1' AND __AGG_EQ.status_code = 200 AND __FILE.state = 'IR_CLOSED'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01, got %q", result)
	}
	if !strings.Contains(result, "agg_f01") {
		t.Errorf("expected agg_f01, got %q", result)
	}
	if !strings.Contains(result, "state") {
		t.Errorf("expected state, got %q", result)
	}
}

func TestRewriteFilterColumns_NotExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "NOT __DIM.region = 'us-west-2'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in NOT expression, got %q", result)
	}
}

func TestRewriteFilterColumns_OrExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region = 'us-east-1' OR __DIM.region = 'eu-west-1'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in OR expression, got %q", result)
	}
	if strings.Contains(result, "__DIM") {
		t.Errorf("__DIM prefix should be rewritten, got %q", result)
	}
}

func TestRewriteFilterColumns_InExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region IN ('us-east-1', 'us-west-2')"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in IN expression, got %q", result)
	}
}

func TestRewriteFilterColumns_IsNullExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region IS NULL"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in IS NULL expression, got %q", result)
	}
}

func TestRewriteFilterColumns_IsNotNullExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__DIM.region IS NOT NULL"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "dim_f01") {
		t.Errorf("expected dim_f01 in IS NOT NULL expression, got %q", result)
	}
}

func TestRewriteFilterColumns_BetweenExpression(t *testing.T) {
	reg := newTestRegistry(t)
	expr := "__AGG_EQ.status_code BETWEEN 200 AND 299"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "agg_f01") {
		t.Errorf("expected agg_f01 in BETWEEN expression, got %q", result)
	}
}

func TestRewriteFilterColumns_FileColumnPassthrough(t *testing.T) {
	// state, min_timestamp, max_timestamp should pass through without prefix
	for _, col := range []string{"state", "min_timestamp", "max_timestamp"} {
		expr := col + " = 'test'"
		result, err := RewriteFilterColumns(expr, nil)
		if err != nil {
			t.Fatalf("RewriteFilterColumns(%q) error: %v", expr, err)
		}
		if !strings.Contains(result, col) {
			t.Errorf("expected %s in result, got %q", col, result)
		}
	}
}

func TestRewriteFilterColumns_UnknownDimPassesThrough(t *testing.T) {
	reg := newTestRegistry(t)
	// Unknown columns without prefix should pass through
	expr := "some_custom_col = 'value'"
	result, err := RewriteFilterColumns(expr, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "some_custom_col") {
		t.Errorf("expected some_custom_col to pass through, got %q", result)
	}
}

// --- isPhysicalColumn tests ---

func TestIsPhysicalColumn(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		// valid dim columns
		{"dim_f01", true},
		{"dim_f99", true},
		{"dim_f123", true},
		// valid agg columns
		{"agg_f01", true},
		{"agg_f10", true},
		{"agg_f999", true},
		// too short
		{"dim_f0", false},
		{"agg_f0", false},
		{"dim_f", false},
		// wrong prefix
		{"col_f01", false},
		{"dimf01", false},
		{"__DIM.x", false},
		// missing 'f'
		{"dim_001", false},
		{"agg_001", false},
		// non-digit after f
		{"dim_faa", false},
		{"agg_f1a", false},
		// empty
		{"", false},
		// plain file column names
		{"record_count", false},
		{"min_timestamp", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPhysicalColumn(tt.name)
			if got != tt.want {
				t.Errorf("isPhysicalColumn(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

// --- ResolveProjectionColumns tests ---

func TestResolveProjectionColumns_Nil(t *testing.T) {
	// Empty requested → nil (SELECT *)
	cols, err := ResolveProjectionColumns(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cols != nil {
		t.Errorf("expected nil for empty requested, got %v", cols)
	}
}

func TestResolveProjectionColumns_EmptySlice(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cols != nil {
		t.Errorf("expected nil for empty slice, got %v", cols)
	}
}

func TestResolveProjectionColumns_FileWildcard(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Must include system columns and all file columns (deduplicated).
	if len(cols) == 0 {
		t.Fatal("expected non-empty result for __FILE.*")
	}
	// System columns always included first.
	if cols[0] != "id" {
		t.Errorf("first column should be id, got %q", cols[0])
	}
	// Should contain all file columns (no duplicates).
	seen := make(map[string]int)
	for _, c := range cols {
		seen[c]++
	}
	for col, cnt := range seen {
		if cnt > 1 {
			t.Errorf("column %q appears %d times (expected 1)", col, cnt)
		}
	}
}

func TestResolveProjectionColumns_FileSpecific(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.record_count"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "record_count") {
		t.Errorf("expected record_count, got %v", cols)
	}
}

func TestResolveProjectionColumns_FileUnknown(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"__FILE.bogus"}, nil)
	if err == nil {
		t.Error("expected error for unknown file column")
	}
}

func TestResolveProjectionColumns_DimWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__DIM.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	// dim_f01, dim_f02, dim_f03 should be present.
	for _, want := range []string{"dim_f01", "dim_f02", "dim_f03"} {
		if !containsStr(cols, want) {
			t.Errorf("expected %s in result, got %v", want, cols)
		}
	}
}

func TestResolveProjectionColumns_DimWildcardNilRegistry(t *testing.T) {
	// __DIM.* with nil registry should be skipped, not error.
	cols, err := ResolveProjectionColumns([]string{"__DIM.*"}, nil)
	if err != nil {
		t.Errorf("__DIM.* with nil registry should not error, got: %v", err)
	}
	// Only system columns expected (no dim columns).
	for _, c := range cols {
		if strings.HasPrefix(c, "dim_") {
			t.Errorf("unexpected dim column %q when registry is nil", c)
		}
	}
}

func TestResolveProjectionColumns_DimSpecific(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__DIM.region"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "dim_f01") {
		t.Errorf("expected dim_f01, got %v", cols)
	}
}

func TestResolveProjectionColumns_DimSpecificNoRegistry(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"__DIM.region"}, nil)
	if err == nil {
		t.Error("expected error for __DIM.region with nil registry")
	}
}

func TestResolveProjectionColumns_DimUnknown(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := ResolveProjectionColumns([]string{"__DIM.nonexistent"}, reg)
	if err == nil {
		t.Error("expected error for unknown dim key")
	}
}

func TestResolveProjectionColumns_AggWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"agg_f01", "agg_f02", "agg_f03", "agg_f04"} {
		if !containsStr(cols, want) {
			t.Errorf("expected %s in result, got %v", want, cols)
		}
	}
}

func TestResolveProjectionColumns_AggWildcardNilRegistry(t *testing.T) {
	// __AGG.* with nil registry should be skipped, not error.
	cols, err := ResolveProjectionColumns([]string{"__AGG.*"}, nil)
	if err != nil {
		t.Errorf("__AGG.* with nil registry should not error, got: %v", err)
	}
	for _, c := range cols {
		if strings.HasPrefix(c, "agg_") {
			t.Errorf("unexpected agg column %q when registry is nil", c)
		}
	}
}

func TestResolveProjectionColumns_AggTypedWildcard(t *testing.T) {
	reg := newTestRegistry(t)
	// __AGG_EQ.* should return all EQ-typed agg columns.
	cols, err := ResolveProjectionColumns([]string{"__AGG_EQ.*"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "agg_f01") {
		t.Errorf("expected agg_f01 for EQ wildcard, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggTypedSpecific(t *testing.T) {
	reg := newTestRegistry(t)
	cols, err := ResolveProjectionColumns([]string{"__AGG_GTE.response_time.p99"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "agg_f02") {
		t.Errorf("expected agg_f02, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggWithoutType(t *testing.T) {
	reg := newTestRegistry(t)
	// __AGG.<key> falls back to __AGG_EQ.<key>.
	cols, err := ResolveProjectionColumns([]string{"__AGG.status_code"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "agg_f01") {
		t.Errorf("expected agg_f01 for __AGG.status_code, got %v", cols)
	}
}

func TestResolveProjectionColumns_AggTypedInvalidPattern(t *testing.T) {
	// __AGG_NOTYPE (no dot) should error.
	_, err := ResolveProjectionColumns([]string{"__AGG_NOTYPE"}, nil)
	if err == nil {
		t.Error("expected error for __AGG_NOTYPE (no dot)")
	}
}

func TestResolveProjectionColumns_PhysicalColumn(t *testing.T) {
	// Raw physical column names should be accepted.
	cols, err := ResolveProjectionColumns([]string{"dim_f01"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "dim_f01") {
		t.Errorf("expected dim_f01 to be passed through, got %v", cols)
	}
}

func TestResolveProjectionColumns_FileColumnDirect(t *testing.T) {
	// Direct file column name (without prefix) should be accepted.
	cols, err := ResolveProjectionColumns([]string{"record_count"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(cols, "record_count") {
		t.Errorf("expected record_count to be passed through, got %v", cols)
	}
}

func TestResolveProjectionColumns_UnknownColumn(t *testing.T) {
	_, err := ResolveProjectionColumns([]string{"totally_unknown_col"}, nil)
	if err == nil {
		t.Error("expected error for unknown column without prefix")
	}
}

func TestResolveProjectionColumns_Deduplication(t *testing.T) {
	// Requesting the same column twice should not duplicate it.
	cols, err := ResolveProjectionColumns([]string{"__FILE.*", "__FILE.record_count"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	for _, c := range cols {
		seen[c]++
	}
	if seen["record_count"] > 1 {
		t.Errorf("record_count appears %d times, expected 1", seen["record_count"])
	}
}

func TestResolveProjectionColumns_SystemColumnsAlwaysFirst(t *testing.T) {
	cols, err := ResolveProjectionColumns([]string{"__FILE.record_count"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// System columns (id, min_timestamp, max_timestamp, state) must come first.
	if len(cols) < 4 {
		t.Fatalf("expected at least 4 columns, got %d", len(cols))
	}
	systemCols := []string{"id", "min_timestamp", "max_timestamp", "state"}
	for i, sc := range systemCols {
		if cols[i] != sc {
			t.Errorf("cols[%d] = %q, want %q", i, cols[i], sc)
		}
	}
}

// --- resolveAggPattern extra tests ---

func TestResolveAggPattern_WildcardNilRegistry(t *testing.T) {
	// resolveAggPattern with nil registry should return error.
	_, err := resolveAggPattern("__AGG_EQ.*", nil)
	if err == nil {
		t.Error("expected error for nil registry in resolveAggPattern")
	}
}

func TestResolveAggPattern_WildcardByType(t *testing.T) {
	reg := newTestRegistry(t)
	// GTE type: only agg_f02 matches.
	cols, err := resolveAggPattern("__AGG_GTE.*", reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0] != "agg_f02" {
		t.Errorf("expected [agg_f02] for __AGG_GTE.*, got %v", cols)
	}
}

func TestResolveAggPattern_WildcardNoMatch(t *testing.T) {
	reg := newTestRegistry(t)
	// UNKNOWN type: no entries, should return empty (not error).
	cols, err := resolveAggPattern("__AGG_UNKNOWN.*", reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 0 {
		t.Errorf("expected empty for unmatched type wildcard, got %v", cols)
	}
}

func TestResolveAggPattern_InvalidPattern(t *testing.T) {
	_, err := resolveAggPattern("__AGG_NODOT", nil)
	if err == nil {
		t.Error("expected error for pattern without dot")
	}
}

// containsStr is a helper to check if a string slice contains a given value.
func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
