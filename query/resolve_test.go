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

