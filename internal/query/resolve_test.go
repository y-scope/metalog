package query

import (
	"strings"
	"testing"
)

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

func TestRewriteFilterColumns_Empty(t *testing.T) {
	result, err := RewriteFilterColumns("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
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
