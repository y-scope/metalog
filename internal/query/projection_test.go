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
