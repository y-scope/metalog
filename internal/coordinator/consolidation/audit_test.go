package consolidation

import (
	"testing"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func TestAuditPolicy_GroupsByDay(t *testing.T) {
	day1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	day1b := time.Date(2024, 1, 15, 22, 0, 0, 0, time.UTC).UnixNano()
	day2 := time.Date(2024, 1, 16, 5, 0, 0, 0, time.UTC).UnixNano()

	candidates := []*metastore.FileRecord{
		{MinTimestamp: day1},
		{MinTimestamp: day1b},
		{MinTimestamp: day2},
	}

	policy := NewAuditPolicy(2, 100)
	groups := policy.SelectFiles(candidates)

	// Day 1 has 2 files (meets min), day 2 has 1 (below min)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	if len(groups[0]) != 2 {
		t.Errorf("group size = %d, want 2", len(groups[0]))
	}
}

func TestAuditPolicy_NeverCrossesDayBoundary(t *testing.T) {
	// Two files from different days
	day1 := time.Date(2024, 1, 15, 23, 59, 0, 0, time.UTC).UnixNano()
	day2 := time.Date(2024, 1, 16, 0, 1, 0, 0, time.UTC).UnixNano()

	candidates := []*metastore.FileRecord{
		{MinTimestamp: day1},
		{MinTimestamp: day2},
	}

	policy := NewAuditPolicy(1, 100)
	groups := policy.SelectFiles(candidates)

	// Each day should be its own group
	if len(groups) != 2 {
		t.Errorf("groups = %d, want 2 (one per day)", len(groups))
	}
}

func TestAuditPolicy_Empty(t *testing.T) {
	policy := NewAuditPolicy(1, 100)
	groups := policy.SelectFiles(nil)
	if groups != nil {
		t.Errorf("expected nil for empty candidates")
	}
}
