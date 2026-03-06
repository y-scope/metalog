package consolidation

import (
	"testing"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func TestSparkJobPolicy_GroupsByDimension(t *testing.T) {
	candidates := []*metastore.FileRecord{
		{MinTimestamp: 1, Dims: map[string]any{"app_id": "job-1"}},
		{MinTimestamp: 2, Dims: map[string]any{"app_id": "job-1"}},
		{MinTimestamp: 3, Dims: map[string]any{"app_id": "job-2"}},
		{MinTimestamp: 4, Dims: map[string]any{"app_id": "job-2"}},
		{MinTimestamp: 5, Dims: map[string]any{"app_id": "job-2"}},
	}

	policy := NewSparkJobPolicy("app_id", 2, 100, 0)
	groups := policy.SelectFiles(candidates)

	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}
}

func TestSparkJobPolicy_MinFilesFilter(t *testing.T) {
	candidates := []*metastore.FileRecord{
		{MinTimestamp: 1, Dims: map[string]any{"app_id": "job-1"}}, // only 1 file
	}

	policy := NewSparkJobPolicy("app_id", 2, 100, 0)
	groups := policy.SelectFiles(candidates)

	if len(groups) != 0 {
		t.Errorf("groups = %d, want 0 (below min)", len(groups))
	}
}

func TestSparkJobPolicy_TimeoutOverridesMinFiles(t *testing.T) {
	// One file that's old enough to trigger timeout
	old := time.Now().Add(-3 * time.Hour).UnixNano()
	candidates := []*metastore.FileRecord{
		{MinTimestamp: old, Dims: map[string]any{"app_id": "job-1"}},
	}

	policy := NewSparkJobPolicy("app_id", 2, 100, 2*time.Hour)
	groups := policy.SelectFiles(candidates)

	if len(groups) != 1 {
		t.Errorf("groups = %d, want 1 (timeout should override minFiles)", len(groups))
	}
}

func TestSparkJobPolicy_UngroupedRecords(t *testing.T) {
	// Records without the grouping dimension go to _ungrouped
	candidates := []*metastore.FileRecord{
		{MinTimestamp: 1},
		{MinTimestamp: 2},
	}

	policy := NewSparkJobPolicy("app_id", 2, 100, 0)
	groups := policy.SelectFiles(candidates)

	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1 (_ungrouped)", len(groups))
	}
}

func TestSparkJobPolicy_Empty(t *testing.T) {
	policy := NewSparkJobPolicy("app_id", 1, 100, 0)
	groups := policy.SelectFiles(nil)
	if groups != nil {
		t.Error("expected nil for empty candidates")
	}
}
