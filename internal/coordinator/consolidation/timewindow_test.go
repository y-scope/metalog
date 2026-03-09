package consolidation

import (
	"testing"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func makeFileRecord(id int64, minTS int64) *metastore.FileRecord {
	return &metastore.FileRecord{
		ID:           id,
		MinTimestamp: minTS,
	}
}

func TestNewTimeWindowPolicy(t *testing.T) {
	p := NewTimeWindowPolicy(15*time.Minute, 2, 10)

	if p.WindowSize != 15*time.Minute {
		t.Errorf("WindowSize = %v, want %v", p.WindowSize, 15*time.Minute)
	}
	if p.MinFilesPerGroup != 2 {
		t.Errorf("MinFilesPerGroup = %d, want 2", p.MinFilesPerGroup)
	}
	if p.MaxFilesPerGroup != 10 {
		t.Errorf("MaxFilesPerGroup = %d, want 10", p.MaxFilesPerGroup)
	}
}

func TestTimeWindowPolicy_SelectFiles_Empty(t *testing.T) {
	p := NewTimeWindowPolicy(15*time.Minute, 2, 10)
	result := p.SelectFiles(nil)

	if result != nil {
		t.Errorf("SelectFiles(nil) = %v, want nil", result)
	}

	result = p.SelectFiles([]*metastore.FileRecord{})
	if result != nil {
		t.Errorf("SelectFiles([]) = %v, want nil", result)
	}
}

func TestTimeWindowPolicy_SelectFiles_BelowMinimum(t *testing.T) {
	p := NewTimeWindowPolicy(15*time.Minute, 3, 10)

	// Only 2 files in same window, below minimum of 3
	candidates := []*metastore.FileRecord{
		makeFileRecord(1, 1000000000), // window 0
		makeFileRecord(2, 1000000001), // window 0
	}

	result := p.SelectFiles(candidates)
	if len(result) != 0 {
		t.Errorf("SelectFiles with %d files (min 3) = %d groups, want 0", len(candidates), len(result))
	}
}

func TestTimeWindowPolicy_SelectFiles_ExactlyMinimum(t *testing.T) {
	p := NewTimeWindowPolicy(time.Hour, 2, 10)
	windowNanos := time.Hour.Nanoseconds()

	// 2 files in same window, exactly at minimum
	candidates := []*metastore.FileRecord{
		makeFileRecord(1, windowNanos*5),     // window 5
		makeFileRecord(2, windowNanos*5+100), // window 5
	}

	result := p.SelectFiles(candidates)
	if len(result) != 1 {
		t.Errorf("SelectFiles = %d groups, want 1", len(result))
	}
	if len(result) > 0 && len(result[0]) != 2 {
		t.Errorf("group size = %d, want 2", len(result[0]))
	}
}

func TestTimeWindowPolicy_SelectFiles_MultipleWindows(t *testing.T) {
	p := NewTimeWindowPolicy(time.Hour, 2, 10)
	windowNanos := time.Hour.Nanoseconds()

	candidates := []*metastore.FileRecord{
		// Window 0: 3 files
		makeFileRecord(1, 100),
		makeFileRecord(2, 200),
		makeFileRecord(3, 300),
		// Window 1: 2 files
		makeFileRecord(4, windowNanos+100),
		makeFileRecord(5, windowNanos+200),
		// Window 2: 1 file (below min)
		makeFileRecord(6, windowNanos*2+100),
	}

	result := p.SelectFiles(candidates)

	// Should have 2 groups (windows 0 and 1), window 2 excluded
	if len(result) != 2 {
		t.Errorf("SelectFiles = %d groups, want 2", len(result))
	}

	// Verify total files in groups
	total := 0
	for _, g := range result {
		total += len(g)
	}
	if total != 5 {
		t.Errorf("total files in groups = %d, want 5", total)
	}
}

func TestTimeWindowPolicy_SelectFiles_SplitLargeGroup(t *testing.T) {
	p := NewTimeWindowPolicy(time.Hour, 2, 3)

	// 7 files in same window, max 3 per group
	candidates := []*metastore.FileRecord{
		makeFileRecord(1, 100),
		makeFileRecord(2, 200),
		makeFileRecord(3, 300),
		makeFileRecord(4, 400),
		makeFileRecord(5, 500),
		makeFileRecord(6, 600),
		makeFileRecord(7, 700),
	}

	result := p.SelectFiles(candidates)

	// Should split into chunks of 3, 3, and 1 (but 1 < min, so dropped)
	// Actually: 3, 3, 1 where 1 is dropped = 2 groups of 3
	// Wait, let's trace through:
	// i=0: chunk [0:3] size 3 >= 2 (min), include
	// i=3: chunk [3:6] size 3 >= 2 (min), include
	// i=6: chunk [6:7] size 1 < 2 (min), exclude
	if len(result) != 2 {
		t.Errorf("SelectFiles = %d groups, want 2", len(result))
	}

	for i, g := range result {
		if len(g) != 3 {
			t.Errorf("group %d size = %d, want 3", i, len(g))
		}
	}
}

func TestTimeWindowPolicy_SelectFiles_ChunkExactlyMax(t *testing.T) {
	p := NewTimeWindowPolicy(time.Hour, 2, 4)

	// 8 files, max 4 per group = 2 full groups
	candidates := make([]*metastore.FileRecord, 8)
	for i := range candidates {
		candidates[i] = makeFileRecord(int64(i), int64(i*100))
	}

	result := p.SelectFiles(candidates)

	if len(result) != 2 {
		t.Errorf("SelectFiles = %d groups, want 2", len(result))
	}
	for i, g := range result {
		if len(g) != 4 {
			t.Errorf("group %d size = %d, want 4", i, len(g))
		}
	}
}

func TestTimeWindowPolicy_WindowKeyCalculation(t *testing.T) {
	p := NewTimeWindowPolicy(time.Hour, 1, 100)
	windowNanos := time.Hour.Nanoseconds()

	// Files at different window boundaries
	candidates := []*metastore.FileRecord{
		makeFileRecord(1, 0),               // window 0
		makeFileRecord(2, windowNanos-1),   // window 0 (just before boundary)
		makeFileRecord(3, windowNanos),     // window 1 (exactly at boundary)
		makeFileRecord(4, windowNanos+1),   // window 1
		makeFileRecord(5, windowNanos*2-1), // window 1
		makeFileRecord(6, windowNanos*2),   // window 2
	}

	result := p.SelectFiles(candidates)

	// Should have 3 windows
	if len(result) != 3 {
		t.Errorf("SelectFiles = %d groups, want 3", len(result))
	}
}
