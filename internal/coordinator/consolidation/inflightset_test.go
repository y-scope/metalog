package consolidation

import (
	"sync"
	"testing"
)

func TestInFlightSet_NewInFlightSet(t *testing.T) {
	s := NewInFlightSet()
	if s == nil {
		t.Fatal("NewInFlightSet returned nil")
	}
	if s.Size() != 0 {
		t.Errorf("Size() = %d, want 0", s.Size())
	}
}

func TestInFlightSet_TryAdd_NewPaths(t *testing.T) {
	s := NewInFlightSet()
	paths := []string{"/a/b/c.ir", "/d/e/f.ir"}

	if !s.TryAdd(paths) {
		t.Error("TryAdd returned false for new paths")
	}
	if s.Size() != 2 {
		t.Errorf("Size() = %d, want 2", s.Size())
	}
}

func TestInFlightSet_TryAdd_DuplicatePaths(t *testing.T) {
	s := NewInFlightSet()
	s.TryAdd([]string{"/a/b/c.ir"})

	// Attempting to add a path that already exists should fail
	if s.TryAdd([]string{"/a/b/c.ir"}) {
		t.Error("TryAdd returned true for duplicate path")
	}
	// Size should remain unchanged
	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}
}

func TestInFlightSet_TryAdd_PartialDuplicate(t *testing.T) {
	s := NewInFlightSet()
	s.TryAdd([]string{"/a/b/c.ir"})

	// Batch with one existing and one new path should fail entirely
	if s.TryAdd([]string{"/a/b/c.ir", "/new/path.ir"}) {
		t.Error("TryAdd returned true when one path was duplicate")
	}
	// The new path should NOT have been added (all-or-nothing semantics)
	if s.Contains("/new/path.ir") {
		t.Error("new path was added despite partial duplicate")
	}
	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}
}

func TestInFlightSet_TryAdd_EmptySlice(t *testing.T) {
	s := NewInFlightSet()
	// Empty slice should succeed (vacuously true)
	if !s.TryAdd([]string{}) {
		t.Error("TryAdd returned false for empty slice")
	}
	if s.Size() != 0 {
		t.Errorf("Size() = %d, want 0", s.Size())
	}
}

func TestInFlightSet_TryAdd_NilSlice(t *testing.T) {
	s := NewInFlightSet()
	// Nil slice should succeed (vacuously true)
	if !s.TryAdd(nil) {
		t.Error("TryAdd returned false for nil slice")
	}
	if s.Size() != 0 {
		t.Errorf("Size() = %d, want 0", s.Size())
	}
}

func TestInFlightSet_Remove(t *testing.T) {
	s := NewInFlightSet()
	paths := []string{"/a.ir", "/b.ir", "/c.ir"}
	s.TryAdd(paths)

	s.Remove([]string{"/a.ir", "/b.ir"})

	if s.Contains("/a.ir") {
		t.Error("/a.ir should have been removed")
	}
	if s.Contains("/b.ir") {
		t.Error("/b.ir should have been removed")
	}
	if !s.Contains("/c.ir") {
		t.Error("/c.ir should still be present")
	}
	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}
}

func TestInFlightSet_Remove_NonExistent(t *testing.T) {
	s := NewInFlightSet()
	s.TryAdd([]string{"/a.ir"})

	// Removing non-existent path should not panic or error
	s.Remove([]string{"/nonexistent.ir"})

	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}
}

func TestInFlightSet_Contains(t *testing.T) {
	s := NewInFlightSet()
	s.TryAdd([]string{"/a.ir", "/b.ir"})

	if !s.Contains("/a.ir") {
		t.Error("Contains(/a.ir) = false, want true")
	}
	if !s.Contains("/b.ir") {
		t.Error("Contains(/b.ir) = false, want true")
	}
	if s.Contains("/c.ir") {
		t.Error("Contains(/c.ir) = true, want false")
	}
}

func TestInFlightSet_Size(t *testing.T) {
	s := NewInFlightSet()

	if s.Size() != 0 {
		t.Errorf("initial Size() = %d, want 0", s.Size())
	}

	s.TryAdd([]string{"/a.ir"})
	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}

	s.TryAdd([]string{"/b.ir", "/c.ir"})
	if s.Size() != 3 {
		t.Errorf("Size() = %d, want 3", s.Size())
	}

	s.Remove([]string{"/a.ir"})
	if s.Size() != 2 {
		t.Errorf("Size() = %d, want 2", s.Size())
	}
}

func TestInFlightSet_Concurrent(t *testing.T) {
	s := NewInFlightSet()
	var wg sync.WaitGroup
	const goroutines = 100
	const pathsPerGoroutine = 10

	// Concurrent TryAdd
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pathsPerGoroutine; j++ {
				path := []string{"/path/" + string(rune('A'+id)) + "/" + string(rune('0'+j)) + ".ir"}
				s.TryAdd(path)
			}
		}(i)
	}
	wg.Wait()

	// All unique paths should have been added
	expectedSize := goroutines * pathsPerGoroutine
	if s.Size() != expectedSize {
		t.Errorf("Size() = %d, want %d", s.Size(), expectedSize)
	}

	// Concurrent Contains
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pathsPerGoroutine; j++ {
				path := "/path/" + string(rune('A'+id)) + "/" + string(rune('0'+j)) + ".ir"
				s.Contains(path)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent Remove
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pathsPerGoroutine; j++ {
				path := []string{"/path/" + string(rune('A'+id)) + "/" + string(rune('0'+j)) + ".ir"}
				s.Remove(path)
			}
		}(i)
	}
	wg.Wait()

	if s.Size() != 0 {
		t.Errorf("after removal Size() = %d, want 0", s.Size())
	}
}

func TestInFlightSet_ConcurrentTryAdd_RaceCondition(t *testing.T) {
	// Test that concurrent TryAdd for the same path only succeeds once
	s := NewInFlightSet()
	var wg sync.WaitGroup
	const goroutines = 100
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.TryAdd([]string{"/same/path.ir"}) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successCount != 1 {
		t.Errorf("TryAdd succeeded %d times, want exactly 1", successCount)
	}
	if s.Size() != 1 {
		t.Errorf("Size() = %d, want 1", s.Size())
	}
}
