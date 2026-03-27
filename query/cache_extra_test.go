package query

import (
	"fmt"
	"testing"
	"time"
)

func TestGetOrCompute_CacheHit(t *testing.T) {
	c := NewCacheWithSize(time.Hour, 100)
	c.Set("key", "cached_value")

	val, err := c.GetOrCompute("key", func() (any, error) {
		t.Error("compute should not be called on cache hit")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "cached_value" {
		t.Errorf("got %v, want cached_value", val)
	}
}

func TestGetOrCompute_CacheMiss(t *testing.T) {
	c := NewCacheWithSize(time.Hour, 100)

	val, err := c.GetOrCompute("key", func() (any, error) {
		return "computed_value", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "computed_value" {
		t.Errorf("got %v, want computed_value", val)
	}

	// Verify it's now cached
	cached, ok := c.Get("key")
	if !ok || cached != "computed_value" {
		t.Error("computed value should be cached")
	}
}

func TestGetOrCompute_ComputeError(t *testing.T) {
	c := NewCacheWithSize(time.Hour, 100)

	_, err := c.GetOrCompute("key", func() (any, error) {
		return nil, fmt.Errorf("compute failed")
	})
	if err == nil {
		t.Error("expected error from compute")
	}

	// Verify nothing was cached
	if _, ok := c.Get("key"); ok {
		t.Error("failed compute should not cache")
	}
}

func TestGetOrCompute_ExpiredEntry(t *testing.T) {
	c := NewCacheWithSize(1*time.Millisecond, 100)
	c.Set("key", "old_value")

	time.Sleep(5 * time.Millisecond)

	val, err := c.GetOrCompute("key", func() (any, error) {
		return "new_value", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "new_value" {
		t.Errorf("got %v, want new_value", val)
	}
}

func TestCache_LRUEviction(t *testing.T) {
	c := NewCacheWithSize(time.Hour, 2)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3) // should evict "a"

	if _, ok := c.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("b should still be cached")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should still be cached")
	}
}

func TestCache_SetExistingKey(t *testing.T) {
	c := NewCacheWithSize(time.Hour, 100)
	c.Set("key", "value1")
	c.Set("key", "value2")

	val, ok := c.Get("key")
	if !ok || val != "value2" {
		t.Errorf("got (%v, %v), want (value2, true)", val, ok)
	}
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1 (duplicate key)", c.Len())
	}
}
