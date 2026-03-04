package query

import (
	"testing"
	"time"
)

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache(1 * time.Minute)

	c.Set("key1", "value1")
	v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if v != "value1" {
		t.Errorf("got %v, want value1", v)
	}
}

func TestCache_Miss(t *testing.T) {
	c := NewCache(1 * time.Minute)

	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestCache_Expiry(t *testing.T) {
	c := NewCache(10 * time.Millisecond)

	c.Set("key1", "value1")
	time.Sleep(20 * time.Millisecond)

	_, ok := c.Get("key1")
	if ok {
		t.Error("expected expired entry to miss")
	}
}

func TestCache_Delete(t *testing.T) {
	c := NewCache(1 * time.Minute)

	c.Set("key1", "value1")
	c.Delete("key1")

	_, ok := c.Get("key1")
	if ok {
		t.Error("expected deleted key to miss")
	}
}

func TestCache_Clear(t *testing.T) {
	c := NewCache(1 * time.Minute)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Clear()

	if c.Len() != 0 {
		t.Errorf("Len() = %d after Clear, want 0", c.Len())
	}
}

func TestCache_EvictExpired(t *testing.T) {
	c := NewCache(10 * time.Millisecond)

	c.Set("a", 1)
	c.Set("b", 2)
	time.Sleep(20 * time.Millisecond)
	c.Set("c", 3) // not expired

	c.EvictExpired()

	if c.Len() != 1 {
		t.Errorf("Len() = %d after EvictExpired, want 1", c.Len())
	}
}

func TestCache_Overwrite(t *testing.T) {
	c := NewCache(1 * time.Minute)

	c.Set("key1", "v1")
	c.Set("key1", "v2")

	v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if v != "v2" {
		t.Errorf("got %v, want v2", v)
	}
}
