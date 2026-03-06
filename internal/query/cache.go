package query

import (
	"sync"
	"time"
)

// Cache provides a simple TTL-based in-memory cache for query metadata.
type Cache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	value     any
	expiresAt time.Time
}

// NewCache creates a Cache with the given TTL.
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
}

// Get retrieves a cached value. Returns the value and true if found and not expired.
func (c *Cache) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.value, true
}

// Set stores a value in the cache with the default TTL.
func (c *Cache) Set(key string, value any) {
	c.mu.Lock()
	c.entries[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// Clear removes all entries.
func (c *Cache) Clear() {
	c.mu.Lock()
	c.entries = make(map[string]*cacheEntry)
	c.mu.Unlock()
}

// GetOrCompute retrieves a cached value, or calls supplier to compute and cache it.
// This is the primary method for caching expensive computations like filter parsing.
func (c *Cache) GetOrCompute(key string, supplier func() (any, error)) (any, error) {
	if val, ok := c.Get(key); ok {
		return val, nil
	}
	val, err := supplier()
	if err != nil {
		return nil, err
	}
	c.Set(key, val)
	return val, nil
}

// Len returns the number of entries (including expired ones not yet evicted).
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// EvictExpired removes expired entries. Call periodically if needed.
func (c *Cache) EvictExpired() {
	now := time.Now()
	c.mu.Lock()
	for k, e := range c.entries {
		if now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}
