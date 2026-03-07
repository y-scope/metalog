package query

import (
	"container/list"
	"sync"
	"time"
)

// Cache provides a TTL + LRU-capped in-memory cache for query metadata.
type Cache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = most recently used
	ttl     time.Duration
	maxSize int
}

type cacheEntry struct {
	key       string
	value     any
	expiresAt time.Time
}

// NewCache creates a Cache with the given TTL and a maximum of 10000 entries.
func NewCache(ttl time.Duration) *Cache {
	return NewCacheWithSize(ttl, 10000)
}

// NewCacheWithSize creates a Cache with the given TTL and max entry count.
func NewCacheWithSize(ttl time.Duration, maxSize int) *Cache {
	return &Cache{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

// Get retrieves a cached value. Returns the value and true if found and not expired.
func (c *Cache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*cacheEntry)
	if time.Now().After(e.expiresAt) {
		c.removeLocked(el)
		return nil, false
	}
	c.order.MoveToFront(el)
	return e.value, true
}

// Set stores a value in the cache with the default TTL.
func (c *Cache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.entries[key]; ok {
		e := el.Value.(*cacheEntry)
		e.value = value
		e.expiresAt = time.Now().Add(c.ttl)
		c.order.MoveToFront(el)
		return
	}

	// Evict LRU entries if at capacity.
	for c.order.Len() >= c.maxSize {
		c.removeLocked(c.order.Back())
	}

	e := &cacheEntry{key: key, value: value, expiresAt: time.Now().Add(c.ttl)}
	el := c.order.PushFront(e)
	c.entries[key] = el
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.removeLocked(el)
	}
}

// Clear removes all entries.
func (c *Cache) Clear() {
	c.mu.Lock()
	c.entries = make(map[string]*list.Element)
	c.order.Init()
	c.mu.Unlock()
}

// GetOrCompute retrieves a cached value, or calls supplier to compute and cache it.
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// EvictExpired removes expired entries.
func (c *Cache) EvictExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.order.Back(); el != nil; {
		prev := el.Prev()
		e := el.Value.(*cacheEntry)
		if now.After(e.expiresAt) {
			c.removeLocked(el)
		}
		el = prev
	}
}

func (c *Cache) removeLocked(el *list.Element) {
	e := el.Value.(*cacheEntry)
	delete(c.entries, e.key)
	c.order.Remove(el)
}
