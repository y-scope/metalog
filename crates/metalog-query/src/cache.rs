use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

/// TTL cache with O(1) eviction for query filter rewriting results.
///
/// Entries expire after `ttl` and the oldest entry is evicted when `max_size`
/// is exceeded. Insertion order is tracked via a `VecDeque` for O(1) eviction
/// without scanning the entire map.
///
/// Thread-safe via external locking (the caller wraps in `RwLock`).
pub struct Cache {
    entries: HashMap<String, CacheEntry>,
    /// Insertion-order queue for O(1) oldest-entry eviction.
    order: VecDeque<String>,
    ttl: Duration,
    max_size: usize,
}

struct CacheEntry {
    value: String,
    inserted_at: Instant,
}

/// Default TTL for cache entries.
const DEFAULT_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// Default maximum entries.
const DEFAULT_MAX_SIZE: usize = 10_000;

impl Cache {
    /// Creates a new cache with default TTL (5 min) and max size (10k).
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            ttl: DEFAULT_TTL,
            max_size: DEFAULT_MAX_SIZE,
        }
    }

    /// Creates a cache with custom TTL and max size.
    pub fn with_config(ttl: Duration, max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            ttl,
            max_size,
        }
    }

    /// Gets a cached value if it exists and hasn't expired.
    pub fn get(&self, key: &str) -> Option<&str> {
        let entry = self.entries.get(key)?;
        if entry.inserted_at.elapsed() > self.ttl {
            return None;
        }
        Some(&entry.value)
    }

    /// Inserts or updates a cache entry.
    pub fn set(&mut self, key: String, value: String) {
        if self.entries.contains_key(&key) {
            // Update existing — no order change needed.
            self.entries.insert(
                key,
                CacheEntry {
                    value,
                    inserted_at: Instant::now(),
                },
            );
            return;
        }

        // Evict oldest if at capacity.
        if self.entries.len() >= self.max_size {
            self.evict_oldest();
        }

        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Gets a cached value, or computes and caches it.
    pub fn get_or_compute<F, E>(&mut self, key: &str, compute: F) -> Result<String, E>
    where
        F: FnOnce() -> Result<String, E>, {
        if let Some(val) = self.get(key) {
            return Ok(val.to_string());
        }
        let val = compute()?;
        self.set(key.to_string(), val.clone());
        Ok(val)
    }

    /// Removes expired entries.
    pub fn evict_expired(&mut self) {
        self.entries
            .retain(|_, entry| entry.inserted_at.elapsed() <= self.ttl);
        self.order.retain(|key| self.entries.contains_key(key));
    }

    /// Removes the oldest entry. O(1) amortized via VecDeque.
    fn evict_oldest(&mut self) {
        // Pop from front, skipping keys already removed (by update or expiry).
        while let Some(key) = self.order.pop_front() {
            if self.entries.remove(&key).is_some() {
                return;
            }
        }
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clears all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let mut cache = Cache::new();
        cache.set("key1".into(), "value1".into());
        assert_eq!(cache.get("key1"), Some("value1"));
    }

    #[test]
    fn missing_key() {
        let cache = Cache::new();
        assert_eq!(cache.get("missing"), None);
    }

    #[test]
    fn expired_entry() {
        let mut cache = Cache::with_config(Duration::from_nanos(1), 100);
        cache.set("key".into(), "val".into());
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(cache.get("key"), None);
    }

    #[test]
    fn evict_oldest_on_overflow() {
        let mut cache = Cache::with_config(Duration::from_secs(60), 2);
        cache.set("a".into(), "1".into());
        cache.set("b".into(), "2".into());
        cache.set("c".into(), "3".into()); // Should evict "a".
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("a"), None);
    }

    #[test]
    fn update_existing_does_not_evict() {
        let mut cache = Cache::with_config(Duration::from_secs(60), 2);
        cache.set("a".into(), "1".into());
        cache.set("b".into(), "2".into());
        cache.set("a".into(), "updated".into()); // Update, not insert.
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("a"), Some("updated"));
        assert_eq!(cache.get("b"), Some("2"));
    }

    #[test]
    fn get_or_compute() {
        let mut cache = Cache::new();
        let val = cache
            .get_or_compute("key", || Ok::<_, String>("computed".into()))
            .unwrap();
        assert_eq!(val, "computed");
        // Second call should return cached value.
        let val2: Result<String, String> =
            cache.get_or_compute("key", || panic!("should not be called"));
        assert_eq!(val2.unwrap(), "computed");
    }

    #[test]
    fn evict_expired() {
        let mut cache = Cache::with_config(Duration::from_nanos(1), 100);
        cache.set("key".into(), "val".into());
        std::thread::sleep(Duration::from_millis(1));
        cache.evict_expired();
        assert!(cache.is_empty());
    }

    #[test]
    fn clear() {
        let mut cache = Cache::new();
        cache.set("a".into(), "1".into());
        cache.set("b".into(), "2".into());
        cache.clear();
        assert!(cache.is_empty());
    }
}
