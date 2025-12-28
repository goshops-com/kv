//! Memory Cache Layer (L1)
//!
//! An LRU cache with size-based eviction that acts as the first tier
//! in our storage hierarchy.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for the memory cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum size in bytes
    pub max_size_bytes: usize,
    /// Maximum number of entries (optional additional limit)
    pub max_entries: Option<usize>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 64 * 1024 * 1024, // 64MB default
            max_entries: None,
        }
    }
}

/// A cached entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: Bytes,
    pub created_at: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    pub access_count: u64,
}

impl CacheEntry {
    fn new(value: Bytes) -> Self {
        let now = Utc::now();
        Self {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 1,
        }
    }

    fn size(&self) -> usize {
        self.value.len()
    }
}

/// Node in the LRU doubly-linked list
#[derive(Clone)]
struct LruNode {
    key: Bytes,
    prev: Option<Bytes>,
    next: Option<Bytes>,
}

/// Inner state of the cache (protected by RwLock)
struct CacheInner {
    entries: HashMap<Bytes, CacheEntry>,
    // LRU tracking
    lru_order: HashMap<Bytes, LruNode>,
    lru_head: Option<Bytes>, // Most recently used
    lru_tail: Option<Bytes>, // Least recently used
    // Stats
    current_size_bytes: usize,
    hits: u64,
    misses: u64,
}

impl CacheInner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru_order: HashMap::new(),
            lru_head: None,
            lru_tail: None,
            current_size_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Move a key to the front of the LRU list (most recently used)
    fn touch(&mut self, key: &Bytes) {
        if self.lru_head.as_ref() == Some(key) {
            return; // Already at head
        }

        // Remove from current position
        if let Some(node) = self.lru_order.get(key).cloned() {
            // Update neighbors
            if let Some(ref prev_key) = node.prev {
                if let Some(prev_node) = self.lru_order.get_mut(prev_key) {
                    prev_node.next = node.next.clone();
                }
            }
            if let Some(ref next_key) = node.next {
                if let Some(next_node) = self.lru_order.get_mut(next_key) {
                    next_node.prev = node.prev.clone();
                }
            }

            // Update tail if this was the tail
            if self.lru_tail.as_ref() == Some(key) {
                self.lru_tail = node.prev.clone();
            }
        }

        // Move to head
        let old_head = self.lru_head.take();
        if let Some(ref old_head_key) = old_head {
            if let Some(old_head_node) = self.lru_order.get_mut(old_head_key) {
                old_head_node.prev = Some(key.clone());
            }
        }

        if let Some(node) = self.lru_order.get_mut(key) {
            node.prev = None;
            node.next = old_head;
        }

        self.lru_head = Some(key.clone());

        // If no tail, this is the only element
        if self.lru_tail.is_none() {
            self.lru_tail = Some(key.clone());
        }
    }

    /// Add a new key to the LRU list at the head
    fn add_to_lru(&mut self, key: Bytes) {
        let old_head = self.lru_head.take();

        let node = LruNode {
            key: key.clone(),
            prev: None,
            next: old_head.clone(),
        };

        if let Some(ref old_head_key) = old_head {
            if let Some(old_head_node) = self.lru_order.get_mut(old_head_key) {
                old_head_node.prev = Some(key.clone());
            }
        }

        self.lru_order.insert(key.clone(), node);
        self.lru_head = Some(key.clone());

        if self.lru_tail.is_none() {
            self.lru_tail = Some(key);
        }
    }

    /// Remove a key from the LRU list
    fn remove_from_lru(&mut self, key: &Bytes) {
        if let Some(node) = self.lru_order.remove(key) {
            if let Some(ref prev_key) = node.prev {
                if let Some(prev_node) = self.lru_order.get_mut(prev_key) {
                    prev_node.next = node.next.clone();
                }
            } else {
                // This was the head
                self.lru_head = node.next.clone();
            }

            if let Some(ref next_key) = node.next {
                if let Some(next_node) = self.lru_order.get_mut(next_key) {
                    next_node.prev = node.prev.clone();
                }
            } else {
                // This was the tail
                self.lru_tail = node.prev.clone();
            }
        }
    }

    /// Evict the least recently used entry
    fn evict_lru(&mut self) -> Option<(Bytes, CacheEntry)> {
        let tail_key = self.lru_tail.clone()?;
        self.remove_from_lru(&tail_key);
        let entry = self.entries.remove(&tail_key)?;
        self.current_size_bytes = self.current_size_bytes.saturating_sub(entry.size());
        Some((tail_key, entry))
    }
}

/// Thread-safe LRU memory cache
pub struct MemoryCache {
    config: CacheConfig,
    inner: Arc<RwLock<CacheInner>>,
}

impl MemoryCache {
    /// Create a new memory cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            inner: Arc::new(RwLock::new(CacheInner::new())),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::copy_from_slice(key);
        let mut inner = self.inner.write();

        let result = if let Some(entry) = inner.entries.get_mut(&key) {
            entry.last_accessed = Utc::now();
            entry.access_count += 1;
            Some(entry.value.clone())
        } else {
            None
        };

        if result.is_some() {
            inner.hits += 1;
            inner.touch(&key);
        } else {
            inner.misses += 1;
        }

        result
    }

    /// Insert a value into the cache, evicting if necessary
    pub fn put(&self, key: &[u8], value: Bytes) -> Vec<(Bytes, CacheEntry)> {
        let key = Bytes::copy_from_slice(key);
        let entry_size = value.len();
        let mut evicted = Vec::new();

        let mut inner = self.inner.write();

        // Remove existing entry if present
        if let Some(old_entry) = inner.entries.remove(&key) {
            inner.current_size_bytes = inner.current_size_bytes.saturating_sub(old_entry.size());
            inner.remove_from_lru(&key);
        }

        // Evict until we have space
        while inner.current_size_bytes + entry_size > self.config.max_size_bytes {
            if let Some((evicted_key, evicted_entry)) = inner.evict_lru() {
                evicted.push((evicted_key, evicted_entry));
            } else {
                break;
            }
        }

        // Check entry count limit
        if let Some(max_entries) = self.config.max_entries {
            while inner.entries.len() >= max_entries {
                if let Some((evicted_key, evicted_entry)) = inner.evict_lru() {
                    evicted.push((evicted_key, evicted_entry));
                } else {
                    break;
                }
            }
        }

        // Insert new entry
        let entry = CacheEntry::new(value);
        inner.current_size_bytes += entry_size;
        inner.entries.insert(key.clone(), entry);
        inner.add_to_lru(key);

        evicted
    }

    /// Remove a value from the cache
    pub fn delete(&self, key: &[u8]) -> Option<CacheEntry> {
        let key = Bytes::copy_from_slice(key);
        let mut inner = self.inner.write();

        if let Some(entry) = inner.entries.remove(&key) {
            inner.current_size_bytes = inner.current_size_bytes.saturating_sub(entry.size());
            inner.remove_from_lru(&key);
            Some(entry)
        } else {
            None
        }
    }

    /// Check if a key exists in the cache (without updating LRU)
    pub fn contains(&self, key: &[u8]) -> bool {
        let key = Bytes::copy_from_slice(key);
        let inner = self.inner.read();
        inner.entries.contains_key(&key)
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read();
        CacheStats {
            entries: inner.entries.len(),
            size_bytes: inner.current_size_bytes,
            max_size_bytes: self.config.max_size_bytes,
            hits: inner.hits,
            misses: inner.misses,
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        inner.lru_order.clear();
        inner.lru_head = None;
        inner.lru_tail = None;
        inner.current_size_bytes = 0;
    }

    /// Get all keys in the cache (for testing/debugging)
    pub fn keys(&self) -> Vec<Bytes> {
        let inner = self.inner.read();
        inner.entries.keys().cloned().collect()
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== BASIC OPERATIONS ====================

    #[test]
    fn test_new_cache_is_empty() {
        let cache = MemoryCache::new(CacheConfig::default());
        let stats = cache.stats();

        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[test]
    fn test_put_and_get_single_entry() {
        let cache = MemoryCache::new(CacheConfig::default());

        let key = b"key1";
        let value = Bytes::from("value1");

        cache.put(key, value.clone());
        let retrieved = cache.get(key);

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), value);
    }

    #[test]
    fn test_get_nonexistent_key_returns_none() {
        let cache = MemoryCache::new(CacheConfig::default());

        let result = cache.get(b"nonexistent");

        assert!(result.is_none());
    }

    #[test]
    fn test_put_overwrites_existing_key() {
        let cache = MemoryCache::new(CacheConfig::default());

        let key = b"key1";
        cache.put(key, Bytes::from("value1"));
        cache.put(key, Bytes::from("value2"));

        let retrieved = cache.get(key);
        assert_eq!(retrieved.unwrap(), Bytes::from("value2"));

        // Should still be just one entry
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn test_delete_removes_entry() {
        let cache = MemoryCache::new(CacheConfig::default());

        let key = b"key1";
        cache.put(key, Bytes::from("value1"));

        let deleted = cache.delete(key);
        assert!(deleted.is_some());

        let retrieved = cache.get(key);
        assert!(retrieved.is_none());
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn test_delete_nonexistent_returns_none() {
        let cache = MemoryCache::new(CacheConfig::default());

        let deleted = cache.delete(b"nonexistent");
        assert!(deleted.is_none());
    }

    #[test]
    fn test_contains_returns_correct_state() {
        let cache = MemoryCache::new(CacheConfig::default());

        let key = b"key1";
        assert!(!cache.contains(key));

        cache.put(key, Bytes::from("value1"));
        assert!(cache.contains(key));

        cache.delete(key);
        assert!(!cache.contains(key));
    }

    #[test]
    fn test_clear_removes_all_entries() {
        let cache = MemoryCache::new(CacheConfig::default());

        cache.put(b"key1", Bytes::from("value1"));
        cache.put(b"key2", Bytes::from("value2"));
        cache.put(b"key3", Bytes::from("value3"));

        assert_eq!(cache.stats().entries, 3);

        cache.clear();

        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().size_bytes, 0);
    }

    // ==================== SIZE TRACKING ====================

    #[test]
    fn test_size_tracking_increases_on_put() {
        let cache = MemoryCache::new(CacheConfig::default());

        let value = Bytes::from("12345"); // 5 bytes
        cache.put(b"key1", value);

        assert_eq!(cache.stats().size_bytes, 5);
    }

    #[test]
    fn test_size_tracking_decreases_on_delete() {
        let cache = MemoryCache::new(CacheConfig::default());

        cache.put(b"key1", Bytes::from("12345")); // 5 bytes
        cache.delete(b"key1");

        assert_eq!(cache.stats().size_bytes, 0);
    }

    #[test]
    fn test_size_tracking_updates_on_overwrite() {
        let cache = MemoryCache::new(CacheConfig::default());

        cache.put(b"key1", Bytes::from("12345")); // 5 bytes
        cache.put(b"key1", Bytes::from("1234567890")); // 10 bytes

        assert_eq!(cache.stats().size_bytes, 10);
    }

    // ==================== LRU EVICTION ====================

    #[test]
    fn test_eviction_when_size_exceeded() {
        let config = CacheConfig {
            max_size_bytes: 10, // Only 10 bytes
            max_entries: None,
        };
        let cache = MemoryCache::new(config);

        // Insert 5 bytes
        cache.put(b"key1", Bytes::from("12345"));
        // Insert another 5 bytes - should fit
        cache.put(b"key2", Bytes::from("67890"));
        // Insert 5 more - should evict key1
        let evicted = cache.put(b"key3", Bytes::from("abcde"));

        assert!(!evicted.is_empty());
        assert!(cache.get(b"key1").is_none()); // key1 was evicted
        assert!(cache.get(b"key2").is_some());
        assert!(cache.get(b"key3").is_some());
    }

    #[test]
    fn test_lru_order_evicts_least_recently_used() {
        let config = CacheConfig {
            max_size_bytes: 15,
            max_entries: None,
        };
        let cache = MemoryCache::new(config);

        cache.put(b"key1", Bytes::from("11111")); // 5 bytes
        cache.put(b"key2", Bytes::from("22222")); // 5 bytes
        cache.put(b"key3", Bytes::from("33333")); // 5 bytes, total 15

        // Access key1, making it recently used
        cache.get(b"key1");

        // Insert new key, should evict key2 (LRU)
        cache.put(b"key4", Bytes::from("44444"));

        assert!(cache.get(b"key1").is_some()); // Recently accessed
        assert!(cache.get(b"key2").is_none()); // Should be evicted
        assert!(cache.get(b"key3").is_some());
        assert!(cache.get(b"key4").is_some());
    }

    #[test]
    fn test_eviction_returns_evicted_entries() {
        let config = CacheConfig {
            max_size_bytes: 5,
            max_entries: None,
        };
        let cache = MemoryCache::new(config);

        cache.put(b"key1", Bytes::from("12345"));
        let evicted = cache.put(b"key2", Bytes::from("67890"));

        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].0, Bytes::from("key1"));
        assert_eq!(evicted[0].1.value, Bytes::from("12345"));
    }

    #[test]
    fn test_max_entries_limit() {
        let config = CacheConfig {
            max_size_bytes: 1000,
            max_entries: Some(2),
        };
        let cache = MemoryCache::new(config);

        cache.put(b"key1", Bytes::from("value1"));
        cache.put(b"key2", Bytes::from("value2"));
        cache.put(b"key3", Bytes::from("value3")); // Should evict key1

        assert_eq!(cache.stats().entries, 2);
        assert!(cache.get(b"key1").is_none());
        assert!(cache.get(b"key2").is_some());
        assert!(cache.get(b"key3").is_some());
    }

    // ==================== STATISTICS ====================

    #[test]
    fn test_hit_miss_tracking() {
        let cache = MemoryCache::new(CacheConfig::default());

        cache.put(b"key1", Bytes::from("value1"));

        cache.get(b"key1"); // Hit
        cache.get(b"key1"); // Hit
        cache.get(b"key2"); // Miss
        cache.get(b"key3"); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_access_count_increments() {
        let cache = MemoryCache::new(CacheConfig::default());

        cache.put(b"key1", Bytes::from("value1"));
        cache.get(b"key1");
        cache.get(b"key1");
        cache.get(b"key1");

        // We need to expose access count or verify via delete
        let entry = cache.delete(b"key1").unwrap();
        assert_eq!(entry.access_count, 4); // 1 initial + 3 gets
    }

    // ==================== THREAD SAFETY ====================

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let cache = Arc::new(MemoryCache::new(CacheConfig {
            max_size_bytes: 10000,
            max_entries: None,
        }));

        let mut handles = vec![];

        // Spawn multiple writers
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key-{}-{}", i, j);
                    let value = format!("value-{}-{}", i, j);
                    cache_clone.put(key.as_bytes(), Bytes::from(value));
                }
            }));
        }

        // Spawn multiple readers
        for _ in 0..5 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..10 {
                    for j in 0..100 {
                        let key = format!("key-{}-{}", i, j);
                        let _ = cache_clone.get(key.as_bytes());
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should not panic or corrupt data
        let stats = cache.stats();
        assert!(stats.entries > 0);
    }
}
