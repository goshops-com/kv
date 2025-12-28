//! Disk Store Layer (L2)
//!
//! Persistent local storage using sled (LSM-tree based, pure Rust)

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiskError {
    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Key not found")]
    NotFound,
}

/// Configuration for the disk store
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Path to the data directory
    pub data_dir: String,
    /// Maximum size in bytes before triggering migration (threshold)
    pub max_size_bytes: u64,
    /// Age in seconds before data is eligible for migration to object storage
    pub migration_age_secs: u64,
    /// Flush every N writes (0 = disable periodic flush)
    pub flush_every_n_writes: u64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            max_size_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            migration_age_secs: 24 * 60 * 60,        // 24 hours
            flush_every_n_writes: 1000,
        }
    }
}

/// Stored entry with metadata (serialized to disk)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEntry {
    pub value: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl StoredEntry {
    fn new(value: Vec<u8>) -> Self {
        let now = Utc::now();
        Self {
            value,
            created_at: now,
            updated_at: now,
        }
    }

    fn size(&self) -> usize {
        self.value.len()
    }
}

/// Public entry returned from the disk store
#[derive(Debug, Clone)]
pub struct DiskEntry {
    pub value: Bytes,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub size_bytes: usize,
}

impl From<StoredEntry> for DiskEntry {
    fn from(stored: StoredEntry) -> Self {
        let size_bytes = stored.value.len();
        Self {
            value: Bytes::from(stored.value),
            created_at: stored.created_at,
            updated_at: stored.updated_at,
            size_bytes,
        }
    }
}

/// Result type for migration candidates
#[derive(Debug)]
pub struct MigrationCandidate {
    pub key: Bytes,
    pub entry: DiskEntry,
    pub reason: MigrationReason,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MigrationReason {
    Age,
    SpacePressure,
}

/// Disk storage layer using sled
pub struct DiskStore {
    db: sled::Db,
    config: DiskConfig,
    write_count: AtomicU64,
    total_size: AtomicU64,
}

impl DiskStore {
    /// Create a new disk store
    pub fn new(config: DiskConfig) -> Result<Self, DiskError> {
        let db = sled::open(&config.data_dir)?;

        // Calculate initial size
        let total_size = Self::calculate_size(&db);

        Ok(Self {
            db,
            config,
            write_count: AtomicU64::new(0),
            total_size: AtomicU64::new(total_size),
        })
    }

    /// Open an existing store or create new
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DiskError> {
        let config = DiskConfig {
            data_dir: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        Self::new(config)
    }

    fn calculate_size(db: &sled::Db) -> u64 {
        db.iter()
            .filter_map(|r| r.ok())
            .map(|(_, v)| v.len() as u64)
            .sum()
    }

    /// Get a value from disk
    pub fn get(&self, key: &[u8]) -> Result<Option<DiskEntry>, DiskError> {
        match self.db.get(key)? {
            Some(data) => {
                let stored: StoredEntry = serde_json::from_slice(&data)
                    .map_err(|e| DiskError::Serialization(e.to_string()))?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Put a value to disk (durable write)
    pub fn put(&self, key: &[u8], value: Bytes) -> Result<Option<DiskEntry>, DiskError> {
        let entry = StoredEntry::new(value.to_vec());
        let serialized = serde_json::to_vec(&entry)
            .map_err(|e| DiskError::Serialization(e.to_string()))?;

        let old_size = self
            .db
            .get(key)?
            .map(|v| v.len() as u64)
            .unwrap_or(0);

        let new_size = serialized.len() as u64;

        let old = self.db.insert(key, serialized)?;

        // Update total size atomically
        let size_diff = new_size as i64 - old_size as i64;
        if size_diff > 0 {
            self.total_size.fetch_add(size_diff as u64, Ordering::Relaxed);
        } else {
            self.total_size.fetch_sub((-size_diff) as u64, Ordering::Relaxed);
        }

        // Periodic flush
        let writes = self.write_count.fetch_add(1, Ordering::Relaxed);
        if self.config.flush_every_n_writes > 0
            && writes % self.config.flush_every_n_writes == 0
        {
            self.db.flush()?;
        }

        // Return old entry if it existed
        match old {
            Some(data) => {
                let stored: StoredEntry = serde_json::from_slice(&data)
                    .map_err(|e| DiskError::Serialization(e.to_string()))?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Delete a value from disk
    pub fn delete(&self, key: &[u8]) -> Result<Option<DiskEntry>, DiskError> {
        match self.db.remove(key)? {
            Some(data) => {
                let stored: StoredEntry = serde_json::from_slice(&data)
                    .map_err(|e| DiskError::Serialization(e.to_string()))?;

                // Update size
                self.total_size.fetch_sub(data.len() as u64, Ordering::Relaxed);

                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool, DiskError> {
        Ok(self.db.contains_key(key)?)
    }

    /// Force flush to disk
    pub fn flush(&self) -> Result<(), DiskError> {
        self.db.flush()?;
        Ok(())
    }

    /// Get approximate total size of stored data
    pub fn size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.db.len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.db.is_empty()
    }

    /// Get disk usage as a percentage of max size
    pub fn usage_percent(&self) -> f64 {
        let size = self.size() as f64;
        let max = self.config.max_size_bytes as f64;
        (size / max) * 100.0
    }

    /// Iterate over all keys
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.db
            .iter()
            .filter_map(|r| r.ok())
            .map(|(k, _)| Bytes::copy_from_slice(&k))
    }

    /// Get entries eligible for migration to object storage
    pub fn get_migration_candidates(&self, limit: usize) -> Result<Vec<MigrationCandidate>, DiskError> {
        let now = Utc::now();
        let age_threshold = chrono::Duration::seconds(self.config.migration_age_secs as i64);
        let space_pressure = self.usage_percent() > 80.0;

        let mut candidates = Vec::new();

        for result in self.db.iter() {
            if candidates.len() >= limit {
                break;
            }

            let (key, data) = result?;
            let stored: StoredEntry = serde_json::from_slice(&data)
                .map_err(|e| DiskError::Serialization(e.to_string()))?;

            let age = now.signed_duration_since(stored.created_at);

            let reason = if age > age_threshold {
                Some(MigrationReason::Age)
            } else if space_pressure {
                Some(MigrationReason::SpacePressure)
            } else {
                None
            };

            if let Some(reason) = reason {
                candidates.push(MigrationCandidate {
                    key: Bytes::copy_from_slice(&key),
                    entry: stored.into(),
                    reason,
                });
            }
        }

        Ok(candidates)
    }

    /// Remove a batch of keys (after successful migration)
    pub fn remove_batch(&self, keys: &[Bytes]) -> Result<usize, DiskError> {
        let mut removed = 0;
        for key in keys {
            if self.delete(key)?.is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_store() -> (DiskStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            max_size_bytes: 1024 * 1024, // 1MB for tests
            migration_age_secs: 1,       // 1 second for tests
            flush_every_n_writes: 0,     // Disable for tests
        };
        let store = DiskStore::new(config).unwrap();
        (store, temp_dir)
    }

    // ==================== BASIC OPERATIONS ====================

    #[test]
    fn test_new_store_is_empty() {
        let (store, _temp) = create_temp_store();

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn test_put_and_get_single_entry() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        let value = Bytes::from("value1");

        store.put(key, value.clone()).unwrap();
        let retrieved = store.get(key).unwrap();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, value);
    }

    #[test]
    fn test_get_nonexistent_key_returns_none() {
        let (store, _temp) = create_temp_store();

        let result = store.get(b"nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_put_returns_old_value() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");

        let old1 = store.put(key, value1.clone()).unwrap();
        assert!(old1.is_none());

        let old2 = store.put(key, value2.clone()).unwrap();
        assert!(old2.is_some());
        assert_eq!(old2.unwrap().value, value1);

        let current = store.get(key).unwrap().unwrap();
        assert_eq!(current.value, value2);
    }

    #[test]
    fn test_delete_removes_entry() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        store.put(key, Bytes::from("value1")).unwrap();

        let deleted = store.delete(key).unwrap();
        assert!(deleted.is_some());
        assert_eq!(deleted.unwrap().value, Bytes::from("value1"));

        let retrieved = store.get(key).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_delete_nonexistent_returns_none() {
        let (store, _temp) = create_temp_store();

        let deleted = store.delete(b"nonexistent").unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_contains_returns_correct_state() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        assert!(!store.contains(key).unwrap());

        store.put(key, Bytes::from("value1")).unwrap();
        assert!(store.contains(key).unwrap());

        store.delete(key).unwrap();
        assert!(!store.contains(key).unwrap());
    }

    // ==================== PERSISTENCE ====================

    #[test]
    fn test_data_persists_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        // Write data
        {
            let config = DiskConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let store = DiskStore::new(config).unwrap();
            store.put(b"key1", Bytes::from("value1")).unwrap();
            store.put(b"key2", Bytes::from("value2")).unwrap();
            store.flush().unwrap();
        }

        // Reopen and verify
        {
            let config = DiskConfig {
                data_dir: path,
                ..Default::default()
            };
            let store = DiskStore::new(config).unwrap();

            assert_eq!(store.len(), 2);
            assert_eq!(store.get(b"key1").unwrap().unwrap().value, Bytes::from("value1"));
            assert_eq!(store.get(b"key2").unwrap().unwrap().value, Bytes::from("value2"));
        }
    }

    // ==================== SIZE TRACKING ====================

    #[test]
    fn test_size_increases_on_put() {
        let (store, _temp) = create_temp_store();

        let initial_size = store.size();
        store.put(b"key1", Bytes::from("value1")).unwrap();

        assert!(store.size() > initial_size);
    }

    #[test]
    fn test_size_decreases_on_delete() {
        let (store, _temp) = create_temp_store();

        store.put(b"key1", Bytes::from("value1")).unwrap();
        let size_after_put = store.size();

        store.delete(b"key1").unwrap();
        assert!(store.size() < size_after_put);
    }

    #[test]
    fn test_len_counts_entries() {
        let (store, _temp) = create_temp_store();

        assert_eq!(store.len(), 0);

        store.put(b"key1", Bytes::from("value1")).unwrap();
        assert_eq!(store.len(), 1);

        store.put(b"key2", Bytes::from("value2")).unwrap();
        assert_eq!(store.len(), 2);

        store.put(b"key1", Bytes::from("updated")).unwrap(); // Overwrite
        assert_eq!(store.len(), 2);

        store.delete(b"key1").unwrap();
        assert_eq!(store.len(), 1);
    }

    // ==================== METADATA ====================

    #[test]
    fn test_entry_has_timestamps() {
        let (store, _temp) = create_temp_store();

        let before = Utc::now();
        store.put(b"key1", Bytes::from("value1")).unwrap();
        let after = Utc::now();

        let entry = store.get(b"key1").unwrap().unwrap();

        assert!(entry.created_at >= before);
        assert!(entry.created_at <= after);
        assert!(entry.updated_at >= before);
        assert!(entry.updated_at <= after);
    }

    #[test]
    fn test_entry_has_size() {
        let (store, _temp) = create_temp_store();

        let value = Bytes::from("hello world");
        store.put(b"key1", value.clone()).unwrap();

        let entry = store.get(b"key1").unwrap().unwrap();
        assert_eq!(entry.size_bytes, value.len());
    }

    // ==================== ITERATION ====================

    #[test]
    fn test_keys_iterator() {
        let (store, _temp) = create_temp_store();

        store.put(b"key1", Bytes::from("value1")).unwrap();
        store.put(b"key2", Bytes::from("value2")).unwrap();
        store.put(b"key3", Bytes::from("value3")).unwrap();

        let keys: Vec<Bytes> = store.keys().collect();
        assert_eq!(keys.len(), 3);

        // Note: sled maintains lexicographic order
        assert!(keys.contains(&Bytes::from("key1")));
        assert!(keys.contains(&Bytes::from("key2")));
        assert!(keys.contains(&Bytes::from("key3")));
    }

    // ==================== MIGRATION ====================

    #[test]
    fn test_migration_candidates_by_age() {
        let (store, _temp) = create_temp_store();

        // Add entries
        store.put(b"key1", Bytes::from("value1")).unwrap();
        store.put(b"key2", Bytes::from("value2")).unwrap();

        // Wait for entries to age past threshold (1 second in test config)
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let candidates = store.get_migration_candidates(10).unwrap();

        assert_eq!(candidates.len(), 2);
        assert!(candidates.iter().all(|c| c.reason == MigrationReason::Age));
    }

    #[test]
    fn test_remove_batch() {
        let (store, _temp) = create_temp_store();

        store.put(b"key1", Bytes::from("value1")).unwrap();
        store.put(b"key2", Bytes::from("value2")).unwrap();
        store.put(b"key3", Bytes::from("value3")).unwrap();

        let keys = vec![Bytes::from("key1"), Bytes::from("key3")];
        let removed = store.remove_batch(&keys).unwrap();

        assert_eq!(removed, 2);
        assert_eq!(store.len(), 1);
        assert!(store.get(b"key2").unwrap().is_some());
    }

    // ==================== USAGE PERCENT ====================

    #[test]
    fn test_usage_percent() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            max_size_bytes: 1000, // Small max for testing
            migration_age_secs: 3600,
            flush_every_n_writes: 0,
        };
        let store = DiskStore::new(config).unwrap();

        assert_eq!(store.usage_percent(), 0.0);

        // Add some data
        store.put(b"key1", Bytes::from("x".repeat(100))).unwrap();

        // Usage should be > 0 now
        assert!(store.usage_percent() > 0.0);
    }
}
