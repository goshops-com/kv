//! Disk Store Layer (L2)
//!
//! Persistent local storage using RocksDB with zstd compression

use bytes::Bytes;
use chrono::{DateTime, Utc};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiskError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
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
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

/// Lightweight metadata-only view for TTL checks.
/// Uses IgnoredAny to skip the value field (~287KB avg) without allocating.
#[derive(Deserialize)]
pub struct StoredEntryMeta {
    #[serde(deserialize_with = "serde::de::IgnoredAny::deserialize")]
    _value: serde::de::IgnoredAny,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

impl StoredEntry {
    pub(crate) fn new(value: Vec<u8>) -> Self {
        let now = Utc::now();
        Self {
            value,
            created_at: now,
            updated_at: now,
            ttl_secs: None,
        }
    }

    pub(crate) fn new_with_ttl(value: Vec<u8>, ttl_secs: Option<u64>) -> Self {
        let now = Utc::now();
        Self {
            value,
            created_at: now,
            updated_at: now,
            ttl_secs,
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

/// Version byte for MessagePack serialization (JSON has no prefix, starts with '{')
const MSGPACK_VERSION: u8 = 0x01;

/// Serialize a StoredEntry to bytes (MessagePack with version prefix)
fn serialize_entry(entry: &StoredEntry) -> Result<Vec<u8>, DiskError> {
    let msgpack = rmp_serde::to_vec(entry)
        .map_err(|e| DiskError::Serialization(e.to_string()))?;
    let mut buf = Vec::with_capacity(1 + msgpack.len());
    buf.push(MSGPACK_VERSION);
    buf.extend(msgpack);
    Ok(buf)
}

/// Deserialize metadata only, skipping the value field (for TTL scans)
fn deserialize_entry_meta(data: &[u8]) -> Result<StoredEntryMeta, DiskError> {
    if data.first() == Some(&MSGPACK_VERSION) {
        rmp_serde::from_slice(&data[1..])
            .map_err(|e| DiskError::Serialization(e.to_string()))
    } else {
        serde_json::from_slice(data)
            .map_err(|e| DiskError::Serialization(e.to_string()))
    }
}

/// Deserialize a StoredEntry from bytes (auto-detects JSON vs MessagePack)
fn deserialize_entry(data: &[u8]) -> Result<StoredEntry, DiskError> {
    if data.first() == Some(&MSGPACK_VERSION) {
        rmp_serde::from_slice(&data[1..])
            .map_err(|e| DiskError::Serialization(e.to_string()))
    } else {
        // Legacy JSON format (starts with '{' = 0x7B)
        serde_json::from_slice(data)
            .map_err(|e| DiskError::Serialization(e.to_string()))
    }
}

/// Disk storage layer using RocksDB
pub struct DiskStore {
    db: DB,
    config: DiskConfig,
    write_count: AtomicU64,
    entry_count: AtomicUsize,
}

fn make_opts() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    // Block cache: 256MB — controls RSS for reads
    let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    block_opts.set_block_size(16 * 1024);
    opts.set_block_based_table_factory(&block_opts);

    // Larger write buffers = fewer L0 flushes = fewer compactions
    opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB (was 32MB)
    opts.set_max_write_buffer_number(3);

    // L0 compaction triggers: tolerate more L0 files before compacting
    opts.set_level_zero_file_num_compaction_trigger(8);  // default 4
    opts.set_level_zero_slowdown_writes_trigger(20);     // default 20
    opts.set_level_zero_stop_writes_trigger(36);         // default 36

    // Larger L1 target = less write amplification across levels
    opts.set_max_bytes_for_level_base(512 * 1024 * 1024); // 512MB (default 256MB)

    // Limit compaction concurrency to reduce CPU spikes
    opts.set_max_background_jobs(2);

    // BlobDB: separate large values (>4KB) into blob files.
    // Only small key pointers stay in the LSM tree, drastically
    // reducing write amplification for our ~287KB average values.
    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(4096); // values > 4KB go to blob files
    opts.set_blob_file_size(256 * 1024 * 1024); // 256MB blob files
    opts.set_blob_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_enable_blob_gc(true); // garbage collect old blobs
    opts.set_blob_gc_age_cutoff(0.25); // GC blobs when 25% is garbage

    // Limit total WAL size
    opts.set_max_total_wal_size(128 * 1024 * 1024);
    // Limit LOG file accumulation
    opts.set_keep_log_file_num(5);
    opts.set_max_log_file_size(10 * 1024 * 1024);
    opts
}

impl DiskStore {
    /// Create a new disk store
    pub fn new(config: DiskConfig) -> Result<Self, DiskError> {
        let opts = make_opts();
        let db = DB::open(&opts, &config.data_dir)?;

        // Estimate entry count from RocksDB metadata (O(1), no full-scan)
        let entry_count = db
            .property_int_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .unwrap_or(0) as usize;

        Ok(Self {
            db,
            config,
            write_count: AtomicU64::new(0),
            entry_count: AtomicUsize::new(entry_count),
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

    /// Get actual disk usage from RocksDB (SST files + WAL)
    fn disk_size(db: &DB) -> u64 {
        let sst = db
            .property_int_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0);
        let wal = db
            .property_int_value("rocksdb.cur-size-all-mem-tables")
            .ok()
            .flatten()
            .unwrap_or(0);
        sst + wal
    }

    /// Get a value from disk
    pub fn get(&self, key: &[u8]) -> Result<Option<DiskEntry>, DiskError> {
        match self.db.get(key)? {
            Some(data) => {
                let stored = deserialize_entry(&data)?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Put a value to disk (durable write)
    pub fn put(&self, key: &[u8], value: Bytes) -> Result<Option<DiskEntry>, DiskError> {
        self.put_with_ttl(key, value, None)
    }

    pub fn put_with_ttl(&self, key: &[u8], value: Bytes, ttl_secs: Option<u64>) -> Result<Option<DiskEntry>, DiskError> {
        let entry = StoredEntry::new_with_ttl(value.to_vec(), ttl_secs);
        let serialized = serialize_entry(&entry)?;

        // Read old value before overwriting
        let old = self.db.get(key)?;

        self.db.put(key, &serialized)?;

        // Track entry count
        if old.is_none() {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
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
                let stored = deserialize_entry(&data)?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Delete a value from disk
    pub fn delete(&self, key: &[u8]) -> Result<Option<DiskEntry>, DiskError> {
        match self.db.get(key)? {
            Some(data) => {
                self.db.delete(key)?;
                self.entry_count.fetch_sub(1, Ordering::Relaxed);

                let stored = deserialize_entry(&data)?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool, DiskError> {
        Ok(self.db.get(key)?.is_some())
    }

    /// Force flush to disk
    pub fn flush(&self) -> Result<(), DiskError> {
        self.db.flush()?;
        Ok(())
    }

    /// Get actual disk size (SST + WAL)
    pub fn size(&self) -> u64 {
        Self::disk_size(&self.db)
    }

    /// Get number of entries (O(1) via atomic counter)
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.entry_count.load(Ordering::Relaxed) == 0
    }

    /// Get disk usage as a percentage of max size
    pub fn usage_percent(&self) -> f64 {
        let size = self.size() as f64;
        let max = self.config.max_size_bytes as f64;
        (size / max) * 100.0
    }

    /// Iterate over all entries with their keys and metadata
    pub fn db_iter(&self) -> impl Iterator<Item = Result<(Vec<u8>, StoredEntry), DiskError>> + '_ {
        self.db.iterator(rocksdb::IteratorMode::Start).map(|result| {
            let (key, data) = result.map_err(DiskError::from)?;
            let entry = deserialize_entry(&data)?;
            Ok((key.to_vec(), entry))
        })
    }

    /// Iterate over all entries with metadata only (skips value deserialization)
    pub fn db_iter_meta(&self) -> impl Iterator<Item = Result<(Vec<u8>, StoredEntryMeta), DiskError>> + '_ {
        self.db.iterator(rocksdb::IteratorMode::Start).map(|result| {
            let (key, data) = result.map_err(DiskError::from)?;
            let meta = deserialize_entry_meta(&data)?;
            Ok((key.to_vec(), meta))
        })
    }

    /// Iterate over all keys
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|r| r.ok())
            .map(|(k, _)| Bytes::copy_from_slice(&k))
    }

    /// Get entries eligible for migration to object storage
    pub fn get_migration_candidates(&self, limit: usize) -> Result<Vec<MigrationCandidate>, DiskError> {
        let now = Utc::now();
        let age_threshold = chrono::Duration::seconds(self.config.migration_age_secs as i64);
        let space_pressure = self.usage_percent() > 80.0;

        let mut candidates = Vec::new();

        for result in self.db.iterator(rocksdb::IteratorMode::Start) {
            if candidates.len() >= limit {
                break;
            }

            let (key, data) = result?;
            let stored = deserialize_entry(&data)?;

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

        store.put(b"key1", Bytes::from("x".repeat(1000))).unwrap();
        store.flush().unwrap(); // flush to SST so size is visible

        assert!(store.size() > 0);
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

    // ==================== SERIALIZATION COMPAT ====================

    #[test]
    fn test_reads_legacy_json_entries() {
        let (store, _temp) = create_temp_store();

        // Simulate a legacy JSON entry written by old code (no version prefix)
        let legacy = StoredEntry::new_with_ttl(b"hello world".to_vec(), Some(3600));
        let json_bytes = serde_json::to_vec(&legacy).unwrap();
        store.db.put(b"legacy_key", &json_bytes).unwrap();

        // New code must read it correctly
        let entry = store.get(b"legacy_key").unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("hello world"));
    }

    #[test]
    fn test_new_writes_are_msgpack() {
        let (store, _temp) = create_temp_store();

        store.put(b"key1", Bytes::from("value1")).unwrap();

        // Raw bytes should start with MSGPACK_VERSION (0x01), not '{' (0x7B)
        let raw = store.db.get(b"key1").unwrap().unwrap();
        assert_eq!(raw[0], MSGPACK_VERSION);
    }

    #[test]
    fn test_mixed_json_and_msgpack_entries() {
        let (store, _temp) = create_temp_store();

        // Write a legacy JSON entry directly
        let legacy = StoredEntry::new_with_ttl(b"old_value".to_vec(), None);
        let json_bytes = serde_json::to_vec(&legacy).unwrap();
        store.db.put(b"old_key", &json_bytes).unwrap();

        // Write a new entry via the API (will be msgpack)
        store.put(b"new_key", Bytes::from("new_value")).unwrap();

        // Both should be readable
        let old = store.get(b"old_key").unwrap().unwrap();
        assert_eq!(old.value, Bytes::from("old_value"));

        let new = store.get(b"new_key").unwrap().unwrap();
        assert_eq!(new.value, Bytes::from("new_value"));
    }

    // ==================== USAGE PERCENT ====================

    #[test]
    fn test_usage_percent() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            max_size_bytes: 100_000, // 100KB max for testing
            migration_age_secs: 3600,
            flush_every_n_writes: 0,
        };
        let store = DiskStore::new(config).unwrap();

        // Add some data and flush to SST
        store.put(b"key1", Bytes::from("x".repeat(1000))).unwrap();
        store.flush().unwrap();

        // Usage should be > 0 now
        assert!(store.usage_percent() > 0.0);
    }
}
