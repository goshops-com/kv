//! Tiered Storage Engine
//!
//! Combines memory cache, disk storage, and object storage into a unified
//! key-value store with automatic data tiering.

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::disk::{DiskConfig, DiskError, DiskStore, MigrationReason};
use crate::memory::{CacheConfig, MemoryCache};
use crate::object::{ObjectConfig, ObjectError, ObjectMetadata, ObjectStore};

/// zstd frame magic number (little-endian)
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Compress a value with zstd (level 1 = fast)
fn compress_value(data: &[u8]) -> Result<Bytes, EngineError> {
    let compressed = zstd::encode_all(data, 1)
        .map_err(|e| EngineError::Disk(DiskError::Serialization(e.to_string())))?;
    Ok(Bytes::from(compressed))
}

/// Decompress if the value starts with zstd magic, otherwise return as-is.
/// Safe because JSON values start with '{'/'"'/etc (0x7B/0x22), never 0x28.
fn decompress_if_needed(data: &Bytes) -> Result<Bytes, EngineError> {
    if data.len() >= 4 && data[..4] == ZSTD_MAGIC {
        let decompressed = zstd::decode_all(data.as_ref())
            .map_err(|e| EngineError::Disk(DiskError::Serialization(e.to_string())))?;
        Ok(Bytes::from(decompressed))
    } else {
        Ok(data.clone())
    }
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Disk error: {0}")]
    Disk(#[from] DiskError),
    #[error("Object storage error: {0}")]
    Object(#[from] ObjectError),
    #[error("Key not found")]
    NotFound,
    #[error("Engine not initialized")]
    NotInitialized,
}

/// A TTL rule: keys starting with `prefix` expire after `ttl_secs`
#[derive(Debug, Clone)]
pub struct TtlRule {
    pub prefix: String,
    pub ttl_secs: u64,
}

/// Configuration for the tiered engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub memory: CacheConfig,
    pub disk: DiskConfig,
    pub object: ObjectConfig,
    /// Number of candidates to migrate per batch
    pub migration_batch_size: usize,
    /// Disk usage threshold (%) that triggers migration
    pub migration_threshold_percent: f64,
    /// TTL rules by key prefix. Keys not matching any rule live forever.
    pub ttl_rules: Vec<TtlRule>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            memory: CacheConfig::default(),
            disk: DiskConfig::default(),
            object: ObjectConfig::default(),
            migration_batch_size: 100,
            migration_threshold_percent: 80.0,
            ttl_rules: vec![],
        }
    }
}

/// Entry metadata from any tier
#[derive(Debug, Clone)]
pub struct EntryInfo {
    pub value: Bytes,
    pub created_at: DateTime<Utc>,
    pub tier: StorageTier,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageTier {
    Memory,
    Disk,
    Object,
}

/// Statistics about the engine
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub memory_entries: usize,
    pub memory_size_bytes: usize,
    pub memory_hits: u64,
    pub memory_misses: u64,
    pub disk_entries: usize,
    pub disk_size_bytes: u64,
    pub object_entries: usize,
    pub migrations_completed: u64,
}

/// The main tiered storage engine
pub struct TieredEngine {
    memory: MemoryCache,
    disk: DiskStore,
    object: Arc<ObjectStore>,
    config: EngineConfig,
    migrations_completed: RwLock<u64>,
}

impl TieredEngine {
    /// Create a new tiered engine
    pub fn new(
        memory: MemoryCache,
        disk: DiskStore,
        object: Arc<ObjectStore>,
        config: EngineConfig,
    ) -> Self {
        Self {
            memory,
            disk,
            object,
            config,
            migrations_completed: RwLock::new(0),
        }
    }

    /// Create with default configuration and in-memory object store (for testing)
    pub fn with_config(config: EngineConfig) -> Result<Self, EngineError> {
        let memory = MemoryCache::new(config.memory.clone());
        let disk = DiskStore::new(config.disk.clone())?;
        let object = Arc::new(ObjectStore::in_memory(config.object.clone()));

        Ok(Self::new(memory, disk, object, config))
    }

    /// Get a value, checking all tiers
    ///
    /// Read path: Memory → Disk → Object Storage
    /// When found in a lower tier, the value is promoted to cache.
    pub async fn get(&self, key: &[u8]) -> Result<Option<EntryInfo>, EngineError> {
        let key_str = String::from_utf8_lossy(key);
        debug!(key = %key_str, "Getting key");

        // L1: Check memory cache (stores compressed values)
        if let Some(cached) = self.memory.get(key) {
            debug!(key = %key_str, "Cache hit");
            let value = decompress_if_needed(&cached)?;
            return Ok(Some(EntryInfo {
                value,
                created_at: Utc::now(), // Approximate
                tier: StorageTier::Memory,
            }));
        }

        // L2: Check disk (values may be compressed or legacy uncompressed)
        if let Some(entry) = self.disk.get(key)? {
            debug!(key = %key_str, "Disk hit, promoting to cache");
            // Promote raw (possibly compressed) value to cache
            self.memory.put(key, entry.value.clone());
            let value = decompress_if_needed(&entry.value)?;
            return Ok(Some(EntryInfo {
                value,
                created_at: entry.created_at,
                tier: StorageTier::Disk,
            }));
        }

        // L3: Check object storage
        let object_key = Self::to_object_key(key);
        match self.object.get(&object_key).await {
            Ok(entry) => {
                debug!(key = %key_str, "Object storage hit, promoting to cache");
                let value = entry.value.clone();
                // Promote to cache (not disk, as it was migrated from disk)
                self.memory.put(key, value.clone());
                let value = decompress_if_needed(&value)?;
                Ok(Some(EntryInfo {
                    value,
                    created_at: entry.metadata.created_at,
                    tier: StorageTier::Object,
                }))
            }
            Err(ObjectError::NotFound(_)) => {
                debug!(key = %key_str, "Key not found in any tier");
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Put a value (writes to disk and cache)
    ///
    /// Write path: Memory + Disk → ACK
    /// The durability guarantee is satisfied when disk write completes.
    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<(), EngineError> {
        self.put_with_ttl(key, value, None).await
    }

    pub async fn put_with_ttl(&self, key: &[u8], value: Bytes, ttl_secs: Option<u64>) -> Result<(), EngineError> {
        let key_str = String::from_utf8_lossy(key);
        debug!(key = %key_str, size = value.len(), "Putting key");

        // Compress value before storing (JSON 287KB → ~30KB with zstd)
        let compressed = compress_value(&value)?;

        // Write compressed to disk (durability)
        self.disk.put_with_ttl(key, compressed.clone(), ttl_secs)?;

        // Cache compressed value (10x more entries fit in cache)
        let evicted = self.memory.put(key, compressed);

        // If cache evicted entries, they're already on disk, so no action needed
        if !evicted.is_empty() {
            debug!(count = evicted.len(), "Cache evicted entries");
        }

        Ok(())
    }

    /// Delete a value from all tiers
    pub async fn delete(&self, key: &[u8]) -> Result<bool, EngineError> {
        let key_str = String::from_utf8_lossy(key);
        debug!(key = %key_str, "Deleting key");

        let mut deleted = false;

        // Remove from cache
        if self.memory.delete(key).is_some() {
            deleted = true;
        }

        // Remove from disk
        if self.disk.delete(key)?.is_some() {
            deleted = true;
        }

        // Remove from object storage
        let object_key = Self::to_object_key(key);
        if self.object.exists(&object_key).await? {
            self.object.delete(&object_key).await?;
            deleted = true;
        }

        Ok(deleted)
    }

    /// Check if a key exists in any tier
    pub async fn contains(&self, key: &[u8]) -> Result<bool, EngineError> {
        // Check memory
        if self.memory.contains(key) {
            return Ok(true);
        }

        // Check disk
        if self.disk.contains(key)? {
            return Ok(true);
        }

        // Check object storage
        let object_key = Self::to_object_key(key);
        Ok(self.object.exists(&object_key).await?)
    }

    /// Run migration from disk to object storage
    ///
    /// This should be called periodically by a background task.
    pub async fn run_migration(&self) -> Result<usize, EngineError> {
        // Collect candidates in a blocking context (scans RocksDB)
        let batch_size = self.config.migration_batch_size;
        let candidates = tokio::task::block_in_place(|| {
            self.disk.get_migration_candidates(batch_size)
        })?;

        if candidates.is_empty() {
            return Ok(0);
        }

        info!(count = candidates.len(), "Starting migration batch");

        let mut migrated_keys = Vec::new();

        for candidate in &candidates {
            let object_key = Self::to_object_key(&candidate.key);

            let metadata = ObjectMetadata {
                size_bytes: candidate.entry.size_bytes,
                created_at: candidate.entry.created_at,
                original_key: String::from_utf8_lossy(&candidate.key).to_string(),
                content_type: "application/octet-stream".to_string(),
            };

            // Upload to object storage
            if let Err(e) = self
                .object
                .put_with_metadata(&object_key, candidate.entry.value.clone(), metadata)
                .await
            {
                warn!(
                    key = %String::from_utf8_lossy(&candidate.key),
                    error = %e,
                    "Failed to migrate key to object storage"
                );
                continue;
            }

            migrated_keys.push(candidate.key.clone());

            debug!(
                key = %String::from_utf8_lossy(&candidate.key),
                reason = ?candidate.reason,
                "Migrated key to object storage"
            );
        }

        // Remove migrated keys from disk
        let removed = self.disk.remove_batch(&migrated_keys)?;

        // Update stats
        {
            let mut completed = self.migrations_completed.write().await;
            *completed += removed as u64;
        }

        info!(count = removed, "Migration batch completed");

        Ok(removed)
    }

    /// Run TTL cleanup: delete expired entries from all tiers
    pub async fn run_ttl_cleanup(&self) -> Result<usize, EngineError> {
        let now = Utc::now();
        let ttl_rules = self.config.ttl_rules.clone();

        // Phase 1: Collect expired keys in a blocking context.
        // Uses metadata-only deserialization (skips the ~287KB value field)
        // to minimize CPU and memory during the scan.
        let expired_keys: Vec<Bytes> = tokio::task::block_in_place(|| {
            let mut expired = Vec::new();

            for result in self.disk.db_iter_meta() {
                let (key_bytes, meta) = match result {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };

                // Check per-key TTL first
                if let Some(ttl_secs) = meta.ttl_secs {
                    if now.signed_duration_since(meta.updated_at) > Duration::seconds(ttl_secs as i64) {
                        expired.push(Bytes::copy_from_slice(&key_bytes));
                        continue;
                    }
                }

                // Fall back to prefix-based TTL rules
                let key_str = String::from_utf8_lossy(&key_bytes);
                let ttl = ttl_rules.iter().find_map(|rule| {
                    if key_str.starts_with(&rule.prefix) {
                        Some(Duration::seconds(rule.ttl_secs as i64))
                    } else {
                        None
                    }
                });

                if let Some(ttl) = ttl {
                    if now.signed_duration_since(meta.created_at) > ttl {
                        expired.push(Bytes::copy_from_slice(&key_bytes));
                    }
                }
            }

            expired
        });

        if expired_keys.is_empty() {
            return Ok(0);
        }

        let count = expired_keys.len();
        info!(count, "Cleaning up expired entries");

        // Phase 2: Delete from memory + disk (fast local operations)
        for key in &expired_keys {
            self.memory.delete(key);
            let _ = self.disk.delete(key);
        }

        // Phase 3: Delete from object storage concurrently
        // Collect futures first (releases borrows) then join them all
        let delete_futs: Vec<_> = expired_keys.iter().map(|key| {
            let object_key = Self::to_object_key(key);
            let object = self.object.clone();
            async move {
                let _ = object.delete(&object_key).await;
            }
        }).collect();
        futures::future::join_all(delete_futs).await;

        Ok(count)
    }

    /// Get engine statistics
    pub async fn stats(&self) -> EngineStats {
        let cache_stats = self.memory.stats();
        let migrations = *self.migrations_completed.read().await;

        EngineStats {
            memory_entries: cache_stats.entries,
            memory_size_bytes: cache_stats.size_bytes,
            memory_hits: cache_stats.hits,
            memory_misses: cache_stats.misses,
            disk_entries: self.disk.len(),
            disk_size_bytes: self.disk.size(),
            object_entries: 0, // Would require listing objects
            migrations_completed: migrations,
        }
    }

    /// Get disk usage percentage
    pub fn disk_usage_percent(&self) -> f64 {
        self.disk.usage_percent()
    }

    /// Check if migration should run
    pub fn should_migrate(&self) -> bool {
        self.disk_usage_percent() >= self.config.migration_threshold_percent
    }

    /// Flush disk to ensure durability
    pub fn flush(&self) -> Result<(), EngineError> {
        self.disk.flush()?;
        Ok(())
    }

    /// Convert a key to its object storage key
    fn to_object_key(key: &[u8]) -> String {
        // Use hex encoding for binary-safe keys
        hex::encode(key)
    }
}

// Add hex dependency for key encoding
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_engine() -> (TieredEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let config = EngineConfig {
            memory: CacheConfig {
                max_size_bytes: 1024,
                max_entries: Some(10),
            },
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                max_size_bytes: 10240,
                migration_age_secs: 1, // 1 second for testing
                flush_every_n_writes: 0,
            },
            object: ObjectConfig::default(),
            migration_batch_size: 10,
            migration_threshold_percent: 80.0,
            ttl_rules: vec![],
        };

        let engine = TieredEngine::with_config(config).unwrap();
        (engine, temp_dir)
    }

    // ==================== BASIC OPERATIONS ====================

    #[tokio::test]
    async fn test_put_and_get() {
        let (engine, _temp) = create_test_engine();

        let key = b"key1";
        let value = Bytes::from("value1");

        engine.put(key, value.clone()).await.unwrap();

        let result = engine.get(key).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let (engine, _temp) = create_test_engine();

        let result = engine.get(b"nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let (engine, _temp) = create_test_engine();

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        assert!(engine.contains(b"key1").await.unwrap());

        let deleted = engine.delete(b"key1").await.unwrap();
        assert!(deleted);
        assert!(!engine.contains(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_returns_false() {
        let (engine, _temp) = create_test_engine();

        let deleted = engine.delete(b"nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_contains() {
        let (engine, _temp) = create_test_engine();

        assert!(!engine.contains(b"key1").await.unwrap());

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        assert!(engine.contains(b"key1").await.unwrap());
    }

    // ==================== TIERED ACCESS ====================

    #[tokio::test]
    async fn test_get_from_cache() {
        let (engine, _temp) = create_test_engine();

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();

        let result = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result.tier, StorageTier::Memory);
    }

    #[tokio::test]
    async fn test_get_from_disk_promotes_to_cache() {
        let (engine, _temp) = create_test_engine();

        // Write directly to disk (simulating cache eviction)
        engine.disk.put(b"key1", Bytes::from("value1")).unwrap();

        // First get should come from disk
        let result1 = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result1.tier, StorageTier::Disk);
        assert_eq!(result1.value, Bytes::from("value1"));

        // Second get should come from cache (promoted)
        let result2 = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result2.tier, StorageTier::Memory);
    }

    #[tokio::test]
    async fn test_get_from_object_storage_promotes_to_cache() {
        let (engine, _temp) = create_test_engine();

        // Write directly to object storage
        let object_key = TieredEngine::to_object_key(b"key1");
        engine.object.put(&object_key, Bytes::from("value1")).await.unwrap();

        // First get should come from object storage
        let result1 = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result1.tier, StorageTier::Object);
        assert_eq!(result1.value, Bytes::from("value1"));

        // Second get should come from cache (promoted)
        let result2 = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result2.tier, StorageTier::Memory);
    }

    // ==================== MIGRATION ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migration_moves_old_data_to_object_storage() {
        let (engine, _temp) = create_test_engine();

        // Add entries
        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        engine.put(b"key2", Bytes::from("value2")).await.unwrap();

        // Clear cache to force disk reads
        engine.memory.clear();

        // Wait for entries to age past migration threshold
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Run migration
        let migrated = engine.run_migration().await.unwrap();
        assert_eq!(migrated, 2);

        // Entries should no longer be on disk
        assert!(engine.disk.get(b"key1").unwrap().is_none());
        assert!(engine.disk.get(b"key2").unwrap().is_none());

        // But should be accessible via the engine (from object storage)
        let result = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result.value, Bytes::from("value1"));
        assert_eq!(result.tier, StorageTier::Object);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migration_returns_zero_when_nothing_to_migrate() {
        let (engine, _temp) = create_test_engine();

        // No entries, nothing to migrate
        let migrated = engine.run_migration().await.unwrap();
        assert_eq!(migrated, 0);
    }

    // ==================== STATISTICS ====================

    #[tokio::test]
    async fn test_stats() {
        let (engine, _temp) = create_test_engine();

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        engine.put(b"key2", Bytes::from("value2")).await.unwrap();

        // Trigger some cache hits/misses
        engine.get(b"key1").await.unwrap();
        engine.get(b"nonexistent").await.unwrap();

        let stats = engine.stats().await;

        assert_eq!(stats.memory_entries, 2);
        assert_eq!(stats.disk_entries, 2);
        assert!(stats.memory_hits >= 1);
        assert!(stats.memory_misses >= 1);
    }

    #[tokio::test]
    async fn test_disk_usage_percent() {
        let (engine, _temp) = create_test_engine();

        // Add some data and flush to SST
        engine.put(b"key1", Bytes::from("x".repeat(1000))).await.unwrap();
        engine.flush().unwrap();

        // Should now have some usage
        assert!(engine.disk_usage_percent() > 0.0);
    }

    // ==================== DURABILITY ====================

    #[tokio::test]
    async fn test_data_persists_after_flush() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        // Write data and flush
        {
            let config = EngineConfig {
                disk: DiskConfig {
                    data_dir: path.clone(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let engine = TieredEngine::with_config(config).unwrap();

            engine.put(b"key1", Bytes::from("value1")).await.unwrap();
            engine.flush().unwrap();
        }

        // Reopen and verify
        {
            let config = EngineConfig {
                disk: DiskConfig {
                    data_dir: path,
                    ..Default::default()
                },
                ..Default::default()
            };
            let engine = TieredEngine::with_config(config).unwrap();

            // Should come from disk (cache is empty on new engine)
            let result = engine.get(b"key1").await.unwrap().unwrap();
            assert_eq!(result.value, Bytes::from("value1"));
            assert_eq!(result.tier, StorageTier::Disk);
        }
    }

    // ==================== DELETE FROM ALL TIERS ====================

    #[tokio::test]
    async fn test_delete_removes_from_all_tiers() {
        let (engine, _temp) = create_test_engine();

        // Put key (goes to cache + disk)
        engine.put(b"key1", Bytes::from("value1")).await.unwrap();

        // Manually add to object storage too
        let object_key = TieredEngine::to_object_key(b"key1");
        engine.object.put(&object_key, Bytes::from("value1")).await.unwrap();

        // Verify exists in all tiers
        assert!(engine.memory.contains(b"key1"));
        assert!(engine.disk.contains(b"key1").unwrap());
        assert!(engine.object.exists(&object_key).await.unwrap());

        // Delete
        engine.delete(b"key1").await.unwrap();

        // Verify removed from all tiers
        assert!(!engine.memory.contains(b"key1"));
        assert!(!engine.disk.contains(b"key1").unwrap());
        assert!(!engine.object.exists(&object_key).await.unwrap());
    }
}
