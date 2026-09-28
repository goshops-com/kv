//! Tiered Storage Engine
//!
//! Combines memory cache, disk storage, and object storage into a unified
//! key-value store with automatic data tiering.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use std::cell::RefCell;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::disk::{BackfillState, DiskConfig, DiskError, DiskStore, StoredEntryMeta};
use crate::metrics;
use crate::memory::{CacheConfig, CacheStats, MemoryCache};
use crate::object::{ObjectConfig, ObjectError, ObjectMetadata, ObjectStore};

/// zstd frame magic number (little-endian)
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Largest frame content size we trust enough to preallocate for in one go.
const MAX_PREALLOC_DECOMPRESSED: u64 = 256 * 1024 * 1024;

thread_local! {
    // One zstd context per blocking-pool thread, reused across calls. Creating a
    // fresh context per value (what `encode_all`/`decode_all` do) showed up as
    // ZSTD_createDCtx in CPU profiles.
    static ZSTD_COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
    // Worst-case-sized scratch output for the compressor, reused across calls
    static ZSTD_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static ZSTD_DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

fn zstd_err(e: std::io::Error) -> EngineError {
    EngineError::Disk(DiskError::Serialization(e.to_string()))
}

/// Compress a value with zstd (level 1 = fast). The frame records its content
/// size, so decompression can allocate the output exactly once.
///
/// The result is an exact-size allocation. `bulk::Compressor::compress` returns a
/// Vec with worst-case capacity (about the *uncompressed* size), and wrapping that
/// in `Bytes` keeps the whole capacity alive: every value held by the L1 cache
/// then pinned ~4.7x the bytes the cache accounted for (search-kv grew to 5Gi of
/// anon with a "1GB" cache).
fn compress_to_vec(data: &[u8]) -> Result<Vec<u8>, EngineError> {
    ZSTD_COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd::bulk::Compressor::new(1).map_err(zstd_err)?);
        }
        ZSTD_SCRATCH.with(|scratch| {
            let mut buf = scratch.borrow_mut();
            buf.clear();
            buf.reserve(zstd::zstd_safe::compress_bound(data.len()));
            slot.as_mut().unwrap().compress_to_buffer(data, &mut *buf).map_err(zstd_err)?;
            let out = buf.as_slice().to_vec();
            // Don't let one huge value pin a huge scratch buffer on this thread
            if buf.capacity() > 4 * 1024 * 1024 {
                *buf = Vec::new();
            }
            Ok(out)
        })
    })
}

fn compress_value(data: &[u8]) -> Result<Bytes, EngineError> {
    Ok(Bytes::from(compress_to_vec(data)?))
}

/// Decompress if the value starts with zstd magic, otherwise return as-is.
/// Safe because JSON values start with '{'/'"'/etc (0x7B/0x22), never 0x28.
fn decompress_if_needed(data: &Bytes) -> Result<Bytes, EngineError> {
    if data.len() < 4 || data[..4] != ZSTD_MAGIC {
        return Ok(data.clone());
    }
    match zstd::zstd_safe::get_frame_content_size(data) {
        Ok(Some(size)) if size <= MAX_PREALLOC_DECOMPRESSED => ZSTD_DECOMPRESSOR.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(zstd::bulk::Decompressor::new().map_err(zstd_err)?);
            }
            let out = slot.as_mut().unwrap().decompress(data, size as usize).map_err(zstd_err)?;
            Ok(Bytes::from(out))
        }),
        // Values written by the streaming encoder don't record their size
        _ => Ok(Bytes::from(zstd::decode_all(data.as_ref()).map_err(zstd_err)?)),
    }
}

/// Decompress off the async runtime. A flood of gets doing inline zstd
/// (`decompress_if_needed`) was CPU-starving the tokio worker threads, so even the
/// trivial static `/health` task couldn't be scheduled — the liveness probe then
/// killed the pod (2026-07-06 search-kv flapping). `spawn_blocking` moves the
/// CPU-bound work to the blocking pool, keeping the async runtime responsive.
async fn decompress_async(data: Bytes) -> Result<Bytes, EngineError> {
    tokio::task::spawn_blocking(move || decompress_if_needed(&data))
        .await
        .map_err(|e| EngineError::Disk(DiskError::Serialization(format!("decompress join error: {e}"))))?
}

/// Compress off the async runtime (same reason as `decompress_async`). Values can
/// be large (287KB JSON → ~30KB), so inline zstd compression on writes — e.g. the
/// cache-repopulation write flood after a cold restart — starved the runtime too.
async fn compress_async(data: Bytes) -> Result<Bytes, EngineError> {
    tokio::task::spawn_blocking(move || compress_value(&data))
        .await
        .map_err(|e| EngineError::Disk(DiskError::Serialization(format!("compress join error: {e}"))))?
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
    /// Cache-only mode: disable migration to object storage
    pub cache_only: bool,
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
            cache_only: false,
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

fn now_secs() -> u64 {
    Utc::now().timestamp().max(0) as u64
}

fn add_secs(t: &DateTime<Utc>, secs: u64) -> u64 {
    (t.timestamp().max(0) as u64).saturating_add(secs)
}

/// Fingerprint of a TTL rule set. Stored with the expiry index so a rule change
/// triggers a rebuild of the indexed expiries.
fn rules_fingerprint(rules: &[TtlRule]) -> u64 {
    let mut desc: Vec<String> = rules.iter().map(|r| format!("{}\0{}", r.prefix, r.ttl_secs)).collect();
    desc.sort();
    xxhash_rust::xxh3::xxh3_64(desc.join("\n").as_bytes())
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

    /// Effective expiry of an entry, in unix seconds (0 = never): the earlier of
    /// its per-key TTL (counted from its last write) and the first matching
    /// prefix rule (counted from creation).
    fn expires_at(&self, key: &[u8], created_at: &DateTime<Utc>, updated_at: &DateTime<Utc>, ttl_secs: Option<u64>) -> u64 {
        let per_key = ttl_secs.map(|t| add_secs(updated_at, t));
        let by_rule = self
            .config
            .ttl_rules
            .iter()
            .find(|r| key.starts_with(r.prefix.as_bytes()))
            .map(|r| add_secs(created_at, r.ttl_secs));
        match (per_key, by_rule) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => 0,
        }
    }

    fn expiry_of_meta(&self, key: &[u8], meta: &StoredEntryMeta) -> u64 {
        self.expires_at(key, &meta.created_at, &meta.updated_at, meta.ttl_secs)
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
            metrics::GET_RESULTS.with_label_values(&["memory"]).inc();
            let value = decompress_async(cached).await?;
            return Ok(Some(EntryInfo {
                value,
                created_at: Utc::now(), // Approximate
                tier: StorageTier::Memory,
            }));
        }

        // L2: Check disk (values may be compressed or legacy uncompressed).
        // RocksDB get is blocking IO; run it off the async runtime so disk
        // contention can't starve the worker threads (would hang /health →
        // liveness kill loop; see the flapping notes on compress/decompress above).
        if let Some(entry) = tokio::task::block_in_place(|| self.disk.get(key))? {
            // An entry past its expiry is dead even if the sweep hasn't removed it yet
            let expires_at = self.expires_at(key, &entry.created_at, &entry.updated_at, entry.ttl_secs);
            if expires_at != 0 && now_secs() >= expires_at {
                debug!(key = %key_str, "Disk hit on expired entry");
                metrics::GET_RESULTS.with_label_values(&["expired"]).inc();
                return Ok(None);
            }
            debug!(key = %key_str, "Disk hit, promoting to cache");
            metrics::GET_RESULTS.with_label_values(&["disk"]).inc();
            // Promote raw (possibly compressed) value to cache
            self.memory.put_with_expiry(key, entry.value.clone(), expires_at);
            let value = decompress_async(entry.value).await?;
            return Ok(Some(EntryInfo {
                value,
                created_at: entry.created_at,
                tier: StorageTier::Disk,
            }));
        }

        // L3: Check object storage (never populated in cache-only mode)
        if self.config.cache_only {
            metrics::GET_RESULTS.with_label_values(&["miss"]).inc();
            return Ok(None);
        }
        let object_key = Self::to_object_key(key);
        match self.object.get(&object_key).await {
            Ok(entry) => {
                debug!(key = %key_str, "Object storage hit, promoting to cache");
                metrics::GET_RESULTS.with_label_values(&["object"]).inc();
                let value = entry.value.clone();
                // Promote to cache (not disk, as it was migrated from disk)
                self.memory.put(key, value.clone());
                let value = decompress_async(value).await?;
                Ok(Some(EntryInfo {
                    value,
                    created_at: entry.metadata.created_at,
                    tier: StorageTier::Object,
                }))
            }
            Err(ObjectError::NotFound(_)) => {
                debug!(key = %key_str, "Key not found in any tier");
                metrics::GET_RESULTS.with_label_values(&["miss"]).inc();
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

        // Compress value before storing (JSON 287KB → ~30KB with zstd), off the
        // async runtime so a write flood can't CPU-starve it (see compress_async).
        let compressed = compress_async(value).await?;

        // Write compressed to disk (durability). Blocking RocksDB write off the
        // async runtime (same reason as the get path).
        let now = Utc::now();
        let expires_at = self.expires_at(key, &now, &now, ttl_secs);
        tokio::task::block_in_place(|| self.disk.put_with_ttl(key, &compressed, ttl_secs, expires_at))?;

        // Cache compressed value (10x more entries fit in cache)
        let evicted = self.memory.put_with_expiry(key, compressed, expires_at);

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

        // Remove from disk (blocking RocksDB, off the async runtime)
        if tokio::task::block_in_place(|| self.disk.delete(key))? {
            deleted = true;
        }

        // Remove from object storage
        if !self.config.cache_only {
            let object_key = Self::to_object_key(key);
            if self.object.exists(&object_key).await? {
                self.object.delete(&object_key).await?;
                deleted = true;
            }
        }

        Ok(deleted)
    }

    /// Check if a key exists in any tier
    pub async fn contains(&self, key: &[u8]) -> Result<bool, EngineError> {
        // Check memory
        if self.memory.contains(key) {
            return Ok(true);
        }

        // Check disk (blocking RocksDB, off the async runtime)
        if tokio::task::block_in_place(|| self.disk.contains_live(key, now_secs()))? {
            return Ok(true);
        }

        // Check object storage
        if self.config.cache_only {
            return Ok(false);
        }
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
        let removed = tokio::task::block_in_place(|| self.disk.remove_batch(&migrated_keys))?;

        // Update stats
        {
            let mut completed = self.migrations_completed.write().await;
            *completed += removed as u64;
        }

        info!(count = removed, "Migration batch completed");

        Ok(removed)
    }

    /// Run TTL cleanup: delete expired entries from all tiers.
    ///
    /// Walks only the due front of the expiry index (`expires_at <= now`), in
    /// batches, so the cost is proportional to what expired, not to the size of
    /// the store. Entries that predate the index are picked up once the backfill
    /// (`run_expiry_backfill`) has indexed them; until then reads already treat
    /// them as absent once expired.
    pub async fn run_ttl_cleanup(&self) -> Result<usize, EngineError> {
        const BATCH: usize = 1000;
        let started = std::time::Instant::now();
        let now = now_secs();
        let mut total = 0;

        loop {
            let expired = tokio::task::block_in_place(|| self.disk.expire_batch(now, BATCH))?;
            let n = expired.len();
            for key in &expired {
                self.memory.delete(key);
            }
            if !self.config.cache_only {
                futures::stream::iter(expired.into_iter().map(|key| {
                    let object_key = Self::to_object_key(&key);
                    let object = self.object.clone();
                    async move {
                        let _ = object.delete(&object_key).await;
                    }
                }))
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;
            }
            total += n;
            if n < BATCH {
                break;
            }
            tokio::task::yield_now().await;
        }

        if total >= 10_000 {
            tokio::task::block_in_place(|| self.disk.compact_expiry_index(now));
        }
        metrics::TTL_SWEEP_SECONDS.observe(started.elapsed().as_secs_f64());
        metrics::TTL_EXPIRED.inc_by(total as u64);
        if total > 0 {
            debug!(count = total, "TTL sweep removed expired entries");
        }
        Ok(total)
    }

    /// Build the expiry index for entries written before it existed, or rebuild
    /// it after the TTL rules changed. Resumable: progress is persisted after every
    /// batch. This is the one pass that reads every value, so it is paced.
    pub async fn run_expiry_backfill(&self) -> Result<(), EngineError> {
        const BATCH: usize = 2000;
        let fp = rules_fingerprint(&self.config.ttl_rules);
        let mut state = tokio::task::block_in_place(|| self.disk.backfill_state())?;
        if state.meta_complete && state.rules_fp == Some(fp) {
            metrics::BACKFILL_COMPLETE.set(1);
            return Ok(());
        }
        if state.pass_fp != Some(fp) {
            state.pass_fp = Some(fp);
            state.cursor = None;
        }
        info!(
            resume = state.cursor.is_some(),
            meta_complete = state.meta_complete,
            "Expiry index backfill starting"
        );
        let started = std::time::Instant::now();
        let mut visited: u64 = 0;
        loop {
            let cursor = state.cursor.clone();
            let (n, next) = tokio::task::block_in_place(|| {
                self.disk.backfill_batch(cursor.as_deref(), BATCH, |k, m| self.expiry_of_meta(k, m))
            })?;
            visited += n as u64;
            metrics::BACKFILL_VISITED.inc_by(n as u64);
            match next {
                Some(k) => {
                    state.cursor = Some(k);
                    tokio::task::block_in_place(|| self.disk.save_backfill_state(&state))?;
                    if visited % 100_000 < BATCH as u64 {
                        info!(visited, "Expiry index backfill progress");
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                }
                None => break,
            }
        }
        let done = BackfillState { meta_complete: true, rules_fp: Some(fp), pass_fp: None, cursor: None };
        tokio::task::block_in_place(|| self.disk.save_backfill_state(&done))?;
        metrics::BACKFILL_COMPLETE.set(1);
        info!(visited, secs = started.elapsed().as_secs(), "Expiry index backfill complete");
        Ok(())
    }

    /// Re-serialize legacy JSON entries to MessagePack format
    pub fn reserialize_legacy_batch(&self, batch_size: usize) -> Result<usize, EngineError> {
        Ok(self.disk.reserialize_legacy_batch(batch_size)?)
    }

    /// Delete keys this shard no longer owns — orphans left behind by a resharding
    /// event. When the ring changed (e.g. 1 shard -> 2), keys written under the old
    /// ring stay physically here but are never read again, because the proxy now
    /// routes them to their new owner. They are pure dead weight on disk (and in
    /// RocksDB index/filter RAM). `owns(key)` must use the SAME consistent-hash ring
    /// as the proxy, so we only ever drop keys the proxy would never send here.
    /// `dry_run` counts without deleting. Deletes are batched with a yield between
    /// batches so a large sweep neither starves serving nor floods RocksDB with
    /// tombstones at once. Returns (scanned, unowned, deleted).
    pub async fn scrub_unowned<F: Fn(&[u8]) -> bool>(
        &self,
        owns: F,
        dry_run: bool,
    ) -> Result<(u64, u64, u64), EngineError> {
        // Snapshot the non-owned keys first; don't mutate the DB mid-iteration.
        let (scanned, unowned) = tokio::task::block_in_place(|| {
            let mut unowned: Vec<Bytes> = Vec::new();
            let mut scanned: u64 = 0;
            for key in self.disk.keys() {
                scanned += 1;
                if !owns(&key) {
                    unowned.push(key);
                }
            }
            (scanned, unowned)
        });
        let unowned_count = unowned.len() as u64;
        if dry_run {
            return Ok((scanned, unowned_count, 0));
        }
        let mut deleted: u64 = 0;
        for batch in unowned.chunks(500) {
            for key in batch {
                if self.delete(key).await? {
                    deleted += 1;
                }
            }
            tokio::task::yield_now().await;
        }
        Ok((scanned, unowned_count, deleted))
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

    /// L1 cache statistics (for metrics)
    pub fn memory_stats(&self) -> CacheStats {
        self.memory.stats()
    }

    /// Read an integer RocksDB property (for metrics)
    pub fn disk_property(&self, name: &str) -> Option<u64> {
        self.disk.property_int(name)
    }

    /// Get disk usage percentage
    pub fn disk_usage_percent(&self) -> f64 {
        self.disk.usage_percent()
    }

    /// Check if migration should run
    pub fn should_migrate(&self) -> bool {
        !self.config.cache_only && self.disk_usage_percent() >= self.config.migration_threshold_percent
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
            cache_only: false,
            ttl_rules: vec![],
        };

        let engine = TieredEngine::with_config(config).unwrap();
        (engine, temp_dir)
    }

    // ==================== BASIC OPERATIONS ====================

    // Every test here drives TieredEngine's get/put/delete/contains paths, which
    // call tokio::task::block_in_place to keep blocking RocksDB work off the async
    // runtime. block_in_place panics with "can call blocking only when running on
    // the multi-threaded runtime" under #[tokio::test]'s default current-thread
    // runtime, so these all need flavor = "multi_thread".
    #[tokio::test(flavor = "multi_thread")]
    async fn test_put_and_get() {
        let (engine, _temp) = create_test_engine();

        let key = b"key1";
        let value = Bytes::from("value1");

        engine.put(key, value.clone()).await.unwrap();

        let result = engine.get(key).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_nonexistent_returns_none() {
        let (engine, _temp) = create_test_engine();

        let result = engine.get(b"nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete() {
        let (engine, _temp) = create_test_engine();

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        assert!(engine.contains(b"key1").await.unwrap());

        let deleted = engine.delete(b"key1").await.unwrap();
        assert!(deleted);
        assert!(!engine.contains(b"key1").await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_nonexistent_returns_false() {
        let (engine, _temp) = create_test_engine();

        let deleted = engine.delete(b"nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_contains() {
        let (engine, _temp) = create_test_engine();

        assert!(!engine.contains(b"key1").await.unwrap());

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();
        assert!(engine.contains(b"key1").await.unwrap());
    }

    // ==================== TIERED ACCESS ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_from_cache() {
        let (engine, _temp) = create_test_engine();

        engine.put(b"key1", Bytes::from("value1")).await.unwrap();

        let result = engine.get(b"key1").await.unwrap().unwrap();
        assert_eq!(result.tier, StorageTier::Memory);
    }

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_usage_percent() {
        let (engine, _temp) = create_test_engine();

        // Add some data and flush to SST
        engine.put(b"key1", Bytes::from("x".repeat(1000))).await.unwrap();
        engine.flush().unwrap();

        // Should now have some usage
        assert!(engine.disk_usage_percent() > 0.0);
    }

    // ==================== DURABILITY ====================

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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

    // ==================== TTL / EXPIRY INDEX ====================

    fn engine_with(temp_dir: &TempDir, ttl_rules: Vec<TtlRule>, cache_only: bool) -> TieredEngine {
        let config = EngineConfig {
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            cache_only,
            ttl_rules,
            ..Default::default()
        };
        TieredEngine::with_config(config).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_expired_entry_not_served_before_sweep() {
        let temp = TempDir::new().unwrap();
        let engine = engine_with(&temp, vec![], false);
        engine.put_with_ttl(b"k", Bytes::from("v"), Some(0)).await.unwrap();
        assert!(engine.get(b"k").await.unwrap().is_none());
        assert!(!engine.contains(b"k").await.unwrap());
        // And the sweep removes it
        assert_eq!(engine.run_ttl_cleanup().await.unwrap(), 1);
        assert_eq!(engine.stats().await.disk_entries, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ttl_cleanup_uses_rules_and_per_key_ttl() {
        let temp = TempDir::new().unwrap();
        let rules = vec![TtlRule { prefix: "CAD/".into(), ttl_secs: 0 }];
        let engine = engine_with(&temp, rules, false);
        engine.put(b"CAD/1", Bytes::from("v")).await.unwrap();
        engine.put_with_ttl(b"tmp", Bytes::from("v"), Some(0)).await.unwrap();
        engine.put_with_ttl(b"keep", Bytes::from("v"), Some(3600)).await.unwrap();
        engine.put(b"forever", Bytes::from("v")).await.unwrap();

        assert_eq!(engine.run_ttl_cleanup().await.unwrap(), 2);
        assert!(engine.get(b"CAD/1").await.unwrap().is_none());
        assert!(engine.get(b"tmp").await.unwrap().is_none());
        assert!(engine.get(b"keep").await.unwrap().is_some());
        assert!(engine.get(b"forever").await.unwrap().is_some());
        assert_eq!(engine.run_ttl_cleanup().await.unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rule_change_rebuilds_expiry_index() {
        let temp = TempDir::new().unwrap();
        {
            let engine = engine_with(&temp, vec![], false);
            engine.run_expiry_backfill().await.unwrap();
            engine.put(b"CAD/1", Bytes::from("v")).await.unwrap();
            engine.put(b"other", Bytes::from("v")).await.unwrap();
        }
        // Restart with a rule that makes CAD/ keys already expired
        let rules = vec![TtlRule { prefix: "CAD/".into(), ttl_secs: 0 }];
        let engine = engine_with(&temp, rules, false);
        // Reads honour the new rule immediately...
        assert!(engine.get(b"CAD/1").await.unwrap().is_none());
        // ...and once the index is rebuilt the sweep deletes it
        engine.run_expiry_backfill().await.unwrap();
        assert_eq!(engine.run_ttl_cleanup().await.unwrap(), 1);
        assert!(engine.get(b"other").await.unwrap().is_some());
        // Rebuild is recorded: a second run is a no-op
        engine.run_expiry_backfill().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cache_only_skips_object_tier() {
        let temp = TempDir::new().unwrap();
        let engine = engine_with(&temp, vec![], true);
        engine
            .object
            .put(&TieredEngine::to_object_key(b"remote"), Bytes::from("v"))
            .await
            .unwrap();
        assert!(engine.get(b"remote").await.unwrap().is_none());
        assert!(!engine.contains(b"remote").await.unwrap());
    }

    #[test]
    fn test_compressed_values_are_exact_size_allocations() {
        let data = "{\"a\":1,\"b\":\"xyz\"}".repeat(10_000);
        for _ in 0..3 {
            let v = compress_to_vec(data.as_bytes()).unwrap();
            assert_eq!(v.capacity(), v.len());
            assert!(v.len() < data.len() / 10);
        }
    }

    #[test]
    fn test_zstd_roundtrip_new_and_streaming_frames() {
        let data = "{\"a\":1}".repeat(10_000);
        let compressed = compress_value(data.as_bytes()).unwrap();
        assert!(compressed.len() < data.len() / 10);
        assert_eq!(decompress_if_needed(&compressed).unwrap(), Bytes::from(data.clone()));
        // Frames from the old streaming encoder carry no content size
        let legacy = Bytes::from(zstd::encode_all(data.as_bytes(), 1).unwrap());
        assert_eq!(decompress_if_needed(&legacy).unwrap(), Bytes::from(data.clone()));
        // Uncompressed values pass through
        let raw = Bytes::from_static(b"{\"plain\":true}");
        assert_eq!(decompress_if_needed(&raw).unwrap(), raw);
    }
}
