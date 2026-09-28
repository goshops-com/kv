//! Disk Store Layer (L2)
//!
//! Persistent local storage using RocksDB.
//!
//! Column families:
//! - `default`: key -> serialized entry (value + timestamps + per-key TTL). Values
//!   above 4KB live in BlobDB blob files.
//! - `meta`:    key -> `EntryMeta` (expiry + write version), one per live key. Tiny,
//!   so existence checks and TTL bookkeeping never have to load a value blob.
//! - `expiry`:  `[expires_at u64 BE][key]` -> empty, only for keys that expire.
//!   Ordered by expiry time, so a TTL sweep is a range scan over exactly the keys
//!   that are due, instead of a full scan that reads every value.
//! - `sys`:     internal state (expiry-index backfill progress).
//!
//! Writes to all column families for one key go in a single atomic WriteBatch
//! under a per-key stripe lock, so `meta`/`expiry` never disagree with `default`.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Error, Debug)]
pub enum DiskError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Key not found")]
    NotFound,
}

const CF_META: &str = "meta";
const CF_EXPIRY: &str = "expiry";
const CF_SYS: &str = "sys";
const SYS_BACKFILL_KEY: &[u8] = b"expiry_backfill";

/// Number of per-key lock stripes serializing read-modify-write of one key's
/// `default`/`meta`/`expiry` records.
const STRIPES: usize = 1024;

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
            flush_every_n_writes: 0,
        }
    }
}

/// Stored entry with metadata, as encoded by format v1 (and legacy JSON).
///
/// v1 serializes `value` as a msgpack *array of integers* (one element per byte,
/// 2 bytes each for bytes >= 0x80), which inflates already-compressed values ~1.5x
/// and makes every decode a per-byte loop. Kept only to read v1 entries and to
/// write them back out for a downgrade; new writes use v2 (`StoredEntryV2Ref`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEntry {
    pub value: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

/// Format v2 on the write side: same field layout as v1, but `value` is a msgpack
/// `bin`, written straight from the caller's slice.
#[derive(Serialize)]
struct StoredEntryV2Ref<'a> {
    #[serde(with = "serde_bytes")]
    value: &'a [u8],
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    ttl_secs: Option<u64>,
}

/// Format v2 on the read side: `value` borrows from the RocksDB buffer.
#[derive(Deserialize)]
struct StoredEntryV2<'a> {
    #[serde(borrow, with = "serde_bytes")]
    value: &'a [u8],
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

/// Metadata-only view of an entry (any format); skips the value field.
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
    #[cfg(test)]
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
}

/// Public entry returned from the disk store
#[derive(Debug, Clone)]
pub struct DiskEntry {
    pub value: Bytes,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ttl_secs: Option<u64>,
    pub size_bytes: usize,
}

impl From<StoredEntry> for DiskEntry {
    fn from(stored: StoredEntry) -> Self {
        let size_bytes = stored.value.len();
        Self {
            value: Bytes::from(stored.value),
            created_at: stored.created_at,
            updated_at: stored.updated_at,
            ttl_secs: stored.ttl_secs,
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

/// Progress of the one-time pass that builds `meta`/`expiry` for entries written
/// before the index existed (and rebuilds expiries when the TTL rules change).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackfillState {
    /// Every key in `default` has a `meta` record (existence checks can trust `meta`).
    pub meta_complete: bool,
    /// Fingerprint of the TTL rules the last completed pass computed expiries with.
    pub rules_fp: Option<u64>,
    /// Fingerprint of the pass in progress, if any, and where it stopped.
    pub pass_fp: Option<u64>,
    pub cursor: Option<Vec<u8>>,
}

/// Version byte for MessagePack v1 (JSON has no prefix, starts with '{')
const MSGPACK_VERSION: u8 = 0x01;
/// Version byte for MessagePack v2 (`value` as msgpack bin)
const MSGPACK_V2: u8 = 0x02;

/// Serialize an entry in the current format (v2)
fn encode_entry(
    value: &[u8],
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    ttl_secs: Option<u64>,
) -> Result<Vec<u8>, DiskError> {
    let mut buf = Vec::with_capacity(value.len() + 80);
    buf.push(MSGPACK_V2);
    rmp_serde::encode::write(
        &mut buf,
        &StoredEntryV2Ref { value, created_at, updated_at, ttl_secs },
    )
    .map_err(|e| DiskError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Serialize an entry in format v1 (only used to downgrade a data dir)
fn encode_entry_v1(entry: &StoredEntry) -> Result<Vec<u8>, DiskError> {
    let msgpack = rmp_serde::to_vec(entry)
        .map_err(|e| DiskError::Serialization(e.to_string()))?;
    let mut buf = Vec::with_capacity(1 + msgpack.len());
    buf.push(MSGPACK_VERSION);
    buf.extend(msgpack);
    Ok(buf)
}

/// Deserialize metadata only, skipping the value field
fn deserialize_entry_meta(data: &[u8]) -> Result<StoredEntryMeta, DiskError> {
    match data.first() {
        Some(&MSGPACK_V2) | Some(&MSGPACK_VERSION) => rmp_serde::from_slice(&data[1..])
            .map_err(|e| DiskError::Serialization(e.to_string())),
        _ => serde_json::from_slice(data).map_err(|e| DiskError::Serialization(e.to_string())),
    }
}

/// Deserialize an entry from any format (v2, v1, legacy JSON)
fn decode_entry(data: &[u8]) -> Result<DiskEntry, DiskError> {
    match data.first() {
        Some(&MSGPACK_V2) => {
            let e: StoredEntryV2 = rmp_serde::from_slice(&data[1..])
                .map_err(|e| DiskError::Serialization(e.to_string()))?;
            Ok(DiskEntry {
                value: Bytes::copy_from_slice(e.value),
                created_at: e.created_at,
                updated_at: e.updated_at,
                ttl_secs: e.ttl_secs,
                size_bytes: e.value.len(),
            })
        }
        Some(&MSGPACK_VERSION) => {
            let e: StoredEntry = rmp_serde::from_slice(&data[1..])
                .map_err(|e| DiskError::Serialization(e.to_string()))?;
            Ok(e.into())
        }
        _ => {
            // Legacy JSON format (starts with '{' = 0x7B)
            let e: StoredEntry = serde_json::from_slice(data)
                .map_err(|e| DiskError::Serialization(e.to_string()))?;
            Ok(e.into())
        }
    }
}

/// Per-key bookkeeping stored in the `meta` column family.
#[derive(Debug, Clone, Copy, PartialEq)]
struct EntryMeta {
    /// Unix seconds at which the entry expires (0 = never)
    expires_at: u64,
    /// `updated_at` of the entry in `default`, in microseconds. Lets the backfill
    /// tell whether the entry it read is still the current version of the key.
    version: i64,
}

const META_FORMAT: u8 = 1;

impl EntryMeta {
    fn encode(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        out[0] = META_FORMAT;
        out[1..9].copy_from_slice(&self.expires_at.to_be_bytes());
        out[9..17].copy_from_slice(&self.version.to_be_bytes());
        out
    }

    fn decode(data: &[u8]) -> Option<Self> {
        if data.len() != 17 || data[0] != META_FORMAT {
            return None;
        }
        Some(Self {
            expires_at: u64::from_be_bytes(data[1..9].try_into().ok()?),
            version: i64::from_be_bytes(data[9..17].try_into().ok()?),
        })
    }
}

fn expiry_key(expires_at: u64, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + key.len());
    k.extend_from_slice(&expires_at.to_be_bytes());
    k.extend_from_slice(key);
    k
}

fn version_of(updated_at: &DateTime<Utc>) -> i64 {
    updated_at.timestamp_micros()
}

/// Disk storage layer using RocksDB
pub struct DiskStore {
    db: DB,
    config: DiskConfig,
    write_count: AtomicU64,
    entry_count: AtomicUsize,
    /// Mirrors `BackfillState::meta_complete`. Until it is set, a missing `meta`
    /// record does not prove a key is absent, so existence checks fall back to
    /// reading `default`.
    meta_complete: AtomicBool,
    /// Per-key lock stripes. The u64 is a mutation counter bumped on every write
    /// to a key in the stripe, which lets the backfill detect concurrent changes.
    stripes: Box<[Mutex<u64>]>,
}

fn env_mb(name: &str, default_mb: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default_mb)
        * 1024
        * 1024
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .unwrap_or(default)
}

fn env_compression(name: &str, default: rocksdb::DBCompressionType) -> rocksdb::DBCompressionType {
    match std::env::var(name).ok().as_deref() {
        Some("none") => rocksdb::DBCompressionType::None,
        Some("zstd") => rocksdb::DBCompressionType::Zstd,
        _ => default,
    }
}

/// Options for the whole DB plus the `default` column family.
/// Returns the options and the block cache shared with the small column families.
fn make_opts() -> (Options, rocksdb::Cache) {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let block_cache_bytes = env_mb("ROCKSDB_BLOCK_CACHE_MB", 256);
    let cache = rocksdb::Cache::new_lru_cache(block_cache_bytes);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    block_opts.set_block_size(16 * 1024);
    // Point lookups for absent keys (cache misses, first PUT of a key) otherwise
    // probe every L0 file and one file per level. ~10 bits/key.
    block_opts.set_bloom_filter(10.0, false);

    // Bound index/filter-block RAM. By default RocksDB keeps each SST's index and
    // filter (bloom) blocks pinned in memory OUTSIDE the block cache, so anon grows
    // with the on-disk dataset (SST count) — on a large shard this crossed the
    // cgroup limit and OOM-killed the pod. Routing those blocks INTO the bounded
    // LRU block cache (and pinning only L0's) caps total block memory at the cache
    // size regardless of how big the disk grows. Opt-in so existing deployments are
    // unchanged unless they set ROCKSDB_CACHE_INDEX_AND_FILTER=true.
    let cache_index_and_filter = cache_index_and_filter();
    if cache_index_and_filter {
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    }
    opts.set_block_based_table_factory(&block_opts);

    // Cap how many SST file readers stay open. -1 (default) keeps every file open,
    // pinning its table metadata in RAM — unbounded as the dataset grows. A finite
    // cap lets RocksDB evict cold readers (and their metadata) under the LRU above.
    let max_open_files = std::env::var("ROCKSDB_MAX_OPEN_FILES")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(-1);
    opts.set_max_open_files(max_open_files);

    opts.set_write_buffer_size(env_mb("ROCKSDB_WRITE_BUFFER_MB", 64));
    let max_wb = std::env::var("ROCKSDB_MAX_WRITE_BUFFERS")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(3);
    opts.set_max_write_buffer_number(max_wb);

    // Hard-cap total memory across all write buffers + memtables.
    // Without this, write bursts grow memtables unbounded until cgroup OOM.
    opts.set_db_write_buffer_size(env_mb("ROCKSDB_DB_WRITE_BUFFER_MB", 256));

    // L0 compaction triggers: tolerate more L0 files before compacting
    opts.set_level_zero_file_num_compaction_trigger(8);  // default 4
    opts.set_level_zero_slowdown_writes_trigger(20);     // default 20
    opts.set_level_zero_stop_writes_trigger(36);         // default 36

    // Larger L1 target = less write amplification across levels
    opts.set_max_bytes_for_level_base(512 * 1024 * 1024); // 512MB (default 256MB)

    // Compaction/flush concurrency. Hardcoded 2 starved compaction under sustained
    // writes: L0 files piled up faster than 2 threads could compact, hitting the
    // L0 slowdown(20)/stop(36) triggers → multi-second write stalls (PUTs returning
    // "context deadline exceeded" under burst). Make it tunable via env. Default
    // stays 2 so existing deployments are unchanged; raise per-deployment in the
    // configmap (search-kv sets 6) where the write load needs it.
    let max_bg_jobs = std::env::var("ROCKSDB_MAX_BACKGROUND_JOBS")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2);
    opts.set_max_background_jobs(max_bg_jobs);

    // BlobDB: separate large values (>4KB) into blob files.
    // Only small key pointers stay in the LSM tree, which keeps compaction from
    // rewriting the values on every level.
    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(4096); // values > 4KB go to blob files
    opts.set_blob_file_size(256 * 1024 * 1024); // 256MB blob files
    // The engine zstd-compresses every value before it gets here, so compressing
    // the blob again burns CPU on both paths for no size win. Existing zstd blob
    // files stay readable (compression is recorded per blob file).
    opts.set_blob_compression_type(env_compression(
        "ROCKSDB_BLOB_COMPRESSION",
        rocksdb::DBCompressionType::None,
    ));
    opts.set_enable_blob_gc(true);

    // age_cutoff does NOT mean "collect a file once 25% of it is garbage" -- it
    // means "on every compaction, relocate the live blobs out of the oldest 25%
    // of blob files", however little garbage those files actually hold. At 0.25
    // (the RocksDB default) that rewrote essentially the whole value set every
    // few hours. Measured on search-kv-1 over 54h of uptime: 55 GB ingested,
    // 1078 GB of blob read and 1070 GB of blob written, 42h of compaction time,
    // W-Amp 49 -- all to reclaim 0.4 GB of garbage out of 61 GB. The disk sat at
    // roughly 2x the live data because obsolete blob files pile up between
    // sweeps, and search-kv-1 hit 96% of a 100Gi volume on ~60 GB of real data.
    //
    // Relocate a much smaller slice per compaction, and use the knob that really
    // is garbage-ratio based to collect files once they are mostly dead.
    // (Even 0.05 still rotated the whole value set every ~6.5h on search-kv:
    // relocated blobs land in the newest file and the "oldest 5%" window keeps
    // moving. Set ROCKSDB_BLOB_GC_AGE_CUTOFF=0 to stop relocation entirely and
    // rely on blob files dying whole as their keys expire.)
    opts.set_blob_gc_age_cutoff(env_f64("ROCKSDB_BLOB_GC_AGE_CUTOFF", 0.05));
    opts.set_blob_gc_force_threshold(env_f64("ROCKSDB_BLOB_GC_FORCE_THRESHOLD", 0.5));

    // Obsolete files are unlinked at the end of each compaction, but files that
    // fall out of scope by another path wait for a full directory sweep, which
    // defaults to every 6 hours. That is what makes free space sawtooth by tens
    // of GB. A scan over a few hundred files is cheap; do it far more often so
    // disk usage tracks the live data instead of the last six hours of churn.
    opts.set_delete_obsolete_files_period_micros(
        std::env::var("ROCKSDB_DELETE_OBSOLETE_PERIOD_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(300)
            * 1_000_000,
    );

    opts.set_max_total_wal_size(env_mb("ROCKSDB_MAX_WAL_MB", 128) as u64);
    // Limit LOG file accumulation
    opts.set_keep_log_file_num(5);
    opts.set_max_log_file_size(10 * 1024 * 1024);
    (opts, cache)
}

fn cache_index_and_filter() -> bool {
    std::env::var("ROCKSDB_CACHE_INDEX_AND_FILTER")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// Options for the small bookkeeping column families (`meta`, `expiry`, `sys`):
/// no blob files, small memtables, same block cache. (Only zstd is compiled
/// into librocksdb here, so no lz4.)
fn make_small_cf_opts(cache: &rocksdb::Cache, bloom: bool) -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(cache);
    block_opts.set_block_size(16 * 1024);
    if bloom {
        block_opts.set_bloom_filter(10.0, false);
    }
    if cache_index_and_filter() {
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    }
    opts.set_block_based_table_factory(&block_opts);
    opts
}

fn open_db(path: &str) -> Result<DB, DiskError> {
    let (opts, cache) = make_opts();
    let cfs = vec![
        ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, opts.clone()),
        ColumnFamilyDescriptor::new(CF_META, make_small_cf_opts(&cache, true)),
        // Only ever range-scanned from the front, so a bloom filter would be dead weight.
        ColumnFamilyDescriptor::new(CF_EXPIRY, make_small_cf_opts(&cache, false)),
        ColumnFamilyDescriptor::new(CF_SYS, make_small_cf_opts(&cache, false)),
    ];
    Ok(DB::open_cf_descriptors(&opts, path, cfs)?)
}

impl DiskStore {
    /// Create a new disk store
    pub fn new(config: DiskConfig) -> Result<Self, DiskError> {
        let db = open_db(&config.data_dir)?;

        // Estimate entry count from RocksDB metadata (O(1), no full-scan)
        let entry_count = db
            .property_int_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .unwrap_or(0) as usize;

        let store = Self {
            db,
            config,
            write_count: AtomicU64::new(0),
            entry_count: AtomicUsize::new(entry_count),
            meta_complete: AtomicBool::new(false),
            stripes: (0..STRIPES).map(|_| Mutex::new(0)).collect(),
        };

        let mut state = store.backfill_state()?;
        if !state.meta_complete && store.default_cf_is_empty() {
            // Fresh (or empty) DB: every future key gets its meta on write.
            state.meta_complete = true;
            store.save_backfill_state(&state)?;
        }
        store.meta_complete.store(state.meta_complete, Ordering::Release);

        Ok(store)
    }

    /// Open an existing store or create new
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DiskError> {
        let config = DiskConfig {
            data_dir: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        Self::new(config)
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).expect("column family opened at startup")
    }

    fn stripe(&self, key: &[u8]) -> &Mutex<u64> {
        &self.stripes[(xxh3_64(key) % STRIPES as u64) as usize]
    }

    fn default_cf_is_empty(&self) -> bool {
        let mut it = self.db.raw_iterator();
        it.seek_to_first();
        !it.valid()
    }

    fn get_meta(&self, key: &[u8]) -> Result<Option<EntryMeta>, DiskError> {
        Ok(self
            .db
            .get_pinned_cf(self.cf(CF_META), key)?
            .and_then(|m| EntryMeta::decode(&m)))
    }

    /// Whether `key` has an entry in `default`. Answered from `meta` once the
    /// backfill has covered every key; before that, a missing `meta` record has
    /// to be confirmed against `default` (which loads the value blob).
    fn exists_with(&self, key: &[u8], meta: &Option<EntryMeta>) -> Result<bool, DiskError> {
        if meta.is_some() {
            return Ok(true);
        }
        if self.meta_complete.load(Ordering::Acquire) || !self.db.key_may_exist(key) {
            return Ok(false);
        }
        Ok(self.db.get_pinned(key)?.is_some())
    }

    /// Get actual disk usage from RocksDB (SST files + blob files + memtables)
    fn disk_size(db: &DB) -> u64 {
        let prop = |name: &str| db.property_int_value(name).ok().flatten().unwrap_or(0);
        prop("rocksdb.total-sst-files-size")
            + prop("rocksdb.total-blob-file-size")
            + prop("rocksdb.cur-size-all-mem-tables")
    }

    /// Read an integer RocksDB property of the `default` column family (metrics)
    pub fn property_int(&self, name: &str) -> Option<u64> {
        self.db.property_int_value(name).ok().flatten()
    }

    /// Get a value from disk
    pub fn get(&self, key: &[u8]) -> Result<Option<DiskEntry>, DiskError> {
        match self.db.get_pinned(key)? {
            Some(data) => Ok(Some(decode_entry(&data)?)),
            None => Ok(None),
        }
    }

    /// Put a value to disk that never expires. Returns whether the key existed.
    pub fn put(&self, key: &[u8], value: Bytes) -> Result<bool, DiskError> {
        self.put_with_ttl(key, &value, None, 0)
    }

    /// Put a value to disk (durable write). `ttl_secs` is the per-key TTL stored
    /// with the entry; `expires_at` (unix seconds, 0 = never) is the effective
    /// expiry the caller computed from it and any prefix rules, and is what the
    /// expiry index is keyed on. Returns whether the key existed before.
    pub fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
        expires_at: u64,
    ) -> Result<bool, DiskError> {
        let now = Utc::now();
        let serialized = encode_entry(value, now, now, ttl_secs)?;
        let new_meta = EntryMeta { expires_at, version: version_of(&now) };

        let existed = {
            let mut epoch = self.stripe(key).lock();
            let old = self.get_meta(key)?;
            let existed = self.exists_with(key, &old)?;

            let mut batch = WriteBatch::default();
            batch.put(key, &serialized);
            batch.put_cf(self.cf(CF_META), key, new_meta.encode());
            if let Some(old) = old {
                if old.expires_at != 0 && old.expires_at != expires_at {
                    batch.delete_cf(self.cf(CF_EXPIRY), expiry_key(old.expires_at, key));
                }
            }
            if expires_at != 0 {
                batch.put_cf(self.cf(CF_EXPIRY), expiry_key(expires_at, key), b"");
            }
            self.db.write(batch)?;
            *epoch += 1;
            existed
        };

        if !existed {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        // Periodic flush (off by default: the WAL already makes writes durable, and
        // forcing a flush every N writes produced tiny L0/blob files and stalled the
        // unlucky writer that triggered it)
        let writes = self.write_count.fetch_add(1, Ordering::Relaxed);
        if self.config.flush_every_n_writes > 0
            && writes % self.config.flush_every_n_writes == 0
        {
            self.db.flush()?;
        }

        Ok(existed)
    }

    /// Delete a value from disk. Returns whether the key existed.
    pub fn delete(&self, key: &[u8]) -> Result<bool, DiskError> {
        let mut epoch = self.stripe(key).lock();
        let old = self.get_meta(key)?;
        if !self.exists_with(key, &old)? {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        batch.delete(key);
        batch.delete_cf(self.cf(CF_META), key);
        if let Some(old) = old {
            if old.expires_at != 0 {
                batch.delete_cf(self.cf(CF_EXPIRY), expiry_key(old.expires_at, key));
            }
        }
        self.db.write(batch)?;
        *epoch += 1;
        drop(epoch);
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool, DiskError> {
        let meta = self.get_meta(key)?;
        self.exists_with(key, &meta)
    }

    /// Check if a key exists and has not expired as of `now_secs`
    pub fn contains_live(&self, key: &[u8], now_secs: u64) -> Result<bool, DiskError> {
        let meta = self.get_meta(key)?;
        if let Some(m) = meta {
            return Ok(m.expires_at == 0 || now_secs < m.expires_at);
        }
        self.exists_with(key, &meta)
    }

    /// Delete up to `limit` entries whose indexed expiry is at or before `now_secs`.
    /// Touches only the `expiry`/`meta` column families plus one delete per key in
    /// `default`; no value is read. Returns the keys that were deleted.
    pub fn expire_batch(&self, now_secs: u64, limit: usize) -> Result<Vec<Bytes>, DiskError> {
        let mut due: Vec<(u64, Vec<u8>)> = Vec::new();
        {
            let mut ro = rocksdb::ReadOptions::default();
            ro.set_iterate_upper_bound(now_secs.saturating_add(1).to_be_bytes().to_vec());
            let mut it = self.db.raw_iterator_cf_opt(self.cf(CF_EXPIRY), ro);
            it.seek_to_first();
            while it.valid() && due.len() < limit {
                if let Some(k) = it.key() {
                    if k.len() >= 8 {
                        let exp = u64::from_be_bytes(k[..8].try_into().unwrap());
                        due.push((exp, k[8..].to_vec()));
                    }
                }
                it.next();
            }
            it.status()?;
        }

        let mut deleted = Vec::with_capacity(due.len());
        for (exp, key) in due {
            let mut epoch = self.stripe(&key).lock();
            let meta = self.get_meta(&key)?;
            let mut batch = WriteBatch::default();
            batch.delete_cf(self.cf(CF_EXPIRY), expiry_key(exp, &key));
            let live = meta.is_some_and(|m| m.expires_at == exp);
            if live {
                batch.delete(&key);
                batch.delete_cf(self.cf(CF_META), &key);
            }
            // else: stale index record (key was rewritten or deleted since)
            self.db.write(batch)?;
            if live {
                *epoch += 1;
                drop(epoch);
                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                deleted.push(Bytes::from(key));
            }
        }
        Ok(deleted)
    }

    /// Compact the already-swept front of the expiry index so the next sweep
    /// doesn't have to skip over the tombstones it left behind.
    pub fn compact_expiry_index(&self, now_secs: u64) {
        self.db.compact_range_cf(
            self.cf(CF_EXPIRY),
            None::<&[u8]>,
            Some(&now_secs.saturating_add(1).to_be_bytes()[..]),
        );
    }

    pub fn backfill_state(&self) -> Result<BackfillState, DiskError> {
        match self.db.get_cf(self.cf(CF_SYS), SYS_BACKFILL_KEY)? {
            Some(raw) => serde_json::from_slice(&raw)
                .map_err(|e| DiskError::Serialization(e.to_string())),
            None => Ok(BackfillState::default()),
        }
    }

    pub fn save_backfill_state(&self, state: &BackfillState) -> Result<(), DiskError> {
        let raw = serde_json::to_vec(state).map_err(|e| DiskError::Serialization(e.to_string()))?;
        self.db.put_cf(self.cf(CF_SYS), SYS_BACKFILL_KEY, raw)?;
        if state.meta_complete {
            self.meta_complete.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub fn meta_complete(&self) -> bool {
        self.meta_complete.load(Ordering::Acquire)
    }

    /// Build (or rebuild) the `meta`/`expiry` records for up to `limit` entries of
    /// `default`, starting after `after`. `expiry_of(key, meta)` returns the
    /// effective expiry (unix seconds, 0 = never). This is the only path that has
    /// to read every value (the key and timestamps sit next to the value in
    /// `default`), and it runs once per data dir, resumably.
    ///
    /// Returns the number of entries visited and the last key visited, or `None`
    /// once the end of `default` is reached.
    pub fn backfill_batch<F>(
        &self,
        after: Option<&[u8]>,
        limit: usize,
        expiry_of: F,
    ) -> Result<(usize, Option<Vec<u8>>), DiskError>
    where
        F: Fn(&[u8], &StoredEntryMeta) -> u64,
    {
        // Mutation counters before the iterator's implicit snapshot: if a stripe's
        // counter is unchanged when we get to a key, nothing wrote to that key
        // after the snapshot, so what the iterator read is still current.
        let epochs: Vec<u64> = self.stripes.iter().map(|s| *s.lock()).collect();

        let mut ro = rocksdb::ReadOptions::default();
        ro.fill_cache(false);
        ro.set_readahead_size(2 * 1024 * 1024);
        let mut it = self.db.raw_iterator_opt(ro);
        match after {
            Some(k) => {
                it.seek(k);
                if it.valid() && it.key() == Some(k) {
                    it.next();
                }
            }
            None => it.seek_to_first(),
        }

        let mut visited = 0;
        let mut last: Option<Vec<u8>> = None;
        while it.valid() && visited < limit {
            let (Some(key), Some(value)) = (it.key(), it.value()) else { break };
            visited += 1;
            last = Some(key.to_vec());
            let Ok(entry_meta) = deserialize_entry_meta(value) else {
                it.next();
                continue;
            };
            let expires_at = expiry_of(key, &entry_meta);
            let version = version_of(&entry_meta.updated_at);
            let stripe_idx = (xxh3_64(key) % STRIPES as u64) as usize;

            let mut epoch = self.stripes[stripe_idx].lock();
            let current = self.get_meta(key)?;
            let still_current = match current {
                Some(m) => m.version == version,
                None if *epoch == epochs[stripe_idx] => true,
                // Something wrote to this stripe since the snapshot; confirm the
                // entry we read is still the one in `default`.
                None => match self.db.get_pinned(key)? {
                    Some(data) => deserialize_entry_meta(&data)
                        .map(|m| version_of(&m.updated_at) == version)
                        .unwrap_or(false),
                    None => false,
                },
            };
            if still_current && current.map(|m| m.expires_at) != Some(expires_at) {
                let mut batch = WriteBatch::default();
                if let Some(old) = current {
                    if old.expires_at != 0 {
                        batch.delete_cf(self.cf(CF_EXPIRY), expiry_key(old.expires_at, key));
                    }
                }
                batch.put_cf(self.cf(CF_META), key, EntryMeta { expires_at, version }.encode());
                if expires_at != 0 {
                    batch.put_cf(self.cf(CF_EXPIRY), expiry_key(expires_at, key), b"");
                }
                self.db.write(batch)?;
                *epoch += 1;
            }
            drop(epoch);
            it.next();
        }
        it.status()?;

        if it.valid() {
            Ok((visited, last))
        } else {
            Ok((visited, None))
        }
    }

    /// Re-serialize legacy JSON entries to MessagePack (batch)
    pub fn reserialize_legacy_batch(&self, batch_size: usize) -> Result<usize, DiskError> {
        let mut converted = 0;
        for result in self.db.iterator(rocksdb::IteratorMode::Start) {
            if converted >= batch_size {
                break;
            }
            let (key, data) = result?;
            // Skip entries already in msgpack format
            if matches!(data.first(), Some(&MSGPACK_VERSION) | Some(&MSGPACK_V2)) {
                continue;
            }
            let entry: StoredEntry = serde_json::from_slice(&data)
                .map_err(|e| DiskError::Serialization(e.to_string()))?;
            let new_data =
                encode_entry(&entry.value, entry.created_at, entry.updated_at, entry.ttl_secs)?;
            let mut epoch = self.stripe(&key).lock();
            self.db.put(&key, &new_data)?;
            *epoch += 1;
            converted += 1;
        }
        Ok(converted)
    }

    /// Force flush to disk
    pub fn flush(&self) -> Result<(), DiskError> {
        self.db.flush()?;
        Ok(())
    }

    /// Get actual disk size (SST + blob files + memtables)
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

    /// Iterate over all keys. Walks the small `meta` column family once it covers
    /// every key; before that, `default` (which loads every value blob).
    pub fn keys(&self) -> Box<dyn Iterator<Item = Bytes> + '_> {
        let mut ro = rocksdb::ReadOptions::default();
        ro.fill_cache(false);
        let it = if self.meta_complete() {
            self.db.iterator_cf_opt(self.cf(CF_META), ro, rocksdb::IteratorMode::Start)
        } else {
            self.db.iterator_opt(rocksdb::IteratorMode::Start, ro)
        };
        Box::new(it.filter_map(|r| r.ok()).map(|(k, _)| Bytes::copy_from_slice(&k)))
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
            let stored = decode_entry(&data)?;

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
                    entry: stored,
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
            if self.delete(key)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// Convert a data dir back to what the pre-index binary can open: rewrite v2
/// entries as v1 and drop the `meta`/`expiry`/`sys` column families (the old
/// binary opens only `default` and refuses a DB with unopened column families).
/// Run with the service stopped: `tieredkv downgrade`. Returns entries rewritten.
pub fn downgrade_data_dir(path: &str) -> Result<usize, DiskError> {
    let mut db = open_db(path)?;
    let mut rewritten = 0;
    let mut pending = WriteBatch::default();
    for result in db.iterator(rocksdb::IteratorMode::Start) {
        let (key, data) = result?;
        if data.first() != Some(&MSGPACK_V2) {
            continue;
        }
        let e = decode_entry(&data)?;
        let v1 = StoredEntry {
            value: e.value.to_vec(),
            created_at: e.created_at,
            updated_at: e.updated_at,
            ttl_secs: e.ttl_secs,
        };
        pending.put(&key, encode_entry_v1(&v1)?);
        rewritten += 1;
        if pending.len() >= 1000 {
            db.write(std::mem::take(&mut pending))?;
        }
    }
    db.write(pending)?;
    db.flush()?;
    for cf in [CF_META, CF_EXPIRY, CF_SYS] {
        db.drop_cf(cf)?;
    }
    db.flush_wal(true)?;
    Ok(rewritten)
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
    fn test_put_reports_whether_key_existed() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");

        let existed1 = store.put(key, value1.clone()).unwrap();
        assert!(!existed1);

        let existed2 = store.put(key, value2.clone()).unwrap();
        assert!(existed2);

        let current = store.get(key).unwrap().unwrap();
        assert_eq!(current.value, value2);
    }

    #[test]
    fn test_delete_removes_entry() {
        let (store, _temp) = create_temp_store();

        let key = b"key1";
        store.put(key, Bytes::from("value1")).unwrap();

        let deleted = store.delete(key).unwrap();
        assert!(deleted);

        let retrieved = store.get(key).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_delete_nonexistent_returns_none() {
        let (store, _temp) = create_temp_store();

        let deleted = store.delete(b"nonexistent").unwrap();
        assert!(!deleted);
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

        // Raw bytes should start with the v2 version byte, not '{' (0x7B)
        let raw = store.db.get(b"key1").unwrap().unwrap();
        assert_eq!(raw[0], MSGPACK_V2);
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

    // ==================== FORMAT V2 ====================

    fn pseudo_random_bytes(n: usize) -> Vec<u8> {
        // High-entropy bytes, like a zstd frame: about half are >= 0x80
        let mut x: u64 = 0x9E3779B97F4A7C15;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn test_reads_v1_msgpack_entries() {
        let (store, _temp) = create_temp_store();
        let value = pseudo_random_bytes(5000);
        let v1 = StoredEntry::new_with_ttl(value.clone(), Some(60));
        store.db.put(b"v1_key", encode_entry_v1(&v1).unwrap()).unwrap();

        let entry = store.get(b"v1_key").unwrap().unwrap();
        assert_eq!(entry.value.as_ref(), value.as_slice());
        assert_eq!(entry.ttl_secs, Some(60));
        let meta = deserialize_entry_meta(&store.db.get(b"v1_key").unwrap().unwrap()).unwrap();
        assert_eq!(meta.ttl_secs, Some(60));
    }

    #[test]
    fn test_v2_stores_bytes_without_inflation() {
        let value = pseudo_random_bytes(10_000);
        let now = Utc::now();
        let v2 = encode_entry(&value, now, now, None).unwrap();
        let v1 = encode_entry_v1(&StoredEntry::new(value.clone())).unwrap();
        assert!(v2.len() < value.len() + 100, "v2 len {}", v2.len());
        assert!(v1.len() > value.len() * 14 / 10, "v1 len {}", v1.len());

        let decoded = decode_entry(&v2).unwrap();
        assert_eq!(decoded.value.as_ref(), value.as_slice());
        let meta = deserialize_entry_meta(&v2).unwrap();
        assert_eq!(meta.created_at, now);
    }

    // ==================== EXPIRY INDEX ====================

    fn now_secs() -> u64 {
        Utc::now().timestamp() as u64
    }

    #[test]
    fn test_expire_batch_deletes_only_due_keys() {
        let (store, _temp) = create_temp_store();
        let now = now_secs();
        store.put_with_ttl(b"due1", b"a", Some(1), now - 10).unwrap();
        store.put_with_ttl(b"due2", b"b", Some(1), now).unwrap();
        store.put_with_ttl(b"later", b"c", Some(100), now + 100).unwrap();
        store.put_with_ttl(b"forever", b"d", None, 0).unwrap();

        let mut deleted = store.expire_batch(now, 100).unwrap();
        deleted.sort();
        assert_eq!(deleted, vec![Bytes::from("due1"), Bytes::from("due2")]);
        assert!(store.get(b"due1").unwrap().is_none());
        assert!(!store.contains(b"due2").unwrap());
        assert!(store.get(b"later").unwrap().is_some());
        assert!(store.get(b"forever").unwrap().is_some());
        assert_eq!(store.len(), 2);

        // Nothing left to do
        assert!(store.expire_batch(now, 100).unwrap().is_empty());
        // Later, the TTL'd key goes too, never the non-expiring one
        assert_eq!(store.expire_batch(now + 1000, 100).unwrap(), vec![Bytes::from("later")]);
        assert!(store.get(b"forever").unwrap().is_some());
    }

    #[test]
    fn test_expire_batch_respects_limit() {
        let (store, _temp) = create_temp_store();
        let now = now_secs();
        for i in 0..25 {
            store.put_with_ttl(format!("k{i}").as_bytes(), b"v", Some(1), now - 1).unwrap();
        }
        assert_eq!(store.expire_batch(now, 10).unwrap().len(), 10);
        assert_eq!(store.expire_batch(now, 10).unwrap().len(), 10);
        assert_eq!(store.expire_batch(now, 10).unwrap().len(), 5);
        assert!(store.is_empty());
    }

    #[test]
    fn test_rewrite_moves_expiry() {
        let (store, _temp) = create_temp_store();
        let now = now_secs();
        store.put_with_ttl(b"k", b"old", Some(1), now - 5).unwrap();
        // Rewritten with a later expiry before the sweep ran
        store.put_with_ttl(b"k", b"new", Some(100), now + 100).unwrap();
        assert!(store.expire_batch(now, 100).unwrap().is_empty());
        assert_eq!(store.get(b"k").unwrap().unwrap().value, Bytes::from("new"));
        // Rewritten without TTL: never expires
        store.put(b"k", Bytes::from("forever")).unwrap();
        assert!(store.expire_batch(now + 1000, 100).unwrap().is_empty());
        assert_eq!(store.get(b"k").unwrap().unwrap().value, Bytes::from("forever"));
    }

    #[test]
    fn test_delete_removes_index_records() {
        let (store, _temp) = create_temp_store();
        let now = now_secs();
        store.put_with_ttl(b"k", b"v", Some(1), now - 5).unwrap();
        assert!(store.delete(b"k").unwrap());
        assert!(store.expire_batch(now, 100).unwrap().is_empty());
        assert!(store.db.iterator_cf(store.cf(CF_EXPIRY), rocksdb::IteratorMode::Start).next().is_none());
        assert!(store.db.iterator_cf(store.cf(CF_META), rocksdb::IteratorMode::Start).next().is_none());
    }

    #[test]
    fn test_contains_live() {
        let (store, _temp) = create_temp_store();
        let now = now_secs();
        store.put_with_ttl(b"k", b"v", Some(10), now + 10).unwrap();
        assert!(store.contains_live(b"k", now).unwrap());
        assert!(!store.contains_live(b"k", now + 10).unwrap());
        assert!(!store.contains_live(b"missing", now).unwrap());
    }

    #[test]
    fn test_expiry_index_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let now = now_secs();
        {
            let store = DiskStore::open(&path).unwrap();
            store.put_with_ttl(b"k", b"v", Some(1), now - 1).unwrap();
        }
        let store = DiskStore::open(&path).unwrap();
        assert!(store.meta_complete());
        assert_eq!(store.expire_batch(now, 10).unwrap(), vec![Bytes::from("k")]);
    }

    // ==================== BACKFILL ====================

    /// A data dir as the pre-index binary left it: `default` CF only, v1 entries.
    fn legacy_dir(entries: &[(&str, StoredEntry)]) -> TempDir {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open_default(temp_dir.path()).unwrap();
        for (k, e) in entries {
            db.put(k.as_bytes(), encode_entry_v1(e).unwrap()).unwrap();
        }
        temp_dir
    }

    fn aged_entry(value: &str, age_secs: i64, ttl: Option<u64>) -> StoredEntry {
        let t = Utc::now() - chrono::Duration::seconds(age_secs);
        StoredEntry { value: value.as_bytes().to_vec(), created_at: t, updated_at: t, ttl_secs: ttl }
    }

    fn ttl_expiry(_key: &[u8], m: &StoredEntryMeta) -> u64 {
        m.ttl_secs.map(|t| m.updated_at.timestamp() as u64 + t).unwrap_or(0)
    }

    fn run_backfill(store: &DiskStore, batch: usize) -> usize {
        let mut cursor: Option<Vec<u8>> = None;
        let mut total = 0;
        loop {
            let (n, next) = store.backfill_batch(cursor.as_deref(), batch, ttl_expiry).unwrap();
            total += n;
            match next {
                Some(k) => cursor = Some(k),
                None => break,
            }
        }
        store
            .save_backfill_state(&BackfillState { meta_complete: true, ..Default::default() })
            .unwrap();
        total
    }

    #[test]
    fn test_backfill_indexes_legacy_entries() {
        let dir = legacy_dir(&[
            ("expired", aged_entry("a", 100, Some(10))),
            ("fresh", aged_entry("b", 1, Some(3600))),
            ("forever", aged_entry("c", 100, None)),
        ]);
        let store = DiskStore::open(dir.path()).unwrap();
        assert!(!store.meta_complete());
        // Existence falls back to `default` before the backfill
        assert!(store.contains(b"forever").unwrap());
        assert!(!store.contains(b"missing").unwrap());

        assert_eq!(run_backfill(&store, 2), 3);
        assert!(store.meta_complete());

        let now = now_secs();
        assert_eq!(store.expire_batch(now, 10).unwrap(), vec![Bytes::from("expired")]);
        assert!(store.contains(b"fresh").unwrap());
        assert!(store.contains(b"forever").unwrap());
        assert_eq!(store.keys().count(), 2);
        assert_eq!(store.expire_batch(now + 7200, 10).unwrap(), vec![Bytes::from("fresh")]);
        assert!(store.contains(b"forever").unwrap());
    }

    #[test]
    fn test_backfill_does_not_resurrect_or_clobber_concurrent_writes() {
        let dir = legacy_dir(&[
            ("deleted", aged_entry("a", 100, Some(10))),
            ("rewritten", aged_entry("b", 100, Some(10))),
        ]);
        let store = DiskStore::open(dir.path()).unwrap();

        // Writes that land while the backfill is still pending must win over it
        assert!(store.delete(b"deleted").unwrap());
        let now = now_secs();
        store.put_with_ttl(b"rewritten", b"new", None, 0).unwrap();

        run_backfill(&store, 10);
        assert!(!store.contains(b"deleted").unwrap());
        assert!(store.expire_batch(now + 1_000_000, 10).unwrap().is_empty());
        assert_eq!(store.get(b"rewritten").unwrap().unwrap().value, Bytes::from("new"));
    }

    #[test]
    fn test_backfill_skips_key_rewritten_after_snapshot() {
        let dir = legacy_dir(&[("k", aged_entry("old", 100, Some(10)))]);
        let store = DiskStore::open(dir.path()).unwrap();
        let data = store.db.get(b"k").unwrap().unwrap();
        let old_meta = deserialize_entry_meta(&data).unwrap();

        // Rewrite after the backfill "read" the old version
        store.put_with_ttl(b"k", b"new", None, 0).unwrap();
        let current = store.get_meta(b"k").unwrap().unwrap();
        assert_ne!(current.version, version_of(&old_meta.updated_at));

        // Replaying the old version through the backfill must not touch it
        run_backfill(&store, 10);
        assert_eq!(store.get_meta(b"k").unwrap().unwrap(), current);
        assert!(store.expire_batch(now_secs(), 10).unwrap().is_empty());
    }

    #[test]
    fn test_backfill_rebuild_applies_new_expiry() {
        let (store, _temp) = create_temp_store();
        store.put(b"CAD/1", Bytes::from("v")).unwrap();
        store.put(b"other", Bytes::from("v")).unwrap();
        // New rule: everything under CAD/ expires 10s after creation
        let rule = |k: &[u8], m: &StoredEntryMeta| {
            if k.starts_with(b"CAD/") { m.created_at.timestamp() as u64 + 10 } else { 0 }
        };
        let (n, next) = store.backfill_batch(None, 100, rule).unwrap();
        assert_eq!((n, next), (2, None));
        assert!(store.expire_batch(now_secs(), 10).unwrap().is_empty());
        assert_eq!(store.expire_batch(now_secs() + 11, 10).unwrap(), vec![Bytes::from("CAD/1")]);
    }

    // ==================== DOWNGRADE ====================

    #[test]
    fn test_downgrade_leaves_a_dir_the_old_binary_can_open() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let value = pseudo_random_bytes(6000);
        {
            let store = DiskStore::open(&path).unwrap();
            store.put_with_ttl(b"k1", &value, Some(60), now_secs() + 60).unwrap();
            store.put(b"k2", Bytes::from("small")).unwrap();
        }
        assert_eq!(downgrade_data_dir(&path).unwrap(), 2);

        // What the old binary does: open `default` only, read v1 entries
        let cfs = DB::list_cf(&Options::default(), &path).unwrap();
        assert_eq!(cfs, vec!["default".to_string()]);
        let db = DB::open_default(&path).unwrap();
        let raw = db.get(b"k1").unwrap().unwrap();
        assert_eq!(raw[0], MSGPACK_VERSION);
        let e: StoredEntry = rmp_serde::from_slice(&raw[1..]).unwrap();
        assert_eq!(e.value, value);
        assert_eq!(e.ttl_secs, Some(60));
        drop(db);

        // And the new binary can take it back (backfill rebuilds the index)
        let store = DiskStore::open(&path).unwrap();
        assert!(!store.meta_complete());
        assert_eq!(store.get(b"k1").unwrap().unwrap().value.as_ref(), value.as_slice());
    }
}
