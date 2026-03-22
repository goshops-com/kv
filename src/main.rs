//! TieredKV - A tiered key-value store
//!
//! Run with: cargo run
//! Or with custom config: RUST_LOG=debug DATA_DIR=/tmp/data cargo run

use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use tieredkv::api::{create_router, AppState};
use tieredkv::disk::DiskConfig;
use tieredkv::engine::{EngineConfig, TieredEngine, TtlRule};
use tieredkv::memory::CacheConfig;
use tieredkv::object::{ObjectConfig, ObjectStore};
#[cfg(feature = "azure")]
use tieredkv::object::azure::{AzureBlobBackend, AzureBlobConfig};
#[cfg(feature = "cluster")]
use tieredkv::api::ShardState;
#[cfg(feature = "cluster")]
use tieredkv::cluster::{ShardConfig, ShardAddress, ShardRouter};

fn get_env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn create_engine(config: EngineConfig) -> Result<TieredEngine, Box<dyn std::error::Error>> {
    let memory = tieredkv::memory::MemoryCache::new(config.memory.clone());
    let disk = tieredkv::disk::DiskStore::new(config.disk.clone())?;

    let object = match (&config.object.azure_account, &config.object.azure_access_key) {
        #[cfg(feature = "azure")]
        (Some(account), Some(key)) => {
            info!("Using Azure Blob Storage backend: account={}", account);
            let azure_config = AzureBlobConfig {
                account_name: account.clone(),
                access_key: key.clone(),
                container_name: config.object.bucket.clone(),
            };
            let backend = AzureBlobBackend::new(&azure_config)?;
            ObjectStore::new(std::sync::Arc::new(backend), config.object.clone())
        }
        _ => {
            warn!("No Azure credentials configured, using in-memory object store");
            ObjectStore::in_memory(config.object.clone())
        }
    };

    Ok(TieredEngine::new(
        memory,
        disk,
        std::sync::Arc::new(object),
        config,
    ))
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("TieredKV v{} starting...", env!("CARGO_PKG_VERSION"));

    // Build configuration from environment
    let config = EngineConfig {
        memory: CacheConfig {
            max_size_bytes: get_env_or("MEMORY_MAX_SIZE_MB", 256) * 1024 * 1024,
            max_entries: Some(get_env_or("MEMORY_MAX_ENTRIES", 100_000)),
        },
        disk: DiskConfig {
            data_dir: get_env_or("DATA_DIR", "./data".to_string()),
            max_size_bytes: get_env_or("DISK_MAX_SIZE_GB", 50) * 1024 * 1024 * 1024,
            migration_age_secs: get_env_or("DISK_MIGRATION_AGE_HOURS", 24) * 3600,
            flush_every_n_writes: get_env_or("FLUSH_EVERY_N_WRITES", 1000),
        },
        object: ObjectConfig {
            bucket: get_env_or("S3_BUCKET", "tieredkv-data".to_string()),
            endpoint: env::var("S3_ENDPOINT").ok(),
            region: get_env_or("S3_REGION", "us-east-1".to_string()),
            prefix: get_env_or("S3_PREFIX", "data/".to_string()),
            azure_account: env::var("AZURE_STORAGE_ACCOUNT").ok(),
            azure_access_key: env::var("AZURE_STORAGE_ACCESS_KEY").ok(),
        },
        migration_batch_size: get_env_or("MIGRATION_BATCH_SIZE", 100),
        migration_threshold_percent: get_env_or("MIGRATION_THRESHOLD_PERCENT", 80.0),
        ttl_rules: parse_ttl_rules(&env::var("TTL_RULES").unwrap_or_default()),
    };

    let port = get_env_or("HTTP_PORT", 8080u16);

    info!("Configuration loaded:");
    info!("  Memory: {}MB max, {} max entries",
        config.memory.max_size_bytes / (1024 * 1024),
        config.memory.max_entries.unwrap_or(0));
    info!("  Disk: {}GB max, {} hour migration age",
        config.disk.max_size_bytes / (1024 * 1024 * 1024),
        config.disk.migration_age_secs / 3600);
    info!("  Object Storage: bucket={}, region={}",
        config.object.bucket, config.object.region);
    if !config.ttl_rules.is_empty() {
        for rule in &config.ttl_rules {
            info!("  TTL: prefix='{}' -> {}s", rule.prefix, rule.ttl_secs);
        }
    }

    // Create the engine
    let engine = match create_engine(config) {
        Ok(engine) => Arc::new(engine),
        Err(e) => {
            tracing::error!("Failed to create engine: {}", e);
            std::process::exit(1);
        }
    };

    info!("Engine initialized successfully");

    // Build app state
    let state = AppState {
        engine: engine.clone(),
        #[cfg(feature = "cluster")]
        shard: build_shard_state(port),
    };

    // Create router
    let app = create_router(state);

    // Start background migration task
    let migration_engine = engine.clone();
    tokio::spawn(async move {
        let interval_secs = get_env_or("MIGRATION_INTERVAL_SECS", 60u64);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            if migration_engine.should_migrate() {
                info!("Starting background migration...");
                match migration_engine.run_migration().await {
                    Ok(count) if count > 0 => {
                        info!("Migrated {} entries to object storage", count);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Migration failed: {}", e);
                    }
                }
            }
        }
    });

    // Background re-serialization: convert legacy JSON entries to MessagePack
    // Runs once at startup, converting 500 entries per batch with 1s pause between batches
    let reser_engine = engine.clone();
    tokio::spawn(async move {
        // Wait for startup to settle
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        info!("Starting legacy JSON → MessagePack re-serialization...");
        let mut total = 0usize;
        loop {
            let converted = tokio::task::block_in_place(|| {
                reser_engine.reserialize_legacy_batch(500)
            });
            match converted {
                Ok(0) => {
                    info!("Re-serialization complete: {} entries converted to MessagePack", total);
                    break;
                }
                Ok(n) => {
                    total += n;
                    if total % 5000 == 0 {
                        info!("Re-serialization progress: {} entries converted", total);
                    }
                    // Pace to avoid CPU spikes
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
                Err(e) => {
                    warn!("Re-serialization error: {}, retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // Start background TTL cleanup task
    let ttl_engine = engine.clone();
    tokio::spawn(async move {
        let interval_secs = get_env_or("TTL_CLEANUP_INTERVAL_SECS", 300u64);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;
            match ttl_engine.run_ttl_cleanup().await {
                Ok(count) if count > 0 => {
                    info!("TTL cleanup: removed {} expired entries", count);
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("TTL cleanup failed: {}", e);
                }
            }
        }
    });

    // Bind to address
    let addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("Server listening on http://{}", addr);

    // Run the server
    axum::serve(listener, app).await.unwrap();
}

/// Build shard state from environment variables
/// Set SHARD_ID and TOTAL_SHARDS to enable sharding
#[cfg(feature = "cluster")]
fn build_shard_state(port: u16) -> Option<Arc<ShardState>> {
    let total_shards: u32 = match env::var("TOTAL_SHARDS").ok().and_then(|v| v.parse().ok()) {
        Some(n) if n > 1 => n,
        _ => return None,
    };

    // Extract shard_id from pod name (e.g., tieredkv-2 -> 2)
    // or from SHARD_ID env var directly
    let shard_id: u32 = env::var("SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| {
            env::var("POD_NAME")
                .ok()
                .and_then(|name| name.rsplit('-').next().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0);

    let service_name = get_env_or("SERVICE_NAME", "tieredkv".to_string());
    let namespace = get_env_or("POD_NAMESPACE", "default".to_string());

    // Build shard addresses for consistent hashing
    let shard_addresses: Vec<ShardAddress> = (0..total_shards)
        .map(|id| ShardAddress {
            shard_id: id,
            address: format!(
                "{}-{}.{}.{}.svc.cluster.local:{}",
                service_name, id, service_name, namespace, port
            ),
        })
        .collect();

    let shard_config = ShardConfig {
        shard_id,
        total_shards,
        virtual_nodes: get_env_or("VIRTUAL_NODES", 150),
        shard_addresses,
    };

    let router = ShardRouter::new(shard_config);

    // DNS template: tieredkv-{}.tieredkv.default.svc.cluster.local
    let service_template = format!("{}-{{}}.{}.{}.svc.cluster.local", service_name, service_name, namespace);

    info!("Cluster mode enabled: shard {}/{}, service={}",
        shard_id, total_shards, service_name);

    Some(Arc::new(ShardState {
        router,
        http_client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client"),
        service_template,
        port,
    }))
}

/// Parse TTL_RULES env var: "CAD/:7d,autocomplete/:24h,tmp/:1h"
/// Supported suffixes: s (seconds), m (minutes), h (hours), d (days)
fn parse_ttl_rules(input: &str) -> Vec<TtlRule> {
    if input.is_empty() {
        return vec![];
    }

    input
        .split(',')
        .filter_map(|part| {
            let (prefix, duration_str) = part.split_once(':')?;
            let duration_str = duration_str.trim();
            let prefix = prefix.trim().to_string();

            let (num_str, multiplier) = if let Some(n) = duration_str.strip_suffix('d') {
                (n, 86400u64)
            } else if let Some(n) = duration_str.strip_suffix('h') {
                (n, 3600u64)
            } else if let Some(n) = duration_str.strip_suffix('m') {
                (n, 60u64)
            } else if let Some(n) = duration_str.strip_suffix('s') {
                (n, 1u64)
            } else {
                (duration_str, 1u64)
            };

            let num: u64 = num_str.parse().ok()?;
            Some(TtlRule {
                prefix,
                ttl_secs: num * multiplier,
            })
        })
        .collect()
}
