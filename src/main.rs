//! TieredKV - A tiered key-value store
//!
//! Run with: cargo run
//! Or with custom config: RUST_LOG=debug DATA_DIR=/tmp/data cargo run

use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use tieredkv::api::create_router;
use tieredkv::disk::DiskConfig;
use tieredkv::engine::{EngineConfig, TieredEngine};
use tieredkv::memory::CacheConfig;
use tieredkv::object::ObjectConfig;

fn get_env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
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
        },
        migration_batch_size: get_env_or("MIGRATION_BATCH_SIZE", 100),
        migration_threshold_percent: get_env_or("MIGRATION_THRESHOLD_PERCENT", 80.0),
    };

    info!("Configuration loaded:");
    info!("  Memory: {}MB max, {} max entries",
        config.memory.max_size_bytes / (1024 * 1024),
        config.memory.max_entries.unwrap_or(0));
    info!("  Disk: {}GB max, {} hour migration age",
        config.disk.max_size_bytes / (1024 * 1024 * 1024),
        config.disk.migration_age_secs / 3600);
    info!("  Object Storage: bucket={}, region={}",
        config.object.bucket, config.object.region);

    // Create the engine
    let engine = match TieredEngine::with_config(config) {
        Ok(engine) => Arc::new(engine),
        Err(e) => {
            tracing::error!("Failed to create engine: {}", e);
            std::process::exit(1);
        }
    };

    info!("Engine initialized successfully");

    // Create router
    let app = create_router(engine.clone());

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

    // Bind to address
    let port = get_env_or("HTTP_PORT", 8080u16);
    let addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("Server listening on http://{}", addr);

    // Run the server
    axum::serve(listener, app).await.unwrap();
}
