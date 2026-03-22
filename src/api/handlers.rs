//! HTTP API Handlers
//!
//! REST endpoints for the tiered key-value store.
//! When cluster feature is enabled, handlers proxy requests
//! to the correct shard if the key doesn't belong to this node.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::server::AppState;
use crate::engine::StorageTier;

/// Request body for PUT operations
#[derive(Debug, Deserialize, Serialize)]
pub struct PutRequest {
    pub value: String,
    pub ttl: Option<u64>,
}

/// Response for GET operations
/// Uses RawValue for the value field to avoid re-serializing JSON-inside-JSON
#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub key: String,
    pub value: Box<serde_json::value::RawValue>,
    pub tier: String,
}

/// Response for DELETE operations
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub key: String,
    pub deleted: bool,
}

/// Response for health check
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Response for stats
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub memory_entries: usize,
    pub memory_size_bytes: usize,
    pub memory_hit_rate: f64,
    pub disk_entries: usize,
    pub disk_size_bytes: u64,
    pub disk_usage_percent: f64,
    pub migrations_completed: u64,
}

/// Error response
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

impl From<StorageTier> for String {
    fn from(tier: StorageTier) -> Self {
        match tier {
            StorageTier::Memory => "memory".to_string(),
            StorageTier::Disk => "disk".to_string(),
            StorageTier::Object => "object".to_string(),
        }
    }
}

/// GET /health - Health check endpoint
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// GET /stats - Get engine statistics
pub async fn stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.engine.stats().await;
    let hit_rate = if stats.memory_hits + stats.memory_misses > 0 {
        stats.memory_hits as f64 / (stats.memory_hits + stats.memory_misses) as f64
    } else {
        0.0
    };

    Json(StatsResponse {
        memory_entries: stats.memory_entries,
        memory_size_bytes: stats.memory_size_bytes,
        memory_hit_rate: hit_rate,
        disk_entries: stats.disk_entries,
        disk_size_bytes: stats.disk_size_bytes,
        disk_usage_percent: state.engine.disk_usage_percent(),
        migrations_completed: stats.migrations_completed,
    })
}

/// Strip leading slash from wildcard path capture
fn normalize_key(key: String) -> String {
    key.strip_prefix('/').map(String::from).unwrap_or(key)
}

/// GET /kv/*key - Get a value by key
pub async fn get_key(
    State(state): State<AppState>,
    Path(raw_key): Path<String>,
) -> Result<Json<GetResponse>, (StatusCode, Json<ErrorResponse>)> {
    let key = normalize_key(raw_key);
    // With client-side routing, reject keys that don't belong to this shard
    #[cfg(feature = "cluster")]
    if let Some(ref shard) = state.shard {
        if !shard.router.owns_key(key.as_bytes()) {
            return Err(not_found(&key));
        }
    }

    match state.engine.get(key.as_bytes()).await {
        Ok(Some(entry)) => {
            // Embed value as raw JSON (no re-parsing, no re-escaping of ~287KB)
            let value_str = String::from_utf8_lossy(&entry.value);
            let raw = serde_json::value::RawValue::from_string(value_str.into_owned())
                .unwrap_or_else(|_| {
                    // Fallback: if value isn't valid JSON, quote it as a JSON string
                    serde_json::value::RawValue::from_string(
                        serde_json::to_string(&String::from_utf8_lossy(&entry.value).as_ref()).unwrap()
                    ).unwrap()
                });
            Ok(Json(GetResponse {
                key,
                value: raw,
                tier: entry.tier.into(),
            }))
        }
        Ok(None) => Err(not_found(&key)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "INTERNAL_ERROR".to_string(),
            }),
        )),
    }
}

/// PUT /kv/*key - Set a value
pub async fn put_key(
    State(state): State<AppState>,
    Path(raw_key): Path<String>,
    Json(body): Json<PutRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let key = normalize_key(raw_key);
    // With client-side routing, reject keys that don't belong to this shard
    #[cfg(feature = "cluster")]
    if let Some(ref shard) = state.shard {
        if !shard.router.owns_key(key.as_bytes()) {
            return Err((
                StatusCode::MISDIRECTED_REQUEST,
                Json(ErrorResponse {
                    error: "Wrong shard".to_string(),
                    code: "WRONG_SHARD".to_string(),
                }),
            ));
        }
    }

    match state.engine.put_with_ttl(key.as_bytes(), Bytes::from(body.value), body.ttl).await {
        Ok(()) => Ok(StatusCode::CREATED),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "INTERNAL_ERROR".to_string(),
            }),
        )),
    }
}

/// DELETE /kv/:key - Delete a value
pub async fn delete_key(
    State(state): State<AppState>,
    Path(raw_key): Path<String>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let key = normalize_key(raw_key);
    // With client-side routing, reject keys that don't belong to this shard
    #[cfg(feature = "cluster")]
    if let Some(ref shard) = state.shard {
        if !shard.router.owns_key(key.as_bytes()) {
            return Err(not_found(&key));
        }
    }

    match state.engine.delete(key.as_bytes()).await {
        Ok(deleted) => Ok(Json(DeleteResponse { key, deleted })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "INTERNAL_ERROR".to_string(),
            }),
        )),
    }
}

/// HEAD /kv/:key - Check if a key exists
pub async fn head_key(
    State(state): State<AppState>,
    Path(raw_key): Path<String>,
) -> StatusCode {
    let key = normalize_key(raw_key);
    // With client-side routing, reject keys that don't belong to this shard
    #[cfg(feature = "cluster")]
    if let Some(ref shard) = state.shard {
        if !shard.router.owns_key(key.as_bytes()) {
            return StatusCode::NOT_FOUND;
        }
    }

    match state.engine.contains(key.as_bytes()).await {
        Ok(true) => StatusCode::OK,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// POST /admin/migrate - Trigger migration manually
pub async fn trigger_migration(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    match state.engine.run_migration().await {
        Ok(count) => Ok(Json(serde_json::json!({
            "migrated": count,
            "message": format!("Migrated {} entries to object storage", count)
        }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "MIGRATION_ERROR".to_string(),
            }),
        )),
    }
}

/// POST /admin/flush - Flush disk to ensure durability
pub async fn flush(
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match state.engine.flush() {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "FLUSH_ERROR".to_string(),
            }),
        )),
    }
}

fn not_found(key: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("Key '{}' not found", key),
            code: "KEY_NOT_FOUND".to_string(),
        }),
    )
}

#[cfg(feature = "cluster")]
fn proxy_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse {
            error: format!("Shard proxy error: {}", e),
            code: "PROXY_ERROR".to_string(),
        }),
    )
}

#[cfg(feature = "cluster")]
fn proxy_status_error(status: u16) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse {
            error: format!("Shard returned status {}", status),
            code: "PROXY_ERROR".to_string(),
        }),
    )
}
