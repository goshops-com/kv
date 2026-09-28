//! HTTP API Handlers
//!
//! REST endpoints for the tiered key-value store.
//! When cluster feature is enabled, handlers proxy requests
//! to the correct shard if the key doesn't belong to this node.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
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

/// Response for GET operations (shape of the body `get_key` writes by hand)
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

/// GET /metrics - Prometheus metrics
pub async fn metrics(State(state): State<AppState>) -> Response {
    let body = crate::metrics::render(&state.engine);
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// Build the GET response body: `{"key":..,"value":<raw JSON>,"tier":..}`.
/// Values are stored JSON documents, embedded verbatim (no re-parse into a tree,
/// no re-escaping); anything that isn't valid JSON is embedded as a JSON string.
/// Runs on the blocking pool: validating a large value is CPU-bound.
fn get_response_body(key: &str, value: &[u8], tier: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + key.len() + 48);
    out.extend_from_slice(b"{\"key\":");
    serde_json::to_writer(&mut out, key).expect("writing to a Vec");
    out.extend_from_slice(b",\"value\":");
    if serde_json::from_slice::<serde::de::IgnoredAny>(value).is_ok() {
        out.extend_from_slice(value);
    } else {
        serde_json::to_writer(&mut out, String::from_utf8_lossy(value).as_ref()).expect("writing to a Vec");
    }
    out.extend_from_slice(b",\"tier\":");
    serde_json::to_writer(&mut out, tier).expect("writing to a Vec");
    out.push(b'}');
    out
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

// normalize_key lives in the shared shard-router crate (re-exported via crate::cluster)
// so the proxy and the shards normalize identically.
use crate::cluster::normalize_key;

/// GET /kv/*key - Get a value by key
pub async fn get_key(
    State(state): State<AppState>,
    Path(raw_key): Path<String>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
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
            let tier: String = entry.tier.into();
            let body = tokio::task::spawn_blocking(move || get_response_body(&key, &entry.value, &tier))
                .await
                .map_err(|e| (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: e.to_string(), code: "INTERNAL_ERROR".to_string() }),
                ))?;
            Ok(([(header::CONTENT_TYPE, "application/json")], Body::from(body)).into_response())
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

/// GET /debug/profile?seconds=10 - Generate CPU flamegraph SVG
pub async fn cpu_profile(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response, (StatusCode, Json<ErrorResponse>)> {
    let seconds: u64 = params.get("seconds").and_then(|s| s.parse().ok()).unwrap_or(10);
    let seconds = seconds.min(60); // cap at 60s

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
            error: e.to_string(), code: "PROFILE_ERROR".to_string(),
        })))?;

    tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;

    let report = guard.report().build().map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: e.to_string(), code: "PROFILE_ERROR".to_string() }),
    ))?;

    let mut body = Vec::new();
    report.flamegraph(&mut body).map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: e.to_string(), code: "PROFILE_ERROR".to_string() }),
    ))?;

    Ok(axum::response::Response::builder()
        .header("content-type", "image/svg+xml")
        .body(axum::body::Body::from(body))
        .unwrap())
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
