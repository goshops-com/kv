//! HTTP API Handlers
//!
//! REST endpoints for the tiered key-value store.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::engine::{EngineStats, StorageTier, TieredEngine};

/// Request body for PUT operations
#[derive(Debug, Deserialize)]
pub struct PutRequest {
    pub value: String,
}

/// Response for GET operations
#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub key: String,
    pub value: String,
    pub tier: String,
}

/// Response for DELETE operations
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
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
pub async fn stats(State(engine): State<Arc<TieredEngine>>) -> Json<StatsResponse> {
    let stats = engine.stats().await;
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
        disk_usage_percent: engine.disk_usage_percent(),
        migrations_completed: stats.migrations_completed,
    })
}

/// GET /kv/:key - Get a value by key
pub async fn get_key(
    State(engine): State<Arc<TieredEngine>>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, (StatusCode, Json<ErrorResponse>)> {
    match engine.get(key.as_bytes()).await {
        Ok(Some(entry)) => {
            let value = String::from_utf8_lossy(&entry.value).to_string();
            Ok(Json(GetResponse {
                key,
                value,
                tier: entry.tier.into(),
            }))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Key '{}' not found", key),
                code: "KEY_NOT_FOUND".to_string(),
            }),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: "INTERNAL_ERROR".to_string(),
            }),
        )),
    }
}

/// PUT /kv/:key - Set a value
pub async fn put_key(
    State(engine): State<Arc<TieredEngine>>,
    Path(key): Path<String>,
    Json(body): Json<PutRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match engine.put(key.as_bytes(), Bytes::from(body.value)).await {
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
    State(engine): State<Arc<TieredEngine>>,
    Path(key): Path<String>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<ErrorResponse>)> {
    match engine.delete(key.as_bytes()).await {
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
    State(engine): State<Arc<TieredEngine>>,
    Path(key): Path<String>,
) -> StatusCode {
    match engine.contains(key.as_bytes()).await {
        Ok(true) => StatusCode::OK,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// POST /admin/migrate - Trigger migration manually
pub async fn trigger_migration(
    State(engine): State<Arc<TieredEngine>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    match engine.run_migration().await {
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
    State(engine): State<Arc<TieredEngine>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match engine.flush() {
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
