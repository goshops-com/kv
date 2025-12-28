//! HTTP Server Setup
//!
//! Creates the Axum router with all API routes.

use axum::{
    routing::{delete, get, head, post, put},
    Router,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use super::handlers;
use crate::engine::TieredEngine;

/// Shared state for the API
pub struct ApiState {
    pub engine: Arc<TieredEngine>,
}

/// Create the API router with all routes
pub fn create_router(engine: Arc<TieredEngine>) -> Router {
    Router::new()
        // Health and stats
        .route("/health", get(handlers::health))
        .route("/stats", get(handlers::stats))
        // Key-value operations
        .route("/kv/:key", get(handlers::get_key))
        .route("/kv/:key", put(handlers::put_key))
        .route("/kv/:key", delete(handlers::delete_key))
        .route("/kv/:key", head(handlers::head_key))
        // Admin operations
        .route("/admin/migrate", post(handlers::trigger_migration))
        .route("/admin/flush", post(handlers::flush))
        // Middleware
        .layer(TraceLayer::new_for_http())
        // State
        .with_state(engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    use tempfile::TempDir;
    use crate::engine::EngineConfig;
    use crate::disk::DiskConfig;

    fn create_test_app() -> (Router, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let config = EngineConfig {
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let engine = Arc::new(TieredEngine::with_config(config).unwrap());
        let router = create_router(engine);

        (router, temp_dir)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (app, _temp) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "healthy");
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let (app, _temp) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("memory_entries").is_some());
        assert!(json.get("disk_entries").is_some());
    }

    #[tokio::test]
    async fn test_put_and_get_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = EngineConfig {
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = Arc::new(TieredEngine::with_config(config).unwrap());
        let app = create_router(engine);

        // PUT a key
        let put_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/kv/mykey")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value": "myvalue"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(put_response.status(), StatusCode::CREATED);

        // GET the key
        let get_response = app
            .oneshot(
                Request::builder()
                    .uri("/kv/mykey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["key"], "mykey");
        assert_eq!(json["value"], "myvalue");
        assert_eq!(json["tier"], "memory");
    }

    #[tokio::test]
    async fn test_get_nonexistent_key_returns_404() {
        let (app, _temp) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/kv/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = EngineConfig {
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = Arc::new(TieredEngine::with_config(config).unwrap());
        let app = create_router(engine);

        // PUT a key
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/kv/mykey")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value": "myvalue"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // DELETE the key
        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/kv/mykey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(delete_response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(delete_response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["deleted"], true);

        // Verify it's gone
        let get_response = app
            .oneshot(
                Request::builder()
                    .uri("/kv/mykey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_head_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = EngineConfig {
            disk: DiskConfig {
                data_dir: temp_dir.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = Arc::new(TieredEngine::with_config(config).unwrap());
        let app = create_router(engine);

        // HEAD non-existent key
        let head_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/kv/mykey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(head_response.status(), StatusCode::NOT_FOUND);

        // PUT a key
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/kv/mykey")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value": "myvalue"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // HEAD existing key
        let head_response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/kv/mykey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(head_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_flush_endpoint() {
        let (app, _temp) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/flush")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_migrate_endpoint() {
        let (app, _temp) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/migrate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("migrated").is_some());
    }
}
