//! HTTP Server Setup
//!
//! Creates the Axum router with all API routes.

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, head, post, put},
    Router,
};
use std::sync::Arc;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use super::handlers;
use crate::engine::TieredEngine;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<TieredEngine>,
    #[cfg(feature = "cluster")]
    pub shard: Option<Arc<ShardState>>,
}

/// Shard routing state (only with cluster feature)
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ShardState {
    pub router: crate::cluster::ShardRouter,
    pub http_client: reqwest::Client,
    /// Service name template, e.g. "tieredkv-{}.tieredkv.default.svc.cluster.local"
    pub service_template: String,
    pub port: u16,
}

#[cfg(feature = "cluster")]
impl ShardState {
    /// Get the URL for a given shard
    pub fn shard_url(&self, shard_id: u32, path: &str) -> String {
        let host = self.service_template.replace("{}", &shard_id.to_string());
        format!("http://{}:{}{}", host, self.port, path)
    }
}

/// Create the API router with all routes
pub fn create_router(state: AppState) -> Router {
    // Bound in-flight request concurrency. Without this the write path buffers an
    // unbounded number of raw ~287KB request bodies: a high-memory heap profile
    // (2026-07-13, search-kv OOM cycle) showed 2.17GB / 91% of live heap sitting in
    // `Json<PutRequest>` body deserialization. Under sustained load the compress →
    // block_in_place(disk write) pipeline drains slower than intake (worse on iowait
    // nodes), so raw values pile up until the 8Gi cgroup OOM-kills the pod. This layer
    // is GLOBAL (one shared semaphore across all connections/clones): requests past the
    // limit wait for a permit *before* the handler runs its body extractor, so nothing
    // is buffered while queued. Tunable without a rebuild via MAX_CONCURRENT_REQUESTS.
    //
    // 128 (was 512): at 512 a slow disk let up to 512 requests each hold a body and
    // a blocking-pool thread; with 3-4 cores nothing is gained past a few dozen.
    let max_concurrent = std::env::var("MAX_CONCURRENT_REQUESTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(128);

    // Data-plane routes share the concurrency limit.
    let data = Router::new()
        .route("/kv/*key", get(handlers::get_key))
        .route("/kv/*key", put(handlers::put_key))
        .route("/kv/*key", delete(handlers::delete_key))
        .route("/kv/*key", head(handlers::head_key))
        // Admin operations
        .route("/admin/migrate", post(handlers::trigger_migration))
        .route("/admin/flush", post(handlers::flush))
        // Debug/profiling
        .route("/debug/profile", get(handlers::cpu_profile))
        // Middleware. Order matters: the concurrency limit sits OUTSIDE the body
        // limit / handler so its permit is acquired before any body is read.
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024)) // 64MB per-request ceiling
        .layer(GlobalConcurrencyLimitLayer::new(max_concurrent));

    // Health, stats and metrics stay outside the limit: when every slot is held
    // (slow disk, compaction stall) the liveness probe must still be answered, or
    // the kubelet kills a pod that is merely busy.
    let ops = Router::new()
        .route("/health", get(handlers::health))
        .route("/stats", get(handlers::stats))
        .route("/metrics", get(handlers::metrics));

    data.merge(ops)
        .layer(middleware::from_fn(crate::metrics::track))
        .layer(TraceLayer::new_for_http())
        // State
        .with_state(state)
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
        let state = AppState {
            engine,
            #[cfg(feature = "cluster")]
            shard: None,
        };
        let router = create_router(state);

        (router, temp_dir)
    }

    // The handlers reach TieredEngine, which uses block_in_place; that panics on
    // the current-thread runtime #[tokio::test] gives you by default. See the same
    // note in engine/tiered.rs.
    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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
        let state = AppState {
            engine,
            #[cfg(feature = "cluster")]
            shard: None,
        };
        let app = create_router(state);

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

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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
        let state = AppState {
            engine,
            #[cfg(feature = "cluster")]
            shard: None,
        };
        let app = create_router(state);

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

    #[tokio::test(flavor = "multi_thread")]
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
        let state = AppState {
            engine,
            #[cfg(feature = "cluster")]
            shard: None,
        };
        let app = create_router(state);

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

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
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

    async fn send(app: &Router, method: &str, uri: &str, body: &str) -> (StatusCode, bytes::Bytes) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        (status, axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_embeds_json_values_raw_and_quotes_the_rest() {
        let (app, _temp) = create_test_app();
        let doc = r#"{"hits":[1,2,{"a":"\u00e9"}],"total":3}"#;
        let put = serde_json::json!({ "value": doc }).to_string();
        assert_eq!(send(&app, "PUT", "/kv/doc", &put).await.0, StatusCode::CREATED);
        let (status, body) = send(&app, "GET", "/kv/doc", "").await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["key"], "doc");
        assert_eq!(json["tier"], "memory");
        assert_eq!(json["value"], serde_json::from_str::<serde_json::Value>(doc).unwrap());
        // Embedded verbatim, not re-serialized
        assert!(std::str::from_utf8(&body).unwrap().contains(doc));

        let put = serde_json::json!({ "value": "not json {" }).to_string();
        send(&app, "PUT", "/kv/plain", &put).await;
        let (_, body) = send(&app, "GET", "/kv/plain", "").await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["value"], "not json {");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_expired_key_is_not_served() {
        let (app, _temp) = create_test_app();
        let put = serde_json::json!({ "value": "\"v\"", "ttl": 0 }).to_string();
        assert_eq!(send(&app, "PUT", "/kv/short", &put).await.0, StatusCode::CREATED);
        assert_eq!(send(&app, "GET", "/kv/short", "").await.0, StatusCode::NOT_FOUND);
        assert_eq!(send(&app, "HEAD", "/kv/short", "").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_metrics_endpoint_reports_request_latency() {
        let (app, _temp) = create_test_app();
        send(&app, "GET", "/kv/missing", "").await;
        let (status, body) = send(&app, "GET", "/metrics", "").await;
        assert_eq!(status, StatusCode::OK);
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains(r#"kv_http_request_duration_seconds_count{method="GET",route="/kv/*key",status="404"}"#), "{text}");
        assert!(text.contains(r#"kv_get_results_total{result="miss"}"#));
        assert!(text.contains(r#"kv_rocksdb_property{name="rocksdb.total-blob-file-size"}"#));
        assert!(text.contains(r#"kv_memory_cache{stat="entries"}"#));
    }
}
