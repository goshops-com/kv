//! search-kv-proxy
//!
//! A tiny, stateless consistent-hashing router that sits in front of N sharded
//! `search-kv` instances and exposes the SAME `/kv/*key` HTTP API as a single kv.
//!
//! Why this exists: sharding search-kv would otherwise require re-implementing the
//! xxh3_64 consistent-hash ring in every client language (Python search-inference,
//! Go model-local-proxy, Node feature-store). That cross-language parity is exactly
//! what caused silent data loss in the feature-store before. Instead, this proxy
//! depends on the SAME `shard-router` crate the kv uses, so its routing is
//! byte-identical to what each shard's `owns_key()` expects. It pulls only the ring
//! (xxhash) — no rocksdb/engine — so it builds in seconds and ships a tiny image.
//!
//! Stateless → run several replicas behind a Service. The only state is the ring,
//! derived deterministically from env (TOTAL_SHARDS + DNS template), so every
//! replica routes identically.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header::CONTENT_TYPE, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use shard_router::{normalize_key, ShardAddress, ShardConfig, ShardRouter};
use tracing::{error, info};

#[derive(Clone)]
struct ProxyState {
    router: Arc<ShardRouter>,
    client: reqwest::Client,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Build the ring from env.
///
/// Shard addresses come from either an explicit `SHARD_ADDRESSES` list (comma
/// separated `host:port`, indexed as shard 0..N — handy for local testing, ops
/// overrides, and the single-shard passthrough cutover) or, by default, the
/// StatefulSet headless-service DNS convention:
/// `{service}-{id}.{service}.{namespace}.svc.cluster.local:{port}`.
fn build_router() -> ShardRouter {
    let virtual_nodes: u32 = env_parse("VIRTUAL_NODES", 150);

    let shard_addresses: Vec<ShardAddress> = match std::env::var("SHARD_ADDRESSES") {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .enumerate()
            .map(|(id, address)| ShardAddress { shard_id: id as u32, address })
            .collect(),
        _ => {
            let total_shards: u32 = env_parse("TOTAL_SHARDS", 1);
            let service = env_or("SHARD_SERVICE_NAME", "search-kv");
            let namespace = env_or("POD_NAMESPACE", "default");
            let port = env_or("SHARD_PORT", "8080");
            (0..total_shards)
                .map(|id| ShardAddress {
                    shard_id: id,
                    address: format!(
                        "{service}-{id}.{service}.{namespace}.svc.cluster.local:{port}"
                    ),
                })
                .collect()
        }
    };

    let total_shards = shard_addresses.len() as u32;

    // shard_id is irrelevant for the router (it owns nothing, it only routes).
    ShardRouter::new(ShardConfig {
        shard_id: 0,
        total_shards,
        virtual_nodes,
        shard_addresses,
    })
}

/// Forward any /kv/*key request to the shard that owns the key.
async fn proxy_kv(
    State(st): State<ProxyState>,
    method: Method,
    Path(raw_key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Normalize + hash with the SAME code the shards use, so the shard we pick is
    // the one whose owns_key() will accept the request.
    let key = normalize_key(raw_key);

    let addr = match st.router.get_key_address(key.as_bytes()) {
        Some(a) => a.address.clone(),
        None => {
            error!("no shard address for key (total_shards=0?)");
            return (StatusCode::BAD_GATEWAY, "no shard available").into_response();
        }
    };

    let url = format!("http://{addr}/kv/{key}");
    let mut rb = st.client.request(method, &url);
    if let Some(ct) = headers.get(CONTENT_TYPE) {
        rb = rb.header(CONTENT_TYPE, ct);
    }
    if !body.is_empty() {
        rb = rb.body(body);
    }

    match rb.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let ct = resp.headers().get(reqwest::header::CONTENT_TYPE).cloned();
            let bytes = resp.bytes().await.unwrap_or_default();
            let mut builder = Response::builder().status(status);
            if let Some(ct) = ct {
                builder = builder.header(CONTENT_TYPE, ct.as_bytes());
            }
            builder
                .body(Body::from(bytes))
                .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
        }
        Err(e) => {
            error!("shard request to {url} failed: {e}");
            (StatusCode::BAD_GATEWAY, format!("shard proxy error: {e}")).into_response()
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let router = Arc::new(build_router());
    info!("search-kv-proxy: routing across {} shard(s)", router.total_shards());
    for s in router.all_shards() {
        info!("  shard[{}] -> {}", s.shard_id, s.address);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(env_parse("SHARD_TIMEOUT_SECS", 5)))
        .pool_max_idle_per_host(env_parse("POOL_MAX_IDLE_PER_HOST", 64))
        .build()
        .expect("failed to build reqwest client");

    let state = ProxyState { router, client };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/kv/*key", any(proxy_kv))
        .with_state(state);

    let port = env_or("HTTP_PORT", "8080");
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr} failed: {e}"));
    info!("search-kv-proxy listening on {addr}");
    axum::serve(listener, app).await.expect("server error");
}
