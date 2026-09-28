//! Prometheus metrics, served at `GET /metrics`.
//!
//! Request latency by route/method/status, GET outcome by tier, TTL sweep and
//! expiry-backfill progress, plus memory-cache and RocksDB gauges sampled at
//! scrape time.

use axum::{
    body::Body,
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::LazyLock;
use std::time::Instant;

use crate::engine::TieredEngine;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

fn register<C: prometheus::core::Collector + Clone + 'static>(c: C) -> C {
    REGISTRY.register(Box::new(c.clone())).expect("metric registered once");
    c
}

const LATENCY_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub static HTTP_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new("kv_http_request_duration_seconds", "HTTP request latency, including time queued for a concurrency slot")
                .buckets(LATENCY_BUCKETS.to_vec()),
            &["route", "method", "status"],
        )
        .unwrap(),
    )
});

pub static HTTP_IN_FLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new("kv_http_requests_in_flight", "HTTP requests currently being served").unwrap())
});

pub static GET_RESULTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(
        IntCounterVec::new(
            Opts::new("kv_get_results_total", "GET outcomes by the tier that answered (memory, disk, object, expired, miss)"),
            &["result"],
        )
        .unwrap(),
    )
});

pub static TTL_SWEEP_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register(
        Histogram::with_opts(
            HistogramOpts::new("kv_ttl_sweep_duration_seconds", "Duration of one TTL sweep")
                .buckets(vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0]),
        )
        .unwrap(),
    )
});

pub static TTL_EXPIRED: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new("kv_ttl_expired_total", "Entries deleted by the TTL sweep").unwrap())
});

pub static BACKFILL_VISITED: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new("kv_expiry_backfill_visited_total", "Entries visited by the expiry-index backfill").unwrap())
});

pub static BACKFILL_COMPLETE: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new("kv_expiry_backfill_complete", "1 once the expiry index covers every entry").unwrap())
});

static MEMORY_CACHE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(
        IntGaugeVec::new(Opts::new("kv_memory_cache", "L1 memory cache state (entries, bytes, max_bytes, hits, misses)"), &["stat"])
            .unwrap(),
    )
});

static ROCKSDB: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(
        IntGaugeVec::new(Opts::new("kv_rocksdb_property", "RocksDB integer properties of the value column family"), &["name"])
            .unwrap(),
    )
});

const ROCKSDB_PROPERTIES: &[&str] = &[
    "rocksdb.estimate-num-keys",
    "rocksdb.block-cache-usage",
    "rocksdb.block-cache-pinned-usage",
    "rocksdb.cur-size-all-mem-tables",
    "rocksdb.estimate-table-readers-mem",
    "rocksdb.live-sst-files-size",
    "rocksdb.total-blob-file-size",
    "rocksdb.live-blob-file-size",
    "rocksdb.live-blob-file-garbage-size",
    "rocksdb.num-blob-files",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes",
    "rocksdb.is-write-stopped",
    "rocksdb.actual-delayed-write-rate",
];

/// Records latency for every request that went through a routed handler.
pub async fn track(req: Request<Body>, next: Next) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = req.method().as_str().to_owned();
    let started = Instant::now();
    HTTP_IN_FLIGHT.inc();
    let response = next.run(req).await;
    HTTP_IN_FLIGHT.dec();
    HTTP_DURATION
        .with_label_values(&[&route, &method, response.status().as_str()])
        .observe(started.elapsed().as_secs_f64());
    response
}

/// Render all metrics in the Prometheus text format.
pub fn render(engine: &TieredEngine) -> Vec<u8> {
    let cache = engine.memory_stats();
    MEMORY_CACHE.with_label_values(&["entries"]).set(cache.entries as i64);
    MEMORY_CACHE.with_label_values(&["bytes"]).set(cache.size_bytes as i64);
    MEMORY_CACHE.with_label_values(&["max_bytes"]).set(cache.max_size_bytes as i64);
    MEMORY_CACHE.with_label_values(&["hits"]).set(cache.hits as i64);
    MEMORY_CACHE.with_label_values(&["misses"]).set(cache.misses as i64);
    for name in ROCKSDB_PROPERTIES {
        if let Some(v) = engine.disk_property(name) {
            ROCKSDB.with_label_values(&[name]).set(v as i64);
        }
    }
    // Touch the lazily-registered metrics so they show up before first use
    LazyLock::force(&GET_RESULTS);
    LazyLock::force(&TTL_SWEEP_SECONDS);
    LazyLock::force(&TTL_EXPIRED);
    LazyLock::force(&BACKFILL_VISITED);
    LazyLock::force(&BACKFILL_COMPLETE);

    let mut out = Vec::with_capacity(16 * 1024);
    TextEncoder::new().encode(&REGISTRY.gather(), &mut out).expect("text encoding");
    out
}
