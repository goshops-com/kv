//! Behaviour of the proxy over real HTTP: a fake shard (axum on 127.0.0.1:0) stands in
//! for search-kv, the proxy is served on another random port, and a reqwest client
//! talks to it like search-inference / model-local-proxy do.

use super::*;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Requests a fake shard received: (method, path, content-type, body).
type Seen = Arc<Mutex<Vec<(String, String, Option<String>, Vec<u8>)>>>;

/// Tests that change process env vars run one at a time.
static ENV_LOCK: Mutex<()> = Mutex::new(());

const ENV_VARS: &[&str] = &[
    "SHARD_ADDRESSES",
    "TOTAL_SHARDS",
    "SHARD_SERVICE_NAME",
    "POD_NAMESPACE",
    "SHARD_PORT",
    "VIRTUAL_NODES",
    "SHARD_TIMEOUT_SECS",
    "SHARD_CONNECT_TIMEOUT_MS",
    "POOL_MAX_IDLE_PER_HOST",
];

fn with_env<R>(vars: &[(&str, &str)], f: impl FnOnce() -> R) -> R {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for v in ENV_VARS {
        std::env::remove_var(v);
    }
    for (k, v) in vars {
        std::env::set_var(k, v);
    }
    let out = f();
    for v in ENV_VARS {
        std::env::remove_var(v);
    }
    out
}

/// Fake search-kv shard. Answers:
/// - `/kv/plain`  → 200, body "raw" without content-type
/// - `/kv/teapot` → 418 "short and stout" (text/plain)
/// - anything else → 201, echoes the body with the request's content-type
async fn fake_shard(name: &'static str) -> (String, Seen) {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let rec = seen.clone();
    let app = Router::new().fallback(move |method: Method, uri: axum::http::Uri, headers: HeaderMap, body: Bytes| {
        let rec = rec.clone();
        async move {
            let ct = headers.get(CONTENT_TYPE).map(|v| v.to_str().unwrap().to_string());
            rec.lock().unwrap().push((method.to_string(), uri.path().to_string(), ct.clone(), body.to_vec()));
            match uri.path() {
                "/kv/plain" => Response::builder().status(200).body(Body::from("raw")).unwrap(),
                "/kv/teapot" => (StatusCode::IM_A_TEAPOT, "short and stout").into_response(),
                _ => {
                    let mut b = Response::builder().status(201).header("x-shard", name);
                    if let Some(ct) = ct {
                        b = b.header(CONTENT_TYPE, ct);
                    }
                    b.body(Body::from(body)).unwrap()
                }
            }
        }
    })
    // Like the real shards (src/api/server.rs).
    .layer(DefaultBodyLimit::max(64 * 1024 * 1024));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, seen)
}

fn router_for(addrs: &[&str]) -> ShardRouter {
    let list = addrs.join(",");
    with_env(&[("SHARD_ADDRESSES", list.as_str())], build_router)
}

async fn serve_proxy(router: ShardRouter, max_body: usize) -> String {
    let state = ProxyState { router: Arc::new(router), client: build_client() };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, build_app(state, max_body)).await.unwrap() });
    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn health_answers_ok() {
    let proxy = serve_proxy(router_for(&["127.0.0.1:1"]), 1024).await;
    let resp = client().get(format!("{proxy}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn put_forwards_method_key_content_type_and_body_and_relays_the_answer() {
    let (shard, seen) = fake_shard("s0").await;
    let proxy = serve_proxy(router_for(&[&shard]), 64 * 1024 * 1024).await;

    let resp = client()
        .put(format!("{proxy}/kv/search-pagination:abc/1"))
        .header(CONTENT_TYPE, "application/json")
        .body(r#"{"ids":[1,2]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    assert_eq!(resp.headers()[CONTENT_TYPE], "application/json");
    // Only content-type and body come back: other shard headers are not relayed.
    assert!(resp.headers().get("x-shard").is_none());
    assert_eq!(resp.text().await.unwrap(), r#"{"ids":[1,2]}"#);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "PUT");
    assert_eq!(seen[0].1, "/kv/search-pagination:abc/1");
    assert_eq!(seen[0].2.as_deref(), Some("application/json"));
    assert_eq!(seen[0].3, br#"{"ids":[1,2]}"#);
}

#[tokio::test]
async fn get_without_body_sends_no_body_nor_content_type() {
    let (shard, seen) = fake_shard("s0").await;
    let proxy = serve_proxy(router_for(&[&shard]), 1024).await;

    let resp = client().get(format!("{proxy}/kv/plain")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get(CONTENT_TYPE).is_none());
    assert_eq!(resp.text().await.unwrap(), "raw");

    let resp = client().delete(format!("{proxy}/kv/teapot")).send().await.unwrap();
    assert_eq!(resp.status(), 418);
    assert_eq!(resp.text().await.unwrap(), "short and stout");

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0, "GET");
    assert_eq!(seen[0].2, None);
    assert!(seen[0].3.is_empty());
    assert_eq!(seen[1].0, "DELETE");
}

#[tokio::test]
async fn each_key_goes_to_the_shard_the_ring_assigns() {
    let (a, seen_a) = fake_shard("a").await;
    let (b, seen_b) = fake_shard("b").await;
    let router = router_for(&[&a, &b]);
    let expected: Vec<(String, String)> = (0..40)
        .map(|i| {
            let key = format!("user:{i}");
            let addr = router.get_key_address(key.as_bytes()).unwrap().address.clone();
            (key, addr)
        })
        .collect();
    let proxy = serve_proxy(router, 1024).await;

    for (key, _) in &expected {
        let resp = client().get(format!("{proxy}/kv/{key}")).send().await.unwrap();
        assert_eq!(resp.status(), 201);
    }
    let got_a: Vec<String> = seen_a.lock().unwrap().iter().map(|r| r.1.clone()).collect();
    let got_b: Vec<String> = seen_b.lock().unwrap().iter().map(|r| r.1.clone()).collect();
    for (key, addr) in &expected {
        let (mine, other) = if *addr == a { (&got_a, &got_b) } else { (&got_b, &got_a) };
        assert!(mine.contains(&format!("/kv/{key}")), "{key} should hit {addr}");
        assert!(!other.contains(&format!("/kv/{key}")));
    }
    // Both shards got traffic: the ring really spreads keys.
    assert!(!got_a.is_empty() && !got_b.is_empty());
}

#[tokio::test]
async fn bodies_above_the_2mb_axum_default_pass_and_above_the_limit_get_413() {
    let (shard, seen) = fake_shard("s0").await;
    let proxy = serve_proxy(router_for(&[&shard]), 3 * 1024 * 1024).await;

    let big = vec![b'x'; 2 * 1024 * 1024 + 10];
    let resp = client().put(format!("{proxy}/kv/big")).body(big.clone()).send().await.unwrap();
    assert_eq!(resp.status(), 201);
    assert_eq!(seen.lock().unwrap()[0].3.len(), big.len());

    let too_big = vec![b'x'; 3 * 1024 * 1024 + 1];
    let resp = client().put(format!("{proxy}/kv/big")).body(too_big).send().await.unwrap();
    assert_eq!(resp.status(), 413);
    assert_eq!(seen.lock().unwrap().len(), 1, "the oversize body never reaches the shard");
}

#[tokio::test]
async fn no_shards_configured_answers_502() {
    // SHARD_ADDRESSES with only separators → empty ring.
    let router = with_env(&[("SHARD_ADDRESSES", " , ")], build_router);
    assert_eq!(router.total_shards(), 0);
    let proxy = serve_proxy(router, 1024).await;
    let resp = client().get(format!("{proxy}/kv/k")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    assert_eq!(resp.text().await.unwrap(), "no shard available");
}

#[tokio::test]
async fn unreachable_shard_answers_502() {
    // Bind and drop: nothing listens on that port any more.
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    let proxy = serve_proxy(router_for(&[&dead]), 1024).await;
    let resp = client().get(format!("{proxy}/kv/k")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    assert!(resp.text().await.unwrap().starts_with("shard proxy error:"));
}

#[tokio::test]
async fn shard_dying_mid_body_answers_502_not_an_empty_200() {
    // Raw shard: promises 100 bytes, sends 3 and closes.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 100\r\n\r\nabc")
                .await;
            drop(sock);
        }
    });
    let proxy = serve_proxy(router_for(&[&addr]), 1024).await;
    let resp = client().get(format!("{proxy}/kv/k")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    assert!(resp.text().await.unwrap().starts_with("shard proxy error:"));
}

#[tokio::test]
async fn shard_slower_than_shard_timeout_answers_502() {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            // Accept and never answer.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                drop(sock);
            });
        }
    });
    let router = router_for(&[&addr]);
    let client_1s = with_env(&[("SHARD_TIMEOUT_SECS", "1")], build_client);
    let state = ProxyState { router: Arc::new(router), client: client_1s };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, build_app(state, 1024)).await.unwrap() });

    let started = std::time::Instant::now();
    let resp = client().get(format!("http://{paddr}/kv/k")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn shard_addresses_list_is_trimmed_and_indexed_in_order() {
    let r = with_env(&[("SHARD_ADDRESSES", " h0:1 , ,h1:2,"), ("VIRTUAL_NODES", "10")], build_router);
    assert_eq!(r.total_shards(), 2);
    assert_eq!(
        r.all_shards(),
        &[
            ShardAddress { shard_id: 0, address: "h0:1".into() },
            ShardAddress { shard_id: 1, address: "h1:2".into() },
        ]
    );
}

#[test]
fn without_shard_addresses_uses_the_statefulset_dns_template() {
    let r = with_env(
        &[
            ("SHARD_ADDRESSES", "   "),
            ("TOTAL_SHARDS", "3"),
            ("SHARD_SERVICE_NAME", "kv"),
            ("POD_NAMESPACE", "prod"),
            ("SHARD_PORT", "9000"),
        ],
        build_router,
    );
    let addrs: Vec<&str> = r.all_shards().iter().map(|s| s.address.as_str()).collect();
    assert_eq!(
        addrs,
        [
            "kv-0.kv.prod.svc.cluster.local:9000",
            "kv-1.kv.prod.svc.cluster.local:9000",
            "kv-2.kv.prod.svc.cluster.local:9000",
        ]
    );
}

#[test]
fn defaults_are_one_search_kv_shard_in_default_namespace() {
    let r = with_env(&[], build_router);
    assert_eq!(r.all_shards()[0].address, "search-kv-0.search-kv.default.svc.cluster.local:8080");
    assert_eq!(r.total_shards(), 1);
    // A value that does not parse falls back to the default instead of failing.
    let r = with_env(&[("TOTAL_SHARDS", "many")], build_router);
    assert_eq!(r.total_shards(), 1);
}

#[test]
fn routing_matches_the_shards_own_ring() {
    // The proxy must pick the same shard a search-kv shard's owns_key() accepts,
    // with the leading '/' axum captures stripped the same way.
    let proxy = with_env(&[("SHARD_ADDRESSES", "a:1,b:1,c:1")], build_router);
    for i in 0..200 {
        let key = normalize_key(format!("/feature:{i}"));
        let shard_id = proxy.get_shard(key.as_bytes());
        let shard = ShardRouter::new(ShardConfig {
            shard_id,
            total_shards: 3,
            virtual_nodes: 150,
            shard_addresses: proxy.all_shards().to_vec(),
        });
        assert!(shard.owns_key(key.as_bytes()));
    }
}
