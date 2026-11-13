//! The real binary: reads its env, logs the ring it built and binds HTTP_PORT.
//! It is never left running: the port is taken on purpose, so it exits on bind.

use std::process::Command;

fn run_on_taken_port(rust_log: Option<&str>) {
    let taken = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = taken.local_addr().unwrap().port();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_search-kv-proxy"));
    // Not env_clear(): cargo llvm-cov passes LLVM_PROFILE_FILE through the env.
    cmd.env_remove("TOTAL_SHARDS")
        .env_remove("MAX_BODY_BYTES")
        .env("HTTP_PORT", port.to_string())
        .env("SHARD_ADDRESSES", "10.0.0.1:8080,10.0.0.2:8080");
    match rust_log {
        Some(level) => cmd.env("RUST_LOG", level),
        None => cmd.env_remove("RUST_LOG"),
    };
    let out = cmd.output().unwrap();

    assert!(!out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("routing across 2 shard(s)"), "{stdout}");
    assert!(stdout.contains("shard[0] -> 10.0.0.1:8080"), "{stdout}");
    assert!(stdout.contains("shard[1] -> 10.0.0.2:8080"), "{stdout}");
    assert!(stderr.contains(&format!("bind 0.0.0.0:{port} failed")), "{stderr}");
}

#[test]
fn logs_the_ring_and_fails_loudly_when_the_port_is_taken() {
    run_on_taken_port(Some("info"));
}

#[test]
fn without_rust_log_it_still_logs_at_info() {
    run_on_taken_port(None);
}
