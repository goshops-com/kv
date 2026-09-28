# TieredKV

A high-performance, tiered key-value store written in Rust with automatic data lifecycle management across memory, disk, and object storage.

## Features

- **Three-Tier Storage Architecture**
  - **L1 Memory**: LRU cache for hot data with configurable size limits
  - **L2 Disk**: Persistent LSM-tree storage using [sled](https://github.com/spacejam/sled)
  - **L3 Object Storage**: S3-compatible cold storage for archived data

- **Automatic Data Tiering**
  - Data automatically migrates from disk to object storage based on age or disk pressure
  - Hot data is promoted back to cache on access

- **Horizontal Scalability**
  - Consistent hashing with virtual nodes for even key distribution
  - Easy to add/remove shards with minimal data movement

- **High Availability**
  - Async replication with configurable consistency
  - Kubernetes-native deployment with StatefulSets

- **Simple HTTP API**
  - RESTful interface for all operations
  - Health checks and metrics endpoints

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Layer                           │
│   GET/PUT/DELETE /kv/:key  •  /health  •  /stats  •  /metrics    │
├─────────────────────────────────────────────────────────────────┤
│                        Tiered Engine                             │
│                                                                  │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐     │
│   │   Memory    │    │    Disk     │    │ Object Storage  │     │
│   │   (L1)      │───▶│    (L2)     │───▶│     (L3)        │     │
│   │  LRU Cache  │    │  LSM Tree   │    │   S3/MinIO      │     │
│   │  ~256MB     │    │   ~50GB     │    │   Unlimited     │     │
│   └─────────────┘    └─────────────┘    └─────────────────┘     │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│     Sharding (Consistent Hash)    │    Replication (Async)      │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Run Locally

```bash
# Clone the repository
git clone https://github.com/goshops-com/kv.git
cd kv

# Build and run
cargo run --release

# The server starts on http://localhost:8080
```

### Basic Usage

```bash
# Health check
curl http://localhost:8080/health

# Store a value
curl -X PUT http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": "Hello, World!"}'

# Retrieve a value
curl http://localhost:8080/kv/mykey

# Delete a value
curl -X DELETE http://localhost:8080/kv/mykey

# Check if key exists
curl -I http://localhost:8080/kv/mykey

# Get statistics
curl http://localhost:8080/stats
```

### Configuration

Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | `8080` | HTTP server port |
| `DATA_DIR` | `./data` | Disk storage directory |
| `MEMORY_MAX_SIZE_MB` | `256` | Max memory cache size |
| `MEMORY_MAX_ENTRIES` | `100000` | Max cached entries |
| `DISK_MAX_SIZE_GB` | `50` | Max disk storage size |
| `DISK_MIGRATION_AGE_HOURS` | `24` | Age before migrating to S3 |
| `S3_BUCKET` | `tieredkv-data` | S3 bucket name |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ENDPOINT` | - | Custom S3 endpoint (for MinIO) |
| `CACHE_ONLY` | `false` | Disable migration to / reads from object storage. Forced on when no object-storage credentials are set |
| `TTL_RULES` | - | Prefix TTLs, e.g. `CAD/:7d,tmp/:1h` (s/m/h/d). Changing them rebuilds the expiry index once on startup |
| `TTL_CLEANUP_INTERVAL_SECS` | `60` | How often the TTL sweep runs |
| `MAX_CONCURRENT_REQUESTS` | `128` | In-flight limit for `/kv`, `/admin`, `/debug` (not `/health`, `/stats`, `/metrics`) |
| `FLUSH_EVERY_N_WRITES` | `0` | Force a memtable flush every N writes (0 = never; the WAL is durable) |
| `RESERIALIZE_ON_STARTUP` | `false` | Convert legacy JSON entries to MessagePack on startup |
| `ROCKSDB_BLOCK_CACHE_MB` | `256` | Block cache shared by all column families |
| `ROCKSDB_CACHE_INDEX_AND_FILTER` | `false` | Keep index/filter blocks inside the block cache (bounded RAM) |
| `ROCKSDB_WRITE_BUFFER_MB` / `ROCKSDB_MAX_WRITE_BUFFERS` / `ROCKSDB_DB_WRITE_BUFFER_MB` | `64` / `3` / `256` | Memtable sizing |
| `ROCKSDB_MAX_BACKGROUND_JOBS` | `2` | Flush/compaction threads |
| `ROCKSDB_MAX_OPEN_FILES` | `-1` | SST readers kept open |
| `ROCKSDB_BLOB_COMPRESSION` | `none` | `none` or `zstd`. Values arrive zstd-compressed already |
| `ROCKSDB_BLOB_GC_AGE_CUTOFF` | `0.05` | Share of oldest blob files relocated on every compaction (`0` = no relocation) |
| `ROCKSDB_BLOB_GC_FORCE_THRESHOLD` | `0.5` | Garbage ratio that forces collection of the oldest blob files |
| `_RJEM_MALLOC_CONF` | - | jemalloc tuning, e.g. `background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000` |
| `RUST_LOG` | `info` | Log level |

### Expiry (TTL)

A key expires at the earlier of its per-key `ttl` (from its last write) and the first
matching `TTL_RULES` prefix (from creation). Expiry is enforced on read, so an expired key
is never served, and a sweep deletes expired entries every `TTL_CLEANUP_INTERVAL_SECS`.

Storage uses RocksDB column families so the sweep never reads values:

| Column family | Contents |
|---------------|----------|
| `default` | key → entry (value + timestamps + per-key TTL); values > 4KB in blob files |
| `meta` | key → expiry + write version (one per key; also answers existence checks) |
| `expiry` | `[expires_at BE][key]` → empty; the sweep range-scans only the due front |
| `sys` | internal state (backfill progress) |

Data written before the index existed is indexed by a one-time, resumable backfill on
startup (`kv_expiry_backfill_complete` turns 1 when done).

### Data format and rollback

Entries are written in format v2 (msgpack with the value as `bin`). v1 and legacy JSON
entries are still read. A binary older than the expiry index cannot open a data dir that
has the extra column families. To roll back, stop the pod and run, against the same volume:

```bash
DATA_DIR=/data tieredkv downgrade   # rewrites v2 entries as v1, drops meta/expiry/sys
```

## Kubernetes Deployment

### Manifests

Both production deployments run the same image:

| Deployment | Manifests | Callers |
|------------|-----------|---------|
| `tieredkv` (3 shards) | `k8s/statefulset.yaml`, `k8s/configmap.yaml`, `k8s/service.yaml` | `feature-store-v2`, which shards client-side over per-pod DNS |
| `search-kv` (2 shards, cache-only) | `k8s/search-kv/` | `search-kv-proxy` (consistent hash) |

```bash
# ConfigMaps and Services
kubectl apply -f k8s/configmap.yaml -f k8s/search-kv/configmap.yaml

# StatefulSets: use replace, not apply. apply's 3-way merge keeps env vars that were
# added to the live object outside these files.
kubectl replace -f k8s/statefulset.yaml
kubectl replace -f k8s/search-kv/statefulset.yaml

kubectl rollout status sts/tieredkv
```

Changing the number of shards moves keys between them; it is a resharding, not a scale-out.
Restarting a shard makes its keys unavailable for ~60-90s (pod termination plus the
negative DNS cache on its per-pod name).

### Architecture in Kubernetes

```
                    ┌──────────────────┐
                    │   Load Balancer  │
                    │   (Service)      │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│  tieredkv-0   │  │  tieredkv-1   │  │  tieredkv-2   │
│   (Shard 0)   │  │   (Shard 1)   │  │   (Shard 2)   │
│               │  │               │  │               │
│  ┌─────────┐  │  │  ┌─────────┐  │  │  ┌─────────┐  │
│  │   PVC   │  │  │  │   PVC   │  │  │  │   PVC   │  │
│  │  100Gi  │  │  │  │  100Gi  │  │  │  │  100Gi  │  │
│  └─────────┘  │  │  └─────────┘  │  │  └─────────┘  │
└───────────────┘  └───────────────┘  └───────────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   S3 / MinIO     │
                    │  (Cold Storage)  │
                    └──────────────────┘
```

## API Reference

### Key-Value Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/kv/:key` | Get value by key |
| `PUT` | `/kv/:key` | Store a value |
| `DELETE` | `/kv/:key` | Delete a value |
| `HEAD` | `/kv/:key` | Check if key exists |

### Administrative

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/stats` | Engine statistics |
| `GET` | `/metrics` | Prometheus metrics: latency by route/method/status, GET outcome by tier, TTL sweep, backfill, L1 cache and RocksDB properties |
| `GET` | `/debug/profile?seconds=N` | CPU flamegraph (SVG), max 60s |
| `POST` | `/admin/migrate` | Trigger migration |
| `POST` | `/admin/flush` | Flush to disk |

### Response Examples

**GET /kv/:key**
```json
{
  "key": "mykey",
  "value": "Hello, World!",
  "tier": "memory"
}
```

**GET /stats**
```json
{
  "memory_entries": 1523,
  "memory_size_bytes": 2456789,
  "memory_hit_rate": 0.847,
  "disk_entries": 45230,
  "disk_size_bytes": 1234567890,
  "disk_usage_percent": 12.5,
  "migrations_completed": 156
}
```

## Data Flow

### Write Path
```
Client Request
      │
      ▼
┌─────────────┐
│  Write to   │◄── Durability guarantee
│    Disk     │
└─────────────┘
      │
      ▼
┌─────────────┐
│  Update     │◄── Fast subsequent reads
│   Cache     │
└─────────────┘
      │
      ▼
   Response
```

### Read Path
```
Client Request
      │
      ▼
┌─────────────┐
│   Check     │──── Hit (not expired) ───▶ Return
│   Cache     │
└─────────────┘
      │ Miss
      ▼
┌─────────────┐
│   Check     │──── Hit (not expired) ───▶ Promote to Cache ──▶ Return
│    Disk     │
└─────────────┘
      │ Miss (skipped in cache-only mode)
      ▼
┌─────────────┐
│   Check     │──── Hit ───▶ Promote to Cache ──▶ Return
│     S3      │
└─────────────┘
      │ Miss
      ▼
   Not Found
```

## Development

### Prerequisites

- Rust 1.75+
- Docker (optional, for containerized deployment)

### Build

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run
```

### Project Structure

```
tieredkv/
├── src/
│   ├── memory/       # L1 LRU cache
│   ├── disk/         # L2 sled-based storage
│   ├── object/       # L3 S3-compatible storage
│   ├── engine/       # Tiered engine coordinator
│   ├── api/          # HTTP handlers and router
│   ├── cluster/      # Sharding and replication
│   ├── lib.rs
│   └── main.rs
├── k8s/              # Kubernetes manifests
├── Cargo.toml
├── Dockerfile
└── README.md
```

### Running Tests

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test memory
cargo test disk
cargo test engine

# Run with output
cargo test -- --nocapture
```

## Performance Considerations

- **Memory Cache**: 16 independently locked LRU shards (caches >= 64MB); values held compressed, as exact-size allocations
- **Disk Storage**: Millisecond reads with LSM-tree optimization; bloom filters for absent keys
- **TTL sweep**: proportional to what expired, not to the size of the store (measured on search-kv, 6.3M keys: 0.27 → 0.03 cores, 3.7 → 0.02 MB/s disk reads)
- **Object Storage**: Higher latency, used for cold/archived data
- **Consistent Hashing**: O(log n) shard lookup with 150 virtual nodes per shard

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
