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
│            GET/PUT/DELETE /kv/:key  •  /health  •  /stats        │
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
| `RUST_LOG` | `info` | Log level |

## Kubernetes Deployment

### Deploy with Kustomize

```bash
# Deploy 3 shards
kubectl apply -k k8s/

# Check status
kubectl get pods -l app=tieredkv

# Scale to 5 shards
kubectl scale statefulset tieredkv --replicas=5
```

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
│   Check     │──── Hit ───▶ Return
│   Cache     │
└─────────────┘
      │ Miss
      ▼
┌─────────────┐
│   Check     │──── Hit ───▶ Promote to Cache ──▶ Return
│    Disk     │
└─────────────┘
      │ Miss
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

- **Memory Cache**: Sub-microsecond reads for cached data
- **Disk Storage**: Millisecond reads with LSM-tree optimization
- **Object Storage**: Higher latency, used for cold/archived data
- **Consistent Hashing**: O(log n) shard lookup with 150 virtual nodes per shard

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
