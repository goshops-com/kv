mod replica;

// The consistent-hash ring lives in the standalone `shard-router` crate so the
// search-kv-proxy can reuse the exact same code without pulling in rocksdb/engine.
pub use shard_router::{ShardRouter, ShardConfig, ShardAddress, create_cluster_configs, normalize_key};
pub use replica::{ReplicaManager, ReplicaConfig};
