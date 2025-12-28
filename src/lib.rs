//! TieredKV - A tiered key-value store with memory, disk, and object storage layers
//!
//! This crate provides a high-performance key-value store that automatically
//! manages data across three storage tiers:
//!
//! 1. **Memory (L1)**: Fast LRU cache with configurable size
//! 2. **Disk (L2)**: Persistent local storage using LSM-tree
//! 3. **Object Storage (L3)**: Cold storage in S3-compatible backends
//!
//! ## Features
//!
//! - Automatic tiered data management
//! - HTTP API for access
//! - Horizontal scaling via sharding
//! - Async replication for high availability

pub mod memory;
pub mod disk;
pub mod object;
pub mod engine;
pub mod api;
pub mod cluster;

// Re-export commonly used types
pub use memory::{MemoryCache, CacheConfig};
