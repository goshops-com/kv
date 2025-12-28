//! Sharding with Consistent Hashing
//!
//! Implements consistent hashing with virtual nodes for distributing
//! keys across multiple shards with minimal key movement when adding
//! or removing nodes.

use std::collections::BTreeMap;
use xxhash_rust::xxh3::xxh3_64;

/// Configuration for sharding
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// This node's shard ID
    pub shard_id: u32,
    /// Total number of shards in the cluster
    pub total_shards: u32,
    /// Number of virtual nodes per physical shard (affects distribution uniformity)
    pub virtual_nodes: u32,
    /// List of all shard addresses in the cluster
    pub shard_addresses: Vec<ShardAddress>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_id: 0,
            total_shards: 1,
            virtual_nodes: 150,
            shard_addresses: vec![ShardAddress {
                shard_id: 0,
                address: "localhost:8080".to_string(),
            }],
        }
    }
}

/// Address of a shard node
#[derive(Debug, Clone, PartialEq)]
pub struct ShardAddress {
    pub shard_id: u32,
    pub address: String,
}

/// A point on the consistent hash ring
#[derive(Debug, Clone)]
struct RingPoint {
    hash: u64,
    shard_id: u32,
}

/// Routes keys to shards using consistent hashing
pub struct ShardRouter {
    config: ShardConfig,
    /// The consistent hash ring (sorted by hash value)
    ring: BTreeMap<u64, u32>,
}

impl ShardRouter {
    /// Create a new shard router with the given configuration
    pub fn new(config: ShardConfig) -> Self {
        let mut ring = BTreeMap::new();

        // Add virtual nodes for each shard
        for shard_id in 0..config.total_shards {
            for vn in 0..config.virtual_nodes {
                let key = format!("shard-{}-vn-{}", shard_id, vn);
                let hash = xxh3_64(key.as_bytes());
                ring.insert(hash, shard_id);
            }
        }

        Self { config, ring }
    }

    /// Get the shard ID for a given key
    pub fn get_shard(&self, key: &[u8]) -> u32 {
        if self.ring.is_empty() {
            return 0;
        }

        let hash = xxh3_64(key);

        // Find the first point on the ring >= the key's hash
        // If none found, wrap around to the first point
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &shard_id)| shard_id)
            .unwrap_or(0)
    }

    /// Check if this node owns the given key
    pub fn owns_key(&self, key: &[u8]) -> bool {
        self.get_shard(key) == self.config.shard_id
    }

    /// Get the address for a given shard
    pub fn get_shard_address(&self, shard_id: u32) -> Option<&ShardAddress> {
        self.config
            .shard_addresses
            .iter()
            .find(|a| a.shard_id == shard_id)
    }

    /// Get the address for the shard that owns the given key
    pub fn get_key_address(&self, key: &[u8]) -> Option<&ShardAddress> {
        let shard_id = self.get_shard(key);
        self.get_shard_address(shard_id)
    }

    /// Get this node's shard ID
    pub fn local_shard_id(&self) -> u32 {
        self.config.shard_id
    }

    /// Get total number of shards
    pub fn total_shards(&self) -> u32 {
        self.config.total_shards
    }

    /// Get all shard addresses
    pub fn all_shards(&self) -> &[ShardAddress] {
        &self.config.shard_addresses
    }

    /// Get the distribution of keys across shards (for debugging/stats)
    /// Returns a map of shard_id -> percentage of ring owned
    pub fn ring_distribution(&self) -> Vec<(u32, f64)> {
        let mut counts: Vec<u32> = vec![0; self.config.total_shards as usize];

        for &shard_id in self.ring.values() {
            if (shard_id as usize) < counts.len() {
                counts[shard_id as usize] += 1;
            }
        }

        let total: u32 = counts.iter().sum();
        if total == 0 {
            return vec![];
        }

        counts
            .into_iter()
            .enumerate()
            .map(|(id, count)| (id as u32, count as f64 / total as f64 * 100.0))
            .collect()
    }

    /// Simulate key distribution across shards
    /// Useful for testing/validation
    pub fn simulate_distribution(&self, num_keys: usize) -> Vec<(u32, usize)> {
        let mut counts: Vec<usize> = vec![0; self.config.total_shards as usize];

        for i in 0..num_keys {
            let key = format!("test-key-{}", i);
            let shard = self.get_shard(key.as_bytes());
            if (shard as usize) < counts.len() {
                counts[shard as usize] += 1;
            }
        }

        counts
            .into_iter()
            .enumerate()
            .map(|(id, count)| (id as u32, count))
            .collect()
    }
}

/// Helper to create shard configs for a cluster
pub fn create_cluster_configs(
    total_shards: u32,
    base_port: u16,
    virtual_nodes: u32,
) -> Vec<ShardConfig> {
    let addresses: Vec<ShardAddress> = (0..total_shards)
        .map(|id| ShardAddress {
            shard_id: id,
            address: format!("localhost:{}", base_port + id as u16),
        })
        .collect();

    (0..total_shards)
        .map(|id| ShardConfig {
            shard_id: id,
            total_shards,
            virtual_nodes,
            shard_addresses: addresses.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_router(total_shards: u32, shard_id: u32) -> ShardRouter {
        let configs = create_cluster_configs(total_shards, 8080, 150);
        ShardRouter::new(configs[shard_id as usize].clone())
    }

    // ==================== BASIC ROUTING ====================

    #[test]
    fn test_single_shard_routes_all_keys() {
        let router = create_test_router(1, 0);

        // All keys should route to shard 0
        for i in 0..100 {
            let key = format!("key-{}", i);
            assert_eq!(router.get_shard(key.as_bytes()), 0);
        }
    }

    #[test]
    fn test_consistent_hashing_is_deterministic() {
        let router = create_test_router(3, 0);

        // Same key should always route to same shard
        let key = b"my-test-key";
        let shard1 = router.get_shard(key);
        let shard2 = router.get_shard(key);
        let shard3 = router.get_shard(key);

        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_different_keys_distribute_across_shards() {
        let router = create_test_router(3, 0);

        // Generate many keys and check distribution
        let distribution = router.simulate_distribution(10000);

        // Each shard should get roughly 1/3 of keys (with some variance)
        for (_shard, count) in distribution {
            // Allow 20% variance from expected 3333
            assert!(count > 2500, "Shard got too few keys: {}", count);
            assert!(count < 4500, "Shard got too many keys: {}", count);
        }
    }

    #[test]
    fn test_owns_key() {
        let router = create_test_router(3, 0);

        let mut owned_count = 0;
        for i in 0..1000 {
            let key = format!("key-{}", i);
            if router.owns_key(key.as_bytes()) {
                owned_count += 1;
                assert_eq!(router.get_shard(key.as_bytes()), 0);
            }
        }

        // Should own roughly 1/3 of keys
        assert!(owned_count > 250, "Owned too few keys: {}", owned_count);
        assert!(owned_count < 450, "Owned too many keys: {}", owned_count);
    }

    // ==================== SHARD ADDRESSES ====================

    #[test]
    fn test_get_shard_address() {
        let router = create_test_router(3, 0);

        let addr0 = router.get_shard_address(0).unwrap();
        assert_eq!(addr0.shard_id, 0);
        assert_eq!(addr0.address, "localhost:8080");

        let addr1 = router.get_shard_address(1).unwrap();
        assert_eq!(addr1.shard_id, 1);
        assert_eq!(addr1.address, "localhost:8081");

        let addr2 = router.get_shard_address(2).unwrap();
        assert_eq!(addr2.shard_id, 2);
        assert_eq!(addr2.address, "localhost:8082");
    }

    #[test]
    fn test_get_shard_address_invalid() {
        let router = create_test_router(3, 0);
        assert!(router.get_shard_address(99).is_none());
    }

    #[test]
    fn test_get_key_address() {
        let router = create_test_router(3, 0);

        let key = b"my-key";
        let shard_id = router.get_shard(key);
        let addr = router.get_key_address(key).unwrap();

        assert_eq!(addr.shard_id, shard_id);
    }

    // ==================== CLUSTER INFO ====================

    #[test]
    fn test_local_shard_id() {
        let router0 = create_test_router(3, 0);
        let router1 = create_test_router(3, 1);
        let router2 = create_test_router(3, 2);

        assert_eq!(router0.local_shard_id(), 0);
        assert_eq!(router1.local_shard_id(), 1);
        assert_eq!(router2.local_shard_id(), 2);
    }

    #[test]
    fn test_total_shards() {
        let router = create_test_router(5, 0);
        assert_eq!(router.total_shards(), 5);
    }

    #[test]
    fn test_all_shards() {
        let router = create_test_router(3, 0);
        let shards = router.all_shards();

        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].shard_id, 0);
        assert_eq!(shards[1].shard_id, 1);
        assert_eq!(shards[2].shard_id, 2);
    }

    // ==================== RING DISTRIBUTION ====================

    #[test]
    fn test_ring_distribution() {
        let router = create_test_router(3, 0);
        let distribution = router.ring_distribution();

        assert_eq!(distribution.len(), 3);

        // Each shard should have roughly 33% of the ring
        for (_, percent) in distribution {
            assert!(percent > 25.0, "Shard has too little ring: {}%", percent);
            assert!(percent < 45.0, "Shard has too much ring: {}%", percent);
        }
    }

    // ==================== VIRTUAL NODES ====================

    #[test]
    fn test_more_virtual_nodes_improves_distribution() {
        // With few virtual nodes, distribution is less uniform
        let configs_low = create_cluster_configs(3, 8080, 10);
        let router_low = ShardRouter::new(configs_low[0].clone());
        let dist_low = router_low.simulate_distribution(10000);

        // With many virtual nodes, distribution is more uniform
        let configs_high = create_cluster_configs(3, 8080, 200);
        let router_high = ShardRouter::new(configs_high[0].clone());
        let dist_high = router_high.simulate_distribution(10000);

        // Calculate variance
        let expected = 10000.0 / 3.0;

        let variance_low: f64 = dist_low
            .iter()
            .map(|(_, count)| (*count as f64 - expected).powi(2))
            .sum::<f64>()
            / 3.0;

        let variance_high: f64 = dist_high
            .iter()
            .map(|(_, count)| (*count as f64 - expected).powi(2))
            .sum::<f64>()
            / 3.0;

        // Higher virtual nodes should result in lower variance
        assert!(
            variance_high < variance_low,
            "Expected lower variance with more virtual nodes: low={}, high={}",
            variance_low,
            variance_high
        );
    }

    // ==================== CONSISTENCY ====================

    #[test]
    fn test_same_routing_across_routers() {
        // All routers in a cluster should route keys the same way
        let router0 = create_test_router(3, 0);
        let router1 = create_test_router(3, 1);
        let router2 = create_test_router(3, 2);

        for i in 0..100 {
            let key = format!("key-{}", i);
            let shard0 = router0.get_shard(key.as_bytes());
            let shard1 = router1.get_shard(key.as_bytes());
            let shard2 = router2.get_shard(key.as_bytes());

            assert_eq!(shard0, shard1);
            assert_eq!(shard1, shard2);
        }
    }
}
