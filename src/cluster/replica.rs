//! Async Replication
//!
//! Implements asynchronous replication between replica nodes.
//! Uses a log-based approach with sequence numbers for ordering.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Replica not found: {0}")]
    ReplicaNotFound(String),
    #[error("Sequence gap detected: expected {expected}, got {got}")]
    SequenceGap { expected: u64, got: u64 },
}

/// Configuration for replication
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// This node's replica ID
    pub replica_id: u32,
    /// Whether this is the primary replica
    pub is_primary: bool,
    /// List of peer replica addresses
    pub peers: Vec<ReplicaPeer>,
    /// Maximum replication lag in milliseconds before alerting
    pub max_lag_ms: u64,
    /// Maximum number of entries to keep in the replication log
    pub max_log_entries: usize,
    /// Batch size for replication
    pub batch_size: usize,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            replica_id: 0,
            is_primary: true,
            peers: vec![],
            max_lag_ms: 1000,
            max_log_entries: 10000,
            batch_size: 100,
        }
    }
}

/// A peer replica
#[derive(Debug, Clone)]
pub struct ReplicaPeer {
    pub replica_id: u32,
    pub address: String,
}

/// An entry in the replication log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    /// Sequence number (monotonically increasing)
    pub sequence: u64,
    /// Operation type
    pub operation: ReplicationOp,
    /// Timestamp when the operation occurred
    pub timestamp: DateTime<Utc>,
    /// Source replica ID
    pub source_replica: u32,
}

/// Types of operations that get replicated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// State of a peer replica
#[derive(Debug, Clone)]
pub struct PeerState {
    pub replica_id: u32,
    pub last_acked_sequence: u64,
    pub last_sync_time: DateTime<Utc>,
    pub is_online: bool,
}

/// Replication statistics
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
    pub current_sequence: u64,
    pub log_size: usize,
    pub pending_entries: usize,
    pub peers_online: usize,
    pub peers_total: usize,
    pub max_lag_sequence: u64,
}

/// Manages async replication to peer nodes
pub struct ReplicaManager {
    config: ReplicaConfig,
    /// Current sequence number
    sequence: AtomicU64,
    /// Replication log
    log: RwLock<VecDeque<ReplicationEntry>>,
    /// State of each peer
    peer_states: RwLock<HashMap<u32, PeerState>>,
    /// Entries pending replication (for incoming replicas)
    pending_apply: RwLock<VecDeque<ReplicationEntry>>,
}

impl ReplicaManager {
    /// Create a new replica manager
    pub fn new(config: ReplicaConfig) -> Self {
        let mut peer_states = HashMap::new();

        for peer in &config.peers {
            peer_states.insert(
                peer.replica_id,
                PeerState {
                    replica_id: peer.replica_id,
                    last_acked_sequence: 0,
                    last_sync_time: Utc::now(),
                    is_online: false,
                },
            );
        }

        Self {
            config,
            sequence: AtomicU64::new(0),
            log: RwLock::new(VecDeque::new()),
            peer_states: RwLock::new(peer_states),
            pending_apply: RwLock::new(VecDeque::new()),
        }
    }

    /// Record a write operation to the replication log
    pub fn record_put(&self, key: &[u8], value: &[u8]) -> u64 {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = ReplicationEntry {
            sequence: seq,
            operation: ReplicationOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            timestamp: Utc::now(),
            source_replica: self.config.replica_id,
        };

        self.append_to_log(entry);
        seq
    }

    /// Record a delete operation to the replication log
    pub fn record_delete(&self, key: &[u8]) -> u64 {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = ReplicationEntry {
            sequence: seq,
            operation: ReplicationOp::Delete { key: key.to_vec() },
            timestamp: Utc::now(),
            source_replica: self.config.replica_id,
        };

        self.append_to_log(entry);
        seq
    }

    fn append_to_log(&self, entry: ReplicationEntry) {
        let mut log = self.log.write();
        log.push_back(entry);

        // Trim log if too large
        while log.len() > self.config.max_log_entries {
            log.pop_front();
        }
    }

    /// Get entries from the log starting at a given sequence
    pub fn get_entries_since(&self, since_sequence: u64) -> Vec<ReplicationEntry> {
        let log = self.log.read();

        log.iter()
            .filter(|e| e.sequence > since_sequence)
            .take(self.config.batch_size)
            .cloned()
            .collect()
    }

    /// Get all entries in the log
    pub fn get_all_entries(&self) -> Vec<ReplicationEntry> {
        let log = self.log.read();
        log.iter().cloned().collect()
    }

    /// Apply entries received from another replica
    pub fn receive_entries(&self, entries: Vec<ReplicationEntry>) -> Result<u64, ReplicationError> {
        if entries.is_empty() {
            return Ok(self.current_sequence());
        }

        let mut pending = self.pending_apply.write();

        for entry in entries {
            pending.push_back(entry);
        }

        // Sort by sequence
        pending.make_contiguous().sort_by_key(|e| e.sequence);

        // Return the highest sequence we now have
        Ok(pending.back().map(|e| e.sequence).unwrap_or(0))
    }

    /// Get pending entries to apply (for the engine to consume)
    pub fn drain_pending(&self) -> Vec<ReplicationEntry> {
        let mut pending = self.pending_apply.write();
        pending.drain(..).collect()
    }

    /// Update peer state after successful sync
    pub fn ack_peer(&self, replica_id: u32, sequence: u64) {
        let mut states = self.peer_states.write();
        if let Some(state) = states.get_mut(&replica_id) {
            state.last_acked_sequence = sequence;
            state.last_sync_time = Utc::now();
            state.is_online = true;
        }
    }

    /// Mark a peer as offline
    pub fn mark_peer_offline(&self, replica_id: u32) {
        let mut states = self.peer_states.write();
        if let Some(state) = states.get_mut(&replica_id) {
            state.is_online = false;
        }
    }

    /// Get the current sequence number
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get this node's replica ID
    pub fn replica_id(&self) -> u32 {
        self.config.replica_id
    }

    /// Check if this is the primary replica
    pub fn is_primary(&self) -> bool {
        self.config.is_primary
    }

    /// Get all peer states
    pub fn peer_states(&self) -> Vec<PeerState> {
        let states = self.peer_states.read();
        states.values().cloned().collect()
    }

    /// Get replication statistics
    pub fn stats(&self) -> ReplicationStats {
        let log = self.log.read();
        let states = self.peer_states.read();
        let pending = self.pending_apply.read();

        let current_seq = self.current_sequence();
        let min_acked = states
            .values()
            .map(|s| s.last_acked_sequence)
            .min()
            .unwrap_or(current_seq);

        ReplicationStats {
            current_sequence: current_seq,
            log_size: log.len(),
            pending_entries: pending.len(),
            peers_online: states.values().filter(|s| s.is_online).count(),
            peers_total: states.len(),
            max_lag_sequence: current_seq.saturating_sub(min_acked),
        }
    }

    /// Check if replication lag is within acceptable bounds
    pub fn is_healthy(&self) -> bool {
        let stats = self.stats();

        // All peers should be online
        if stats.peers_online < stats.peers_total {
            return false;
        }

        // Check lag is not too high
        // (in a real system, this would also check time-based lag)
        stats.max_lag_sequence < 1000
    }

    /// Get the minimum acked sequence across all peers
    pub fn min_acked_sequence(&self) -> u64 {
        let states = self.peer_states.read();
        states
            .values()
            .map(|s| s.last_acked_sequence)
            .min()
            .unwrap_or(0)
    }

    /// Truncate log entries that have been acked by all peers
    pub fn truncate_acked(&self) {
        let min_acked = self.min_acked_sequence();
        let mut log = self.log.write();

        while let Some(entry) = log.front() {
            if entry.sequence <= min_acked {
                log.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager(replica_id: u32, is_primary: bool) -> ReplicaManager {
        let config = ReplicaConfig {
            replica_id,
            is_primary,
            peers: vec![
                ReplicaPeer {
                    replica_id: 1,
                    address: "localhost:8081".to_string(),
                },
                ReplicaPeer {
                    replica_id: 2,
                    address: "localhost:8082".to_string(),
                },
            ],
            max_lag_ms: 1000,
            max_log_entries: 100,
            batch_size: 10,
        };
        ReplicaManager::new(config)
    }

    // ==================== BASIC OPERATIONS ====================

    #[test]
    fn test_new_manager_starts_at_sequence_zero() {
        let manager = create_test_manager(0, true);
        assert_eq!(manager.current_sequence(), 0);
    }

    #[test]
    fn test_record_put_increments_sequence() {
        let manager = create_test_manager(0, true);

        let seq1 = manager.record_put(b"key1", b"value1");
        assert_eq!(seq1, 1);

        let seq2 = manager.record_put(b"key2", b"value2");
        assert_eq!(seq2, 2);

        assert_eq!(manager.current_sequence(), 2);
    }

    #[test]
    fn test_record_delete_increments_sequence() {
        let manager = create_test_manager(0, true);

        manager.record_put(b"key1", b"value1");
        let seq = manager.record_delete(b"key1");

        assert_eq!(seq, 2);
    }

    #[test]
    fn test_entries_are_added_to_log() {
        let manager = create_test_manager(0, true);

        manager.record_put(b"key1", b"value1");
        manager.record_put(b"key2", b"value2");
        manager.record_delete(b"key1");

        let entries = manager.get_all_entries();
        assert_eq!(entries.len(), 3);
    }

    // ==================== LOG OPERATIONS ====================

    #[test]
    fn test_get_entries_since() {
        let manager = create_test_manager(0, true);

        for i in 0..5 {
            manager.record_put(format!("key{}", i).as_bytes(), b"value");
        }

        let entries = manager.get_entries_since(2);
        assert_eq!(entries.len(), 3); // sequences 3, 4, 5
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[2].sequence, 5);
    }

    #[test]
    fn test_get_entries_since_respects_batch_size() {
        let config = ReplicaConfig {
            batch_size: 3,
            ..Default::default()
        };
        let manager = ReplicaManager::new(config);

        for i in 0..10 {
            manager.record_put(format!("key{}", i).as_bytes(), b"value");
        }

        let entries = manager.get_entries_since(0);
        assert_eq!(entries.len(), 3); // Limited by batch_size
    }

    #[test]
    fn test_log_truncation_on_size_limit() {
        let config = ReplicaConfig {
            max_log_entries: 5,
            ..Default::default()
        };
        let manager = ReplicaManager::new(config);

        for i in 0..10 {
            manager.record_put(format!("key{}", i).as_bytes(), b"value");
        }

        let entries = manager.get_all_entries();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].sequence, 6); // Oldest should be trimmed
    }

    // ==================== PEER STATE ====================

    #[test]
    fn test_peer_states_initialized() {
        let manager = create_test_manager(0, true);
        let states = manager.peer_states();

        assert_eq!(states.len(), 2);
    }

    #[test]
    fn test_ack_peer_updates_state() {
        let manager = create_test_manager(0, true);

        manager.record_put(b"key1", b"value1");
        manager.ack_peer(1, 1);

        let states = manager.peer_states();
        let peer1 = states.iter().find(|s| s.replica_id == 1).unwrap();

        assert_eq!(peer1.last_acked_sequence, 1);
        assert!(peer1.is_online);
    }

    #[test]
    fn test_mark_peer_offline() {
        let manager = create_test_manager(0, true);

        manager.ack_peer(1, 0); // First mark online
        manager.mark_peer_offline(1);

        let states = manager.peer_states();
        let peer1 = states.iter().find(|s| s.replica_id == 1).unwrap();

        assert!(!peer1.is_online);
    }

    // ==================== RECEIVING ENTRIES ====================

    #[test]
    fn test_receive_entries() {
        let manager = create_test_manager(1, false);

        let entries = vec![
            ReplicationEntry {
                sequence: 1,
                operation: ReplicationOp::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                },
                timestamp: Utc::now(),
                source_replica: 0,
            },
            ReplicationEntry {
                sequence: 2,
                operation: ReplicationOp::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                },
                timestamp: Utc::now(),
                source_replica: 0,
            },
        ];

        let result = manager.receive_entries(entries).unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_drain_pending() {
        let manager = create_test_manager(1, false);

        let entries = vec![ReplicationEntry {
            sequence: 1,
            operation: ReplicationOp::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            },
            timestamp: Utc::now(),
            source_replica: 0,
        }];

        manager.receive_entries(entries).unwrap();

        let pending = manager.drain_pending();
        assert_eq!(pending.len(), 1);

        // Second drain should be empty
        let pending2 = manager.drain_pending();
        assert!(pending2.is_empty());
    }

    // ==================== STATISTICS ====================

    #[test]
    fn test_stats() {
        let manager = create_test_manager(0, true);

        manager.record_put(b"key1", b"value1");
        manager.record_put(b"key2", b"value2");
        manager.ack_peer(1, 1);

        let stats = manager.stats();

        assert_eq!(stats.current_sequence, 2);
        assert_eq!(stats.log_size, 2);
        assert_eq!(stats.peers_total, 2);
        assert_eq!(stats.peers_online, 1);
        assert_eq!(stats.max_lag_sequence, 2); // peer 2 hasn't acked anything
    }

    #[test]
    fn test_min_acked_sequence() {
        let manager = create_test_manager(0, true);

        manager.record_put(b"key1", b"value1");
        manager.record_put(b"key2", b"value2");

        manager.ack_peer(1, 2);
        manager.ack_peer(2, 1);

        assert_eq!(manager.min_acked_sequence(), 1);
    }

    #[test]
    fn test_truncate_acked() {
        let manager = create_test_manager(0, true);

        for i in 0..5 {
            manager.record_put(format!("key{}", i).as_bytes(), b"value");
        }

        manager.ack_peer(1, 3);
        manager.ack_peer(2, 3);

        manager.truncate_acked();

        let entries = manager.get_all_entries();
        assert_eq!(entries.len(), 2); // Only sequences 4 and 5 remain
        assert_eq!(entries[0].sequence, 4);
    }

    // ==================== HEALTH CHECK ====================

    #[test]
    fn test_is_healthy_when_all_peers_online() {
        let manager = create_test_manager(0, true);

        manager.ack_peer(1, 0);
        manager.ack_peer(2, 0);

        assert!(manager.is_healthy());
    }

    #[test]
    fn test_is_not_healthy_when_peer_offline() {
        let manager = create_test_manager(0, true);

        manager.ack_peer(1, 0);
        // peer 2 is still offline (never acked)

        assert!(!manager.is_healthy());
    }

    // ==================== IDENTITY ====================

    #[test]
    fn test_replica_id() {
        let manager = create_test_manager(5, true);
        assert_eq!(manager.replica_id(), 5);
    }

    #[test]
    fn test_is_primary() {
        let primary = create_test_manager(0, true);
        let replica = create_test_manager(1, false);

        assert!(primary.is_primary());
        assert!(!replica.is_primary());
    }
}
