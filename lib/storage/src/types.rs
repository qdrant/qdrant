use std::collections::HashMap;

use collection::config::WalConfig;
use collection::optimizers_builder::OptimizersConfig;
use collection::PeerId;
use schemars::JsonSchema;
use segment::types::HnswConfig;
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;

pub type PeerAddressById = HashMap<PeerId, Uri>;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
}

/// Global configuration of the storage, loaded on the service launch, default stored in ./config
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct StorageConfig {
    pub storage_path: String,
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    pub optimizers: OptimizersConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
    pub hnsw_index: HnswConfig,
}

fn default_on_disk_payload() -> bool {
    false
}

/// Information of a peer in the cluster
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PeerInfo {
    pub uri: String,
    // ToDo: How long ago was the last communication? In milliseconds
    // pub last_responded_millis: usize
}

/// Summary information about the current raft state
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct RaftInfo {
    /// Raft divides time into terms of arbitrary length, each beginning with an election.
    /// If a candidate wins the election, it remains the leader for the rest of the term.
    /// The term number increases monotonically.
    /// Each server stores the current term number which is also exchanged in every communication.
    pub term: u64,
    // ToDo: What does this mean?
    pub commit: u64,
    /// Number of consensus operations pending to be applied on this peer
    pub pending_operations: usize,
    // ToDo: Who is the current leader?
    // pub leader: u64
}

/// Description of enabled cluster
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct ClusterInfo {
    /// ID of this peer
    pub peer_id: PeerId,
    /// Peers composition of the cluster with main information
    pub peers: HashMap<PeerId, PeerInfo>,
    /// Status of the Raft consensus
    pub raft_info: RaftInfo,
}

/// Information about current cluster status and structure
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(tag = "status")]
#[serde(rename_all = "snake_case")]
pub enum ClusterStatus {
    Disabled,
    Enabled(ClusterInfo),
}
