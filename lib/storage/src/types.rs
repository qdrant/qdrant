use std::collections::HashMap;

use chrono::{DateTime, Utc};
use collection::config::WalConfig;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::shard::PeerId;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::madvise;
use segment::types::HnswConfig;
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;

pub type PeerAddressById = HashMap<PeerId, Uri>;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
}

/// Global configuration of the storage, loaded on the service launch, default stored in ./config
#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub storage_path: String,
    #[serde(default = "default_snapshots_path")]
    pub snapshots_path: String,
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    pub optimizers: OptimizersConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
    pub hnsw_index: HnswConfig,
    #[serde(default = "default_mmap_advice")]
    pub mmap_advice: madvise::Advice,
}

fn default_snapshots_path() -> String {
    "./snapshots".to_string()
}

fn default_on_disk_payload() -> bool {
    false
}

fn default_mmap_advice() -> madvise::Advice {
    madvise::Advice::Random
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
    /// The index of the latest committed (finalized) operation that this peer is aware of.
    pub commit: u64,
    /// Number of consensus operations pending to be applied on this peer
    pub pending_operations: usize,
    /// Leader of the current term
    pub leader: Option<u64>,
    /// Role of this peer in the current term
    pub role: Option<StateRole>,
    /// Is this peer a voter or a learner
    pub is_voter: bool,
}

/// Role of the peer in the consensus
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, JsonSchema, Deserialize)]
pub enum StateRole {
    // The node is a follower of the leader.
    Follower,
    // The node could become a leader.
    Candidate,
    // The node is a leader.
    Leader,
    // The node could become a candidate, if `prevote` is enabled.
    PreCandidate,
}

impl From<raft::StateRole> for StateRole {
    fn from(role: raft::StateRole) -> Self {
        match role {
            raft::StateRole::Follower => Self::Follower,
            raft::StateRole::Candidate => Self::Candidate,
            raft::StateRole::Leader => Self::Leader,
            raft::StateRole::PreCandidate => Self::PreCandidate,
        }
    }
}

/// Message send failures for a particular peer
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
pub struct MessageSendErrors {
    pub count: usize,
    pub latest_error: Option<String>,
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
    /// Status of the thread that executes raft consensus
    pub consensus_thread_status: ConsensusThreadStatus,
    /// Consequent failures of message send operations in consensus by peer address.
    /// On the first success to send to that peer - entry is removed from this hashmap.
    pub message_send_failures: HashMap<String, MessageSendErrors>,
}

/// Information about current cluster status and structure
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(tag = "status")]
#[serde(rename_all = "snake_case")]
pub enum ClusterStatus {
    Disabled,
    Enabled(ClusterInfo),
}

/// Information about current consensus thread status
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(tag = "consensus_thread_status")]
#[serde(rename_all = "snake_case")]
pub enum ConsensusThreadStatus {
    Working { last_update: DateTime<Utc> },
    Stopped,
    StoppedWithErr { err: String },
}

impl Anonymize for PeerInfo {
    fn anonymize(&self) -> Self {
        PeerInfo {
            uri: self.uri.anonymize(),
        }
    }
}

impl Anonymize for RaftInfo {
    fn anonymize(&self) -> Self {
        RaftInfo {
            term: self.term,
            commit: self.commit,
            pending_operations: self.pending_operations,
            leader: self.leader,
            role: self.role,
            is_voter: self.is_voter,
        }
    }
}

impl Anonymize for ClusterInfo {
    fn anonymize(&self) -> Self {
        ClusterInfo {
            peer_id: self.peer_id,
            peers: self
                .peers
                .iter()
                .map(|(key, value)| (*key, value.anonymize()))
                .collect(),
            raft_info: self.raft_info.anonymize(),
            consensus_thread_status: self.consensus_thread_status.clone(),
            message_send_failures: self.message_send_failures.clone(),
        }
    }
}

impl Anonymize for ClusterStatus {
    fn anonymize(&self) -> Self {
        match self {
            ClusterStatus::Disabled => ClusterStatus::Disabled,
            ClusterStatus::Enabled(cluster_info) => {
                ClusterStatus::Enabled(cluster_info.anonymize())
            }
        }
    }
}
