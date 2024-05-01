use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use collection::common::snapshots_manager::S3Config;
use collection::config::WalConfig;
use collection::operations::shared_storage_config::{
    SharedStorageConfig, DEFAULT_IO_SHARD_TRANSFER_LIMIT, DEFAULT_SNAPSHOTS_PATH,
    DEFAULT_SNAPSHOTS_STORAGE,
};
use collection::operations::types::{NodeType, PeerMetadata};
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::shard::PeerId;
use collection::shards::transfer::ShardTransferMethod;
use memory::madvise;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::{HnswConfig, QuantizationConfig};
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;
use validator::Validate;

pub type PeerAddressById = HashMap<PeerId, Uri>;
pub type PeerMetadataById = HashMap<PeerId, PeerMetadata>;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
    #[serde(default)]
    pub max_optimization_threads: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_rate_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_timeout_sec: Option<usize>,
    /// CPU budget, how many CPUs (threads) to allocate for an optimization job.
    /// If 0 - auto selection, keep 1 or more CPUs unallocated depending on CPU size
    /// If negative - subtract this relative number of CPUs from the available CPUs.
    /// If positive - use this absolute number of CPUs.
    #[serde(default)]
    pub optimizer_cpu_budget: isize,
    #[serde(default = "default_io_shard_transfers_limit")]
    pub incoming_shard_transfers_limit: Option<usize>,
    #[serde(default = "default_io_shard_transfers_limit")]
    pub outgoing_shard_transfers_limit: Option<usize>,
}

const fn default_io_shard_transfers_limit() -> Option<usize> {
    DEFAULT_IO_SHARD_TRANSFER_LIMIT
}

/// Global configuration of the storage, loaded on the service launch, default stored in ./config
#[derive(Clone, Debug, Deserialize, Validate)]
pub struct StorageConfig {
    #[validate(length(min = 1))]
    pub storage_path: String,
    #[serde(default = "default_snapshots_path")]
    #[validate(length(min = 1))]
    pub snapshots_path: String,
    #[serde(default)]
    pub s3_config: Option<S3Config>,
    #[validate(length(min = 1))]
    #[serde(default)]
    pub temp_path: Option<String>,
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    #[validate]
    pub optimizers: OptimizersConfig,
    #[validate]
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
    #[validate]
    pub hnsw_index: HnswConfig,
    #[validate]
    pub quantization: Option<QuantizationConfig>,
    #[serde(default = "default_mmap_advice")]
    pub mmap_advice: madvise::Advice,
    #[serde(default)]
    pub node_type: NodeType,
    #[serde(default)]
    pub update_queue_size: Option<usize>,
    #[serde(default)]
    pub handle_collection_load_errors: bool,
    #[serde(default)]
    pub async_scorer: bool,
    /// If provided - qdrant will start in recovery mode, which means that it will not accept any new data.
    /// Only collection metadata will be available, and it will only process collection delete requests.
    /// Provided value will be used error message for unavailable requests.
    #[serde(default)]
    pub recovery_mode: Option<String>,
    #[serde(default)]
    pub update_concurrency: Option<NonZeroUsize>,
    /// Default method used for transferring shards.
    #[serde(default)]
    pub shard_transfer_method: Option<ShardTransferMethod>,
    #[serde(default = "default_snapshots_storage_path")]
    pub snapshots_storage: String,
}

impl StorageConfig {
    pub fn to_shared_storage_config(&self, is_distributed: bool) -> SharedStorageConfig {
        SharedStorageConfig::new(
            self.update_queue_size,
            self.node_type,
            self.handle_collection_load_errors,
            self.recovery_mode.clone(),
            self.performance
                .search_timeout_sec
                .map(|x| Duration::from_secs(x as u64)),
            self.update_concurrency,
            is_distributed,
            self.shard_transfer_method,
            self.performance.incoming_shard_transfers_limit,
            self.performance.outgoing_shard_transfers_limit,
            self.snapshots_path.clone(),
            self.s3_config.clone(),
            self.snapshots_storage.clone(),
        )
    }
}

fn default_snapshots_path() -> String {
    DEFAULT_SNAPSHOTS_PATH.to_string()
}

const fn default_on_disk_payload() -> bool {
    false
}

const fn default_mmap_advice() -> madvise::Advice {
    madvise::Advice::Random
}

fn default_snapshots_storage_path() -> String {
    DEFAULT_SNAPSHOTS_STORAGE.to_string()
}

/// Information of a peer in the cluster
#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct PeerInfo {
    pub uri: String,
    // ToDo: How long ago was the last communication? In milliseconds
    // pub last_responded_millis: usize
}

/// Summary information about the current raft state
#[derive(Debug, Serialize, JsonSchema, Clone)]
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
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, JsonSchema)]
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
#[derive(Debug, Serialize, JsonSchema, Clone, Default)]
pub struct MessageSendErrors {
    pub count: usize,
    pub latest_error: Option<String>,
    /// Timestamp of the latest error
    pub latest_error_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Description of enabled cluster
#[derive(Debug, Serialize, JsonSchema, Clone)]
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
#[derive(Debug, Serialize, JsonSchema, Clone)]
#[serde(tag = "status")]
#[serde(rename_all = "snake_case")]
pub enum ClusterStatus {
    Disabled,
    Enabled(ClusterInfo),
}

/// Information about current consensus thread status
#[derive(Debug, Serialize, JsonSchema, Clone)]
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
