use collection::shards::shard::PeerId;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::dispatcher::Dispatcher;
use storage::types::{ClusterStatus, ConsensusThreadStatus, StateRole};

use crate::settings::Settings;

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct P2pConfigTelemetry {
    connection_pool_size: usize,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ConsensusConfigTelemetry {
    max_message_queue_size: usize,
    tick_period_ms: u64,
    bootstrap_timeout_sec: u64,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ClusterConfigTelemetry {
    grpc_timeout_ms: u64,
    p2p: P2pConfigTelemetry,
    consensus: ConsensusConfigTelemetry,
}

impl From<&Settings> for ClusterConfigTelemetry {
    fn from(settings: &Settings) -> Self {
        ClusterConfigTelemetry {
            grpc_timeout_ms: settings.cluster.grpc_timeout_ms,
            p2p: P2pConfigTelemetry {
                connection_pool_size: settings.cluster.p2p.connection_pool_size,
            },
            consensus: ConsensusConfigTelemetry {
                max_message_queue_size: settings.cluster.consensus.max_message_queue_size,
                tick_period_ms: settings.cluster.consensus.tick_period_ms,
                bootstrap_timeout_sec: settings.cluster.consensus.bootstrap_timeout_sec,
            },
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ClusterStatusTelemetry {
    pub number_of_peers: usize,
    pub term: u64,
    pub commit: u64,
    pub pending_operations: usize,
    pub role: Option<StateRole>,
    pub is_voter: bool,
    pub peer_id: Option<PeerId>,
    pub consensus_thread_status: ConsensusThreadStatus,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ClusterTelemetry {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ClusterStatusTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<ClusterConfigTelemetry>,
}

impl ClusterTelemetry {
    pub fn collect(
        detail: TelemetryDetail,
        dispatcher: &Dispatcher,
        settings: &Settings,
    ) -> ClusterTelemetry {
        ClusterTelemetry {
            enabled: settings.cluster.enabled,
            status: (detail.level >= DetailsLevel::Level1)
                .then(|| match dispatcher.cluster_status() {
                    ClusterStatus::Disabled => None,
                    ClusterStatus::Enabled(cluster_info) => Some(ClusterStatusTelemetry {
                        number_of_peers: cluster_info.peers.len(),
                        term: cluster_info.raft_info.term,
                        commit: cluster_info.raft_info.commit,
                        pending_operations: cluster_info.raft_info.pending_operations,
                        role: cluster_info.raft_info.role,
                        is_voter: cluster_info.raft_info.is_voter,
                        peer_id: Some(cluster_info.peer_id),
                        consensus_thread_status: cluster_info.consensus_thread_status,
                    }),
                })
                .flatten(),
            config: (detail.level >= DetailsLevel::Level2)
                .then(|| ClusterConfigTelemetry::from(settings)),
        }
    }
}

impl Anonymize for ClusterTelemetry {
    fn anonymize(&self) -> Self {
        ClusterTelemetry {
            enabled: self.enabled,
            status: self.status.clone().map(|x| x.anonymize()),
            config: self.config.clone().map(|x| x.anonymize()),
        }
    }
}

impl Anonymize for ClusterStatusTelemetry {
    fn anonymize(&self) -> Self {
        ClusterStatusTelemetry {
            number_of_peers: self.number_of_peers,
            term: self.term,
            commit: self.commit,
            pending_operations: self.pending_operations,
            role: self.role,
            is_voter: self.is_voter,
            peer_id: None,
            consensus_thread_status: self.consensus_thread_status.clone(),
        }
    }
}

impl Anonymize for ClusterConfigTelemetry {
    fn anonymize(&self) -> Self {
        self.clone()
    }
}
