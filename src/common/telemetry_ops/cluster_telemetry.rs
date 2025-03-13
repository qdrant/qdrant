use std::collections::HashMap;

use collection::shards::shard::PeerId;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use storage::types::{ClusterStatus, ConsensusThreadStatus, PeerInfo, StateRole};

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

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
#[anonymize(false)]
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

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct ClusterStatusTelemetry {
    #[anonymize(false)]
    pub number_of_peers: usize,
    #[anonymize(false)]
    pub term: u64,
    #[anonymize(false)]
    pub commit: u64,
    #[anonymize(false)]
    pub pending_operations: usize,
    pub role: Option<StateRole>,
    pub is_voter: bool,
    #[anonymize(value = None)]
    pub peer_id: Option<PeerId>,
    pub consensus_thread_status: ConsensusThreadStatus,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct ClusterTelemetry {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ClusterStatusTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<ClusterConfigTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub peers: Option<HashMap<PeerId, PeerInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl ClusterTelemetry {
    pub fn collect(
        access: &Access,
        detail: TelemetryDetail,
        dispatcher: &Dispatcher,
        settings: &Settings,
    ) -> Option<ClusterTelemetry> {
        let global_access = AccessRequirements::new().whole();
        if access.check_global_access(global_access).is_err() {
            return None;
        }

        Some(ClusterTelemetry {
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
            peers: (detail.level >= DetailsLevel::Level2)
                .then(|| match dispatcher.cluster_status() {
                    ClusterStatus::Disabled => None,
                    ClusterStatus::Enabled(cluster_info) => Some(cluster_info.peers),
                })
                .flatten(),
            metadata: (detail.level >= DetailsLevel::Level1)
                .then(|| {
                    dispatcher
                        .consensus_state()
                        .map(|state| state.persistent.read().cluster_metadata.clone())
                        .filter(|metadata| !metadata.is_empty())
                })
                .flatten(),
        })
    }
}
