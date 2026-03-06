use std::collections::HashMap;

use api::grpc::qdrant as grpc;
use chrono::DateTime;
use storage::types::{ConsensusThreadStatus, PeerInfo, StateRole};
use tonic::Status;

use super::cluster_telemetry::ClusterStatusTelemetry;
use crate::common::telemetry_ops::app_telemetry::AppBuildTelemetry;
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;

fn try_convert_opt_role_from_grpc(role: Option<i32>) -> Result<Option<StateRole>, Status> {
    role.map(|role| {
        Result::<_, Status>::Ok(StateRole::from(grpc::StateRole::try_from(role).map_err(
            |err| Status::invalid_argument(format!("Invalid state role: {err}")),
        )?))
    })
    .transpose()
}

impl TryFrom<grpc::AppTelemetry> for AppBuildTelemetry {
    type Error = Status;

    fn try_from(value: grpc::AppTelemetry) -> Result<Self, Self::Error> {
        let grpc::AppTelemetry {
            name,
            version,
            startup,
        } = value;

        Ok(Self {
            name,
            version,
            features: None,
            runtime_features: None,
            hnsw_global_config: None,
            system: None,
            jwt_rbac: None,
            hide_jwt_dashboard: None,
            startup: DateTime::from_timestamp_secs(startup)
                .ok_or_else(|| Status::internal("startup time is out-of-range"))?,
        })
    }
}

impl From<AppBuildTelemetry> for grpc::AppTelemetry {
    fn from(value: AppBuildTelemetry) -> Self {
        let AppBuildTelemetry {
            name,
            version,
            features: _,
            runtime_features: _,
            hnsw_global_config: _,
            system: _,
            jwt_rbac: _,
            hide_jwt_dashboard: _,
            startup,
        } = value;

        Self {
            name,
            version,
            startup: startup.timestamp(),
        }
    }
}

impl TryFrom<grpc::ClusterStatusTelemetry> for ClusterStatusTelemetry {
    type Error = Status;

    fn try_from(value: grpc::ClusterStatusTelemetry) -> Result<Self, Self::Error> {
        let grpc::ClusterStatusTelemetry {
            num_peers,
            term,
            commit,
            pending_operations,
            role,
            is_voter,
            peer_id,
            consensus_thread_status,
        } = value;

        let consensus_thread_status = ConsensusThreadStatus::try_from(
            consensus_thread_status
                .ok_or_else(|| Status::invalid_argument("Consensus thread status is missing"))?,
        )?;

        let role = try_convert_opt_role_from_grpc(role)?;

        Ok(ClusterStatusTelemetry {
            number_of_peers: num_peers as usize,
            term,
            commit,
            pending_operations: pending_operations as usize,
            role,
            is_voter,
            peer_id,
            consensus_thread_status,
        })
    }
}

impl From<ClusterStatusTelemetry> for grpc::ClusterStatusTelemetry {
    fn from(value: ClusterStatusTelemetry) -> Self {
        let ClusterStatusTelemetry {
            number_of_peers,
            term,
            commit,
            pending_operations,
            role,
            is_voter,
            peer_id,
            consensus_thread_status,
        } = value;

        grpc::ClusterStatusTelemetry {
            consensus_thread_status: Some(grpc::ConsensusThreadStatus::from(
                consensus_thread_status,
            )),
            role: role.map(|r| grpc::StateRole::from(r) as i32),
            num_peers: number_of_peers as u32,
            term,
            commit,
            pending_operations: pending_operations as u64,
            is_voter,
            peer_id,
        }
    }
}

impl TryFrom<grpc::ClusterTelemetry> for ClusterTelemetry {
    type Error = Status;

    fn try_from(value: grpc::ClusterTelemetry) -> Result<Self, Self::Error> {
        let grpc::ClusterTelemetry { status, peers } = value;

        let peers = peers
            .into_iter()
            .map(|(peer_id, grpc::PeerInfo { uri })| (peer_id, PeerInfo { uri }))
            .collect::<HashMap<_, _>>();

        Ok(ClusterTelemetry {
            enabled: true, // Not provided in gRPC, default to true if we have telemetry
            status: status.map(ClusterStatusTelemetry::try_from).transpose()?,
            config: None, // Not provided in gRPC
            peers: (!peers.is_empty()).then_some(peers),
            peer_metadata: None,      // Not provided in gRPC
            metadata: None,           // Not provided in gRPC
            resharding_enabled: None, // Not provided in gRPC
        })
    }
}

impl From<ClusterTelemetry> for grpc::ClusterTelemetry {
    fn from(value: ClusterTelemetry) -> Self {
        let ClusterTelemetry {
            enabled: _,
            status,
            config: _,
            peers,
            peer_metadata: _,
            metadata: _,
            resharding_enabled: _,
        } = value;

        let peers = peers
            .unwrap_or_default()
            .into_iter()
            .map(|(peer_id, PeerInfo { uri })| (peer_id, grpc::PeerInfo { uri }))
            .collect();

        grpc::ClusterTelemetry {
            status: status.map(grpc::ClusterStatusTelemetry::from),
            peers,
        }
    }
}
