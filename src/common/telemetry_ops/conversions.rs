use api::grpc::qdrant as grpc;
use tonic::Status;

use super::cluster_telemetry::ClusterStatusTelemetry;
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;

impl TryFrom<grpc::ClusterStatusTelemetry> for ClusterStatusTelemetry {
    type Error = Status;

    fn try_from(value: grpc::ClusterStatusTelemetry) -> Result<Self, Self::Error> {
        let consensus_thread_status = value
            .consensus_thread_status
            .ok_or_else(|| Status::invalid_argument("Consensus thread status is missing"))?
            .try_into()?;

        let role = Some(
            grpc::StateRole::try_from(value.role)
                .map_err(|err| Status::invalid_argument(format!("Invalid state role: {err}")))?
                .into(),
        );

        Ok(ClusterStatusTelemetry {
            number_of_peers: value.num_peers as usize,
            term: value.term,
            commit: value.commit,
            pending_operations: value.pending_operations as usize,
            role,
            is_voter: value.is_voter,
            peer_id: Some(value.peer_id),
            consensus_thread_status,
        })
    }
}

impl TryFrom<grpc::ClusterTelemetry> for ClusterTelemetry {
    type Error = Status;

    fn try_from(value: grpc::ClusterTelemetry) -> Result<Self, Self::Error> {
        Ok(ClusterTelemetry {
            enabled: true, // Not provided in gRPC, default to true if we have telemetry
            status: value.status.map(|s| s.try_into()).transpose()?,
            config: None,        // Not provided in gRPC
            peers: None,         // Not provided in gRPC
            peer_metadata: None, // Not provided in gRPC
            metadata: None,      // Not provided in gRPC
        })
    }
}
