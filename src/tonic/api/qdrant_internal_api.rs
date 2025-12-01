use api::grpc::qdrant_internal_server::QdrantInternal;
use api::grpc::{
    GetConsensusCommitRequest, GetConsensusCommitResponse, GetPeerTelemetryRequest,
    GetPeerTelemetryResponse, WaitOnConsensusCommitRequest, WaitOnConsensusCommitResponse,
};
use std::time::Duration;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use tonic::{Request, Response, Status};

use crate::settings::Settings;

pub struct QdrantInternalService {
    /// Qdrant settings
    settings: Settings,
    /// Consensus state
    consensus_state: ConsensusStateRef,
}

impl QdrantInternalService {
    pub fn new(settings: Settings, consensus_state: ConsensusStateRef) -> Self {
        Self {
            settings,
            consensus_state,
        }
    }
}

#[tonic::async_trait]
impl QdrantInternal for QdrantInternalService {
    async fn get_consensus_commit(
        &self,
        _: tonic::Request<GetConsensusCommitRequest>,
    ) -> Result<Response<GetConsensusCommitResponse>, Status> {
        let persistent = self.consensus_state.persistent.read();
        let commit = persistent.state.hard_state.commit as _;
        let term = persistent.state.hard_state.term as _;
        Ok(Response::new(GetConsensusCommitResponse { commit, term }))
    }

    async fn wait_on_consensus_commit(
        &self,
        request: Request<WaitOnConsensusCommitRequest>,
    ) -> Result<Response<WaitOnConsensusCommitResponse>, Status> {
        let request = request.into_inner();
        let commit = request.commit as u64;
        let term = request.term as u64;
        let timeout = Duration::from_secs(request.timeout as u64);
        let consensus_tick = Duration::from_millis(self.settings.cluster.consensus.tick_period_ms);
        let ok = self
            .consensus_state
            .wait_for_consensus_commit(commit, term, consensus_tick, timeout)
            .await
            .is_ok();
        Ok(Response::new(WaitOnConsensusCommitResponse { ok }))
    }

    async fn get_peer_telemetry(
        &self,
        _request: Request<GetPeerTelemetryRequest>,
    ) -> Result<Response<GetPeerTelemetryResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }
}
