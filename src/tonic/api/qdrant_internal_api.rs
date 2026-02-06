use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant_internal_server::QdrantInternal;
use api::grpc::{
    GetConsensusCommitRequest, GetConsensusCommitResponse, GetTelemetryRequest,
    GetTelemetryResponse, PeerTelemetry, WaitOnConsensusCommitRequest,
    WaitOnConsensusCommitResponse,
};
use common::types::{DetailsLevel, TelemetryDetail};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::rbac::{Access, Auth, AuthType};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::common::telemetry::TelemetryCollector;
use crate::settings::Settings;

pub struct QdrantInternalService {
    /// Telemetry collector
    telemetry_collector: Arc<Mutex<TelemetryCollector>>,
    /// Qdrant settings
    settings: Settings,
    /// Consensus state
    consensus_state: ConsensusStateRef,
}

impl QdrantInternalService {
    pub fn new(
        telemetry_collector: Arc<Mutex<TelemetryCollector>>,
        settings: Settings,
        consensus_state: ConsensusStateRef,
    ) -> Self {
        Self {
            telemetry_collector,
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

    async fn get_telemetry(
        &self,
        request: Request<GetTelemetryRequest>,
    ) -> Result<Response<GetTelemetryResponse>, Status> {
        let GetTelemetryRequest {
            details_level,
            collections_selector,
            timeout,
        } = request.into_inner();

        if details_level < 2 {
            return Err(Status::invalid_argument(
                "details_level for internal service must be >= 2",
            ));
        }

        let details_level = DetailsLevel::from(details_level.max(2) as usize);

        let detail = TelemetryDetail {
            level: details_level,
            histograms: false,
        };

        let only_collections =
            collections_selector.map(|selector| selector.only_collections.into_iter().collect());

        let timing = Instant::now();
        let timeout = Duration::from_secs(timeout);

        let auth = Auth::new(
            Access::full("internal service"),
            None,
            None,
            AuthType::Internal,
        );

        let telemetry_collector = self.telemetry_collector.lock().await;
        let telemetry_data = telemetry_collector
            .prepare_data(&auth, detail, only_collections, Some(timeout))
            .await?;

        let response = GetTelemetryResponse {
            result: Some(PeerTelemetry::try_from(telemetry_data)?),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }
}
