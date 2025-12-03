use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant_internal_server::QdrantInternal;
use api::grpc::{
    ClusterTelemetry, CollectionTelemetry, GetConsensusCommitRequest, GetConsensusCommitResponse,
    GetPeerTelemetryRequest, GetPeerTelemetryResponse, PeerTelemetry, WaitOnConsensusCommitRequest,
    WaitOnConsensusCommitResponse,
};
use common::types::{DetailsLevel, TelemetryDetail};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::rbac::Access;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::common::telemetry::{TelemetryCollector, TelemetryData};
use crate::common::telemetry_ops::collections_telemetry::CollectionTelemetryEnum;
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

    async fn get_peer_telemetry(
        &self,
        request: Request<GetPeerTelemetryRequest>,
    ) -> Result<Response<GetPeerTelemetryResponse>, Status> {
        let request = request.into_inner();

        if request.details_level < 2 {
            return Err(Status::invalid_argument(
                "details_level for internal service must be >= 2",
            ));
        }

        let details_level = DetailsLevel::from(request.details_level.max(2) as usize);

        let detail = TelemetryDetail {
            level: details_level,
            histograms: false,
        };

        let timing = Instant::now();
        let timeout = Duration::from_secs(request.timeout);

        let access = Access::full("internal service");

        let telemetry_collector = self.telemetry_collector.lock().await;
        let telemetry_data = telemetry_collector
            .prepare_data(&access, detail, Some(timeout))
            .await?;

        let TelemetryData {
            id: _,
            app: _,
            collections,
            cluster,
            requests: _,
            memory: _,
            hardware: _,
        } = telemetry_data;

        let collections = collections
            .collections
            .into_iter()
            .flatten()
            .filter_map(|telemetry_enum| {
                match telemetry_enum {
                    CollectionTelemetryEnum::Full(collection_telemetry) => {
                        let telemetry = CollectionTelemetry::from(*collection_telemetry);
                        let collection_name = telemetry.id.clone();
                        Some((collection_name, telemetry))
                    }
                    // This only happens when details_level is < 2, which we explicitly fail
                    CollectionTelemetryEnum::Aggregated(_) => None,
                }
            })
            .collect();

        let response = GetPeerTelemetryResponse {
            result: Some(PeerTelemetry {
                collections,
                cluster: cluster.map(ClusterTelemetry::from),
            }),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }
}
