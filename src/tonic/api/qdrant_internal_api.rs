use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant_internal_server::QdrantInternal;
use api::grpc::{
    GetAuditLogRequest, GetAuditLogResponse, GetConsensusCommitRequest, GetConsensusCommitResponse,
    GetTelemetryRequest, GetTelemetryResponse, PeerTelemetry, WaitOnConsensusCommitRequest,
    WaitOnConsensusCommitResponse,
};
use chrono::DateTime;
use common::types::{DetailsLevel, TelemetryDetail};
use storage::audit::AuditConfig;
use storage::audit_reader::{AuditLogQuery, read_local_audit_logs};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::rbac::{Access, Auth};
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
    /// Audit configuration
    audit_config: Option<AuditConfig>,
}

impl QdrantInternalService {
    pub fn new(
        telemetry_collector: Arc<Mutex<TelemetryCollector>>,
        settings: Settings,
        consensus_state: ConsensusStateRef,
    ) -> Self {
        let audit_config = settings.audit.clone();
        Self {
            telemetry_collector,
            settings,
            consensus_state,
            audit_config,
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
            per_collection: false,
        };

        let only_collections =
            collections_selector.map(|selector| selector.only_collections.into_iter().collect());

        let timing = Instant::now();
        let timeout = Duration::from_secs(timeout);

        let auth = Auth::new_internal(Access::full("internal service"));

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

    async fn get_audit_log(
        &self,
        request: Request<GetAuditLogRequest>,
    ) -> Result<Response<GetAuditLogResponse>, Status> {
        let GetAuditLogRequest {
            time_from,
            time_to,
            filters,
            limit,
        } = request.into_inner();

        let audit_config = self
            .audit_config
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Audit logging is not configured"))?;

        let time_from = time_from
            .as_deref()
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| Status::invalid_argument(format!("Invalid time_from: {e}")))
            })
            .transpose()?;

        let time_to = time_to
            .as_deref()
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| Status::invalid_argument(format!("Invalid time_to: {e}")))
            })
            .transpose()?;

        let filters: HashMap<String, String> = filters;

        let limit = if limit == 0 {
            None
        } else {
            Some(limit as usize)
        };

        let query = AuditLogQuery::new(time_from, time_to, filters, limit);

        let config = audit_config.clone();
        let entries = cancel::blocking::spawn_cancel_on_drop(move |cancel| {
            read_local_audit_logs(&config, &query, &cancel)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to read local audit logs: {e}")))?
        .map_err(|e| Status::internal(e.to_string()))?;

        let entries: Vec<String> = entries
            .iter()
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect();

        Ok(Response::new(GetAuditLogResponse { entries }))
    }
}
