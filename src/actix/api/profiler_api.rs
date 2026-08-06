use std::time::Duration;

use actix_web::{Responder, get, web};
use actix_web_validator::Query;
use api::grpc::transport_channel_pool::DEFAULT_GRPC_TIMEOUT;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::profiling::interface::get_requests_profile_log;
use collection::profiling::slow_requests_log::LogEntry;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::common::consensus_lag::collect_consensus_lag;

#[derive(Deserialize, Validate)]
struct LogParams {
    limit: Option<usize>,
    /// Optional filter by request name (substring match)
    request: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct SlowRequestsResponse {
    requests: Vec<LogEntry>,
}

const DEFAULT_SLOW_REQUESTS_LIMIT: usize = 10;

#[get("/profiler/slow_requests")]
async fn get_slow_requests(ActixAuth(auth): ActixAuth, params: Query<LogParams>) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_slow_requests")?;
        let LogParams { limit, request } = params.into_inner();

        let slow_requests = get_requests_profile_log(
            limit.unwrap_or(DEFAULT_SLOW_REQUESTS_LIMIT),
            request.as_deref(),
        )
        .await;

        Ok(SlowRequestsResponse {
            requests: slow_requests,
        })
    })
    .await
}

#[derive(Deserialize, Validate)]
struct ConsensusLagParams {
    /// Timeout in seconds for reaching each peer
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

#[get("/profiler/consensus_lag")]
async fn get_consensus_lag(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    params: Query<ConsensusLagParams>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_consensus_lag")?;

        let consensus_state = dispatcher.consensus_state().ok_or_else(|| {
            StorageError::bad_request("Consensus lag is only available in distributed mode")
        })?;

        // Not a collection level request.
        let pass = new_unchecked_verification_pass();
        let toc = dispatcher.toc(&auth, &pass);

        let ConsensusLagParams { timeout } = params.into_inner();
        let timeout = Duration::from_secs(timeout.unwrap_or(DEFAULT_GRPC_TIMEOUT.as_secs()));

        collect_consensus_lag(
            consensus_state,
            toc.get_channel_service(),
            toc.this_peer_id,
            timeout,
        )
        .await
    })
    .await
}

pub fn config_profiler_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_slow_requests);
    cfg.service(get_consensus_lag);
}
