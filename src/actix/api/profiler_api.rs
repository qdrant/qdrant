use actix_web::{Responder, delete, get, web};
use actix_web_validator::Query;
use collection::profiling::interface::get_requests_profile_log;
use collection::profiling::slow_requests_log::LogEntry;
use common::spike_profiler::SpikeRecord;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;

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

#[derive(Serialize, JsonSchema)]
struct SearchSpikesResponse {
    count: usize,
    spikes: Vec<SpikeRecord>,
}

#[get("/profiler/search_spikes")]
async fn get_search_spikes(ActixAuth(auth): ActixAuth) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_search_spikes")?;
        let spikes = common::spike_profiler::get_spike_records();
        Ok(SearchSpikesResponse {
            count: spikes.len(),
            spikes,
        })
    })
    .await
}

#[delete("/profiler/search_spikes")]
async fn clear_search_spikes(ActixAuth(auth): ActixAuth) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "clear_search_spikes")?;
        common::spike_profiler::clear_spike_records();
        Ok(true)
    })
    .await
}

pub fn config_profiler_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_slow_requests)
        .service(get_search_spikes)
        .service(clear_search_spikes);
}
