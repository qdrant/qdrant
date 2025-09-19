use actix_web::{Responder, get, web};
use actix_web_validator::Query;
use collection::profiling::interface::get_requests_profile_log;
use collection::profiling::slow_requests_log::LogEntry;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAccess;

#[derive(Deserialize, Validate)]
struct LogParams {
    limit: Option<usize>,
}

#[derive(Serialize, JsonSchema)]
struct SlowRequestsResponse {
    requests: Vec<LogEntry>,
}

const DEFAULT_SLOW_REQUESTS_LIMIT: usize = 10;

#[get("/profiler/slow_requests")]
async fn get_slow_requests(
    ActixAccess(access): ActixAccess,
    params: Query<LogParams>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        let slow_requests =
            get_requests_profile_log(params.limit.unwrap_or(DEFAULT_SLOW_REQUESTS_LIMIT)).await;

        Ok(SlowRequestsResponse {
            requests: slow_requests,
        })
    })
    .await
}

pub fn config_profiler_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_slow_requests);
}
