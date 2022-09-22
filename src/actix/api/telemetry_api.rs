use actix_web::rt::time::Instant;
use actix_web::web::Query;
use actix_web::{get, web, Responder};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::actix::helpers::process_response;
use crate::common::telemetry::TelemetryCollector;

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct TelemetryParam {
    pub anonymize: Option<bool>,
    pub details_level: Option<usize>,
}

#[get("/telemetry")]
async fn telemetry(
    telemetry_collector: web::Data<Mutex<TelemetryCollector>>,
    params: Query<TelemetryParam>,
) -> impl Responder {
    let timing = Instant::now();
    let anonymize = params.anonymize.unwrap_or(false);
    let details_level = params.details_level.unwrap_or(0);
    let telemetry_collector = telemetry_collector.lock().await;
    let telemetry_data = telemetry_collector.prepare_data().await;
    let mut telemetry_data = if anonymize {
        telemetry_data.anonymize()
    } else {
        telemetry_data
    };
    telemetry_data.cut_by_detail_level(details_level);
    process_response(Ok(telemetry_data), timing)
}

// Configure services
pub fn config_telemetry_api(cfg: &mut web::ServiceConfig) {
    cfg.service(telemetry);
}
