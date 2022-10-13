use actix_web::rt::time::Instant;
use actix_web::web::Query;
use actix_web::{get, put, web, Responder};
use schemars::JsonSchema;
use segment::telemetry::Anonymize;
use serde::{Deserialize, Serialize};
use storage::content_manager::toc::TableOfContent;
use tokio::sync::Mutex;

use crate::actix::helpers::process_response;
use crate::common::telemetry::TelemetryCollector;

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct TelemetryParam {
    pub anonymize: Option<bool>,
}

#[get("/telemetry")]
async fn telemetry(
    telemetry_collector: web::Data<Mutex<TelemetryCollector>>,
    params: Query<TelemetryParam>,
) -> impl Responder {
    let timing = Instant::now();
    let anonymize = params.anonymize.unwrap_or(false);
    let telemetry_collector = telemetry_collector.lock().await;
    let telemetry_data = telemetry_collector.prepare_data().await;
    let telemetry_data = if anonymize {
        telemetry_data.anonymize()
    } else {
        telemetry_data
    };
    process_response(Ok(telemetry_data), timing)
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct EnableUpdatingParam {
    pub enable: bool,
}

#[put("/enable_updating")]
async fn enable_updating(
    toc: web::Data<TableOfContent>,
    params: Query<EnableUpdatingParam>,
) -> impl Responder {
    let timing = Instant::now();
    let result = toc.get_ref().enable_updating(params.enable);
    process_response(Ok(result), timing)
}

#[get("/is_updating_enabled")]
async fn is_updating_enabled(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let result = toc.get_ref().is_updating_enabled();
    process_response(Ok(result), timing)
}

// Configure services
pub fn config_telemetry_api(cfg: &mut web::ServiceConfig) {
    cfg.service(telemetry)
        .service(is_updating_enabled)
        .service(enable_updating);
}
