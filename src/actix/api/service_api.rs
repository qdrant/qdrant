use actix_web::rt::time::Instant;
use actix_web::web::Query;
use actix_web::{get, put, web, Responder};
use schemars::JsonSchema;
use segment::telemetry::Anonymize;
use serde::{Deserialize, Serialize};
use storage::content_manager::toc::TableOfContent;
use tokio::sync::Mutex;

use crate::actix::helpers::process_response;
use crate::common::helpers::WriteLockOptions;
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

#[put("/write_lock")]
async fn put_write_lock(
    toc: web::Data<TableOfContent>,
    options: Option<web::Json<WriteLockOptions>>,
) -> impl Responder {
    let timing = Instant::now();
    let result = toc.get_ref().set_write_lock(
        true,
        options.and_then(|options| options.error_message.clone()),
    );
    process_response(Ok(result), timing)
}

#[get("/write_lock")]
async fn get_write_lock(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let result = toc.get_ref().is_locked();
    process_response(Ok(result), timing)
}

#[put("/write_unlock")]
async fn put_write_unlock(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let result = toc.get_ref().set_write_lock(false, None);
    process_response(Ok(result), timing)
}

#[get("/write_unlock")]
async fn get_write_unlock(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let result = !toc.get_ref().is_locked();
    process_response(Ok(result), timing)
}

// Configure services
pub fn config_service_api(cfg: &mut web::ServiceConfig) {
    cfg.service(telemetry)
        .service(put_write_lock)
        .service(get_write_lock)
        .service(put_write_unlock)
        .service(get_write_unlock);
}
