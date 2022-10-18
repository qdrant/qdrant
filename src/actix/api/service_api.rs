use actix_web::rt::time::Instant;
use actix_web::web::Query;
use actix_web::{get, post, web, Responder};
use schemars::JsonSchema;
use segment::telemetry::Anonymize;
use serde::{Deserialize, Serialize};
use storage::content_manager::toc::TableOfContent;
use tokio::sync::Mutex;

use crate::actix::helpers::process_response;
use crate::common::helpers::LocksOption;
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

#[post("/locks")]
async fn put_locks(
    toc: web::Data<TableOfContent>,
    locks_option: web::Json<LocksOption>,
) -> impl Responder {
    let timing = Instant::now();
    let result = LocksOption {
        write: toc.get_ref().is_write_locked(),
        error_message: toc.get_ref().get_lock_error_message(),
    };
    toc.get_ref()
        .set_locks(locks_option.write, locks_option.error_message.clone());
    process_response(Ok(result), timing)
}

#[get("/locks")]
async fn get_locks(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let result = LocksOption {
        write: toc.get_ref().is_write_locked(),
        error_message: toc.get_ref().get_lock_error_message(),
    };
    process_response(Ok(result), timing)
}

// Configure services
pub fn config_service_api(cfg: &mut web::ServiceConfig) {
    cfg.service(telemetry).service(put_locks).service(get_locks);
}
