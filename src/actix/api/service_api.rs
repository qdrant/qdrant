use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use actix_web::http::StatusCode;
use actix_web::http::header::ContentType;
use actix_web::rt::time::Instant;
use actix_web::web::Data;
use actix_web::{HttpResponse, Responder, get, post, web};
use actix_web_validator::{Path, Query};
use collection::operations::verification::new_unchecked_verification_pass;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tokio::sync::Mutex;
use validator::Validate;

use super::CollectionPath;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{self, process_response_error};
use crate::common::health;
use crate::common::metrics::MetricsData;
use crate::common::stacktrace::get_stack_trace;
use crate::common::telemetry::TelemetryCollector;
use crate::settings::ServiceConfig;
use crate::tracing;

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct TelemetryParam {
    pub anonymize: Option<bool>,
    pub details_level: Option<usize>,
    #[validate(range(min = 1))]
    pub timeout: Option<u64>,
}

impl TelemetryParam {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/telemetry")]
fn telemetry(
    telemetry_collector: Data<Mutex<TelemetryCollector>>,
    params: Query<TelemetryParam>,
    ActixAuth(auth): ActixAuth,
) -> impl Future<Output = HttpResponse> {
    helpers::time(async move {
        let anonymize = params.anonymize.unwrap_or(false);
        let details_level = params
            .details_level
            .map_or(DetailsLevel::Level0, Into::into);

        let detail = TelemetryDetail {
            level: details_level,
            histograms: false,
        };
        let telemetry_data = telemetry_collector
            .lock()
            .await
            .prepare_data(&auth, detail, None, params.timeout())
            .await?;
        let telemetry_data = if anonymize {
            telemetry_data.anonymize()
        } else {
            telemetry_data
        };
        Ok(telemetry_data)
    })
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct MetricsParam {
    pub anonymize: Option<bool>,
    #[validate(range(min = 1))]
    pub timeout: Option<u64>,
}

impl MetricsParam {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/metrics")]
async fn metrics(
    telemetry_collector: Data<Mutex<TelemetryCollector>>,
    params: Query<MetricsParam>,
    config: Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    if let Err(err) = auth
        .unlogged_access() // Do not log access to metrics, as it is too noizy
        .check_global_access(AccessRequirements::new())
    {
        return process_response_error(err, Instant::now(), None);
    }

    let anonymize = params.anonymize.unwrap_or(false);
    let telemetry_data = telemetry_collector
        .lock()
        .await
        .prepare_data(
            &auth,
            TelemetryDetail {
                level: DetailsLevel::Level4,
                histograms: true,
            },
            None,
            params.timeout(),
        )
        .await;
    match telemetry_data {
        Err(err) => process_response_error(err, Instant::now(), None),
        Ok(telemetry_data) => {
            let telemetry_data = if anonymize {
                telemetry_data.anonymize()
            } else {
                telemetry_data
            };

            let metrics_prefix = config.metrics_prefix.as_deref();
            HttpResponse::Ok()
                .content_type(ContentType::plaintext())
                .body(
                    MetricsData::new_from_telemetry(telemetry_data, metrics_prefix)
                        .format_metrics(),
                )
        }
    }
}

#[get("/stacktrace")]
fn get_stacktrace(ActixAuth(auth): ActixAuth) -> impl Future<Output = HttpResponse> {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_stacktrace")?;
        Ok(get_stack_trace())
    })
}

#[get("/healthz")]
async fn healthz() -> impl Responder {
    kubernetes_healthz()
}

#[get("/livez")]
async fn livez() -> impl Responder {
    kubernetes_healthz()
}

#[get("/readyz")]
async fn readyz(health_checker: web::Data<Option<Arc<health::HealthChecker>>>) -> impl Responder {
    let is_ready = match health_checker.as_ref() {
        Some(health_checker) => health_checker.check_ready().await,
        None => true,
    };

    let (status, body) = if is_ready {
        (StatusCode::OK, "all shards are ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "some shards are not ready")
    };

    HttpResponse::build(status)
        .content_type(ContentType::plaintext())
        .body(body)
}

/// Basic Kubernetes healthz endpoint
fn kubernetes_healthz() -> impl Responder {
    HttpResponse::Ok()
        .content_type(ContentType::plaintext())
        .body("healthz check passed")
}

#[get("/logger")]
async fn get_logger_config(
    ActixAuth(auth): ActixAuth,
    handle: web::Data<tracing::LoggerHandle>,
) -> impl Responder {
    let timing = Instant::now();

    let future = async {
        let _ = auth.check_global_access(AccessRequirements::new(), "get_logger_config")?;
        let config = handle.get_config().await;
        Ok(config)
    };

    helpers::process_response(future.await, timing, None)
}

#[post("/logger")]
async fn update_logger_config(
    ActixAuth(auth): ActixAuth,
    handle: web::Data<tracing::LoggerHandle>,
    mut config: web::Json<tracing::LoggerConfig>,
) -> impl Responder {
    let timing = Instant::now();

    let future = async {
        let _ =
            auth.check_global_access(AccessRequirements::new().manage(), "update_logger_config")?;

        // Log file can only be set in Qdrant config file
        config.on_disk.log_file = None;

        handle
            .update_config(config.into_inner())
            .await
            .map_err(|err| StorageError::service_error(err.to_string()))?;

        Ok(true)
    };

    helpers::process_response(future.await, timing, None)
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct TruncateUnappliedWalParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<bool>,
}

#[post("/collections/{name}/truncate_unapplied_wal")]
async fn truncate_unapplied_wal(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    params: Query<TruncateUnappliedWalParams>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let future = async move {
        let collection_pass = auth
            .check_global_access(AccessRequirements::new().manage(), "truncate_unapplied_wal")?
            .issue_pass(&collection.name)
            .into_static();

        let pass = new_unchecked_verification_pass();
        let collection = dispatcher
            .toc(&auth, &pass)
            .get_collection(&collection_pass)
            .await?;

        collection
            .truncate_unapplied_wal()
            .await
            .map_err(StorageError::from)
    };
    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

// Configure services
pub fn config_service_api(cfg: &mut web::ServiceConfig) {
    cfg.service(telemetry)
        .service(metrics)
        .service(get_stacktrace)
        .service(healthz)
        .service(livez)
        .service(readyz)
        .service(get_logger_config)
        .service(update_logger_config)
        .service(truncate_unapplied_wal);
}

// Dedicated service for metrics
pub fn config_metrics_api(cfg: &mut web::ServiceConfig) {
    cfg.service(metrics);
}
