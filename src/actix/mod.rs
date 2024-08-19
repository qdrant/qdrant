#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod actix_telemetry;
pub mod api;
mod auth;
mod certificate_helpers;
#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod helpers;
pub mod web_ui;

use std::io;
use std::sync::Arc;

use ::api::grpc::models::{ApiResponse, ApiStatus, VersionInfo};
use actix_cors::Cors;
use actix_multipart::form::tempfile::TempFileConfig;
use actix_multipart::form::MultipartFormConfig;
use actix_web::middleware::{Compress, Condition, Logger};
use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_extras::middleware::Condition as ConditionEx;
use api::facet_api::config_facet_api;
use collection::operations::validation;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;

use crate::actix::api::cluster_api::config_cluster_api;
use crate::actix::api::collections_api::config_collections_api;
use crate::actix::api::count_api::count_points;
use crate::actix::api::debug_api::config_debugger_api;
use crate::actix::api::discovery_api::config_discovery_api;
use crate::actix::api::issues_api::config_issues_api;
use crate::actix::api::query_api::config_query_api;
use crate::actix::api::recommend_api::config_recommend_api;
use crate::actix::api::retrieve_api::{get_point, get_points, scroll_points};
use crate::actix::api::search_api::config_search_api;
use crate::actix::api::service_api::config_service_api;
use crate::actix::api::shards_api::config_shards_api;
use crate::actix::api::snapshot_api::config_snapshots_api;
use crate::actix::api::update_api::config_update_api;
use crate::actix::auth::{Auth, WhitelistItem};
use crate::actix::web_ui::{web_ui_factory, web_ui_folder, WEB_UI_PATH};
use crate::common::auth::AuthKeys;
use crate::common::debugger::DebuggerState;
use crate::common::health;
use crate::common::http_client::HttpClient;
use crate::common::telemetry::TelemetryCollector;
use crate::settings::{max_web_workers, Settings};
use crate::tracing::LoggerHandle;

#[get("/")]
pub async fn index() -> impl Responder {
    HttpResponse::Ok().json(VersionInfo::default())
}

#[allow(dead_code)]
pub fn init(
    dispatcher: Arc<Dispatcher>,
    telemetry_collector: Arc<tokio::sync::Mutex<TelemetryCollector>>,
    health_checker: Option<Arc<health::HealthChecker>>,
    settings: Settings,
    logger_handle: LoggerHandle,
) -> io::Result<()> {
    actix_web::rt::System::new().block_on(async {
        let auth_keys = AuthKeys::try_create(
            &settings.service,
            dispatcher.toc(&Access::full("For JWT validation")).clone(),
        );
        let upload_dir = dispatcher
            .toc(&Access::full("For upload dir"))
            .upload_dir()
            .unwrap();
        let dispatcher_data = web::Data::from(dispatcher);
        let actix_telemetry_collector = telemetry_collector
            .lock()
            .await
            .actix_telemetry_collector
            .clone();
        let debugger_state = web::Data::new(DebuggerState::from_settings(&settings));
        let telemetry_collector_data = web::Data::from(telemetry_collector);
        let logger_handle_data = web::Data::new(logger_handle);
        let http_client = web::Data::new(HttpClient::from_settings(&settings)?);
        let health_checker = web::Data::new(health_checker);
        let web_ui_available = web_ui_folder(&settings);

        let mut api_key_whitelist = vec![
            WhitelistItem::exact("/"),
            WhitelistItem::exact("/healthz"),
            WhitelistItem::prefix("/readyz"),
            WhitelistItem::prefix("/livez"),
        ];
        if web_ui_available.is_some() {
            api_key_whitelist.push(WhitelistItem::prefix(WEB_UI_PATH));
        }

        let mut server = HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header();
            let validate_path_config = actix_web_validator::PathConfig::default()
                .error_handler(|err, rec| validation_error_handler("path parameters", err, rec));
            let validate_query_config = actix_web_validator::QueryConfig::default()
                .error_handler(|err, rec| validation_error_handler("query parameters", err, rec));
            let validate_json_config = actix_web_validator::JsonConfig::default()
                .limit(settings.service.max_request_size_mb * 1024 * 1024)
                .error_handler(|err, rec| validation_error_handler("JSON body", err, rec));

            let mut app = App::new()
                .wrap(Compress::default()) // Reads the `Accept-Encoding` header to negotiate which compression codec to use.
                // api_key middleware
                // note: the last call to `wrap()` or `wrap_fn()` is executed first
                .wrap(ConditionEx::from_option(auth_keys.as_ref().map(
                    |auth_keys| Auth::new(auth_keys.clone(), api_key_whitelist.clone()),
                )))
                .wrap(Condition::new(settings.service.enable_cors, cors))
                .wrap(
                    // Set up logger, but avoid logging hot status endpoints
                    Logger::default()
                        .exclude("/")
                        .exclude("/metrics")
                        .exclude("/telemetry")
                        .exclude("/healthz")
                        .exclude("/readyz")
                        .exclude("/livez"),
                )
                .wrap(actix_telemetry::ActixTelemetryTransform::new(
                    actix_telemetry_collector.clone(),
                ))
                .app_data(dispatcher_data.clone())
                .app_data(telemetry_collector_data.clone())
                .app_data(logger_handle_data.clone())
                .app_data(http_client.clone())
                .app_data(debugger_state.clone())
                .app_data(health_checker.clone())
                .app_data(validate_path_config)
                .app_data(validate_query_config)
                .app_data(validate_json_config)
                .app_data(TempFileConfig::default().directory(&upload_dir))
                .app_data(MultipartFormConfig::default().total_limit(usize::MAX))
                .service(index)
                .configure(config_collections_api)
                .configure(config_snapshots_api)
                .configure(config_update_api)
                .configure(config_cluster_api)
                .configure(config_service_api)
                .configure(config_search_api)
                .configure(config_recommend_api)
                .configure(config_discovery_api)
                .configure(config_query_api)
                .configure(config_facet_api)
                .configure(config_shards_api)
                .configure(config_issues_api)
                .configure(config_debugger_api)
                // Ordering of services is important for correct path pattern matching
                // See: <https://github.com/qdrant/qdrant/issues/3543>
                .service(scroll_points)
                .service(count_points)
                .service(get_point)
                .service(get_points);

            if let Some(static_folder) = web_ui_available.as_deref() {
                app = app.service(web_ui_factory(static_folder));
            }

            app
        })
        .workers(max_web_workers(&settings));

        let port = settings.service.http_port;
        let bind_addr = format!("{}:{}", settings.service.host, port);

        // With TLS enabled, bind with certificate helper and Rustls, or bind regularly
        server = if settings.service.enable_tls {
            log::info!(
                "TLS enabled for REST API (TTL: {})",
                settings
                    .tls
                    .as_ref()
                    .and_then(|tls| tls.cert_ttl)
                    .map(|ttl| ttl.to_string())
                    .unwrap_or_else(|| "none".into()),
            );

            let config = certificate_helpers::actix_tls_server_config(&settings)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            server.bind_rustls_0_23(bind_addr, config)?
        } else {
            log::info!("TLS disabled for REST API");

            server.bind(bind_addr)?
        };

        log::info!("Qdrant HTTP listening on {}", port);
        server.run().await
    })
}

fn validation_error_handler(
    name: &str,
    err: actix_web_validator::Error,
    _req: &HttpRequest,
) -> error::Error {
    use actix_web_validator::error::DeserializeErrors;

    // Nicely describe deserialization and validation errors
    let msg = match &err {
        actix_web_validator::Error::Validate(errs) => {
            validation::label_errors(format!("Validation error in {name}"), errs)
        }
        actix_web_validator::Error::Deserialize(err) => {
            format!(
                "Deserialize error in {name}: {}",
                match err {
                    DeserializeErrors::DeserializeQuery(err) => err.to_string(),
                    DeserializeErrors::DeserializeJson(err) => err.to_string(),
                    DeserializeErrors::DeserializePath(err) => err.to_string(),
                }
            )
        }
        actix_web_validator::Error::JsonPayloadError(
            actix_web::error::JsonPayloadError::Deserialize(err),
        ) => {
            format!("Format error in {name}: {err}",)
        }
        err => err.to_string(),
    };

    // Build fitting response
    let response = match &err {
        actix_web_validator::Error::Validate(_) => HttpResponse::UnprocessableEntity(),
        _ => HttpResponse::BadRequest(),
    }
    .json(ApiResponse::<()> {
        result: None,
        status: ApiStatus::Error(msg),
        time: 0.0,
    });
    error::InternalError::from_response(err, response).into()
}

#[cfg(test)]
mod tests {
    use ::api::grpc::api_crate_version;

    #[test]
    fn test_version() {
        assert_eq!(
            api_crate_version(),
            env!("CARGO_PKG_VERSION"),
            "Qdrant and lib/api crate versions are not same"
        );
    }
}
