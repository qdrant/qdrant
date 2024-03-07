#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod actix_telemetry;
pub mod api;
mod api_key;
mod certificate_helpers;
#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod helpers;

use std::io;
use std::path::Path;
use std::sync::Arc;

use ::api::grpc::models::{ApiResponse, ApiStatus, VersionInfo};
use actix_cors::Cors;
use actix_multipart::form::tempfile::TempFileConfig;
use actix_multipart::form::MultipartFormConfig;
use actix_web::middleware::{Compress, Condition, Logger};
use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use collection::operations::validation;
use storage::dispatcher::Dispatcher;

use crate::actix::api::cluster_api::config_cluster_api;
use crate::actix::api::collections_api::config_collections_api;
use crate::actix::api::count_api::count_points;
use crate::actix::api::discovery_api::config_discovery_api;
use crate::actix::api::recommend_api::config_recommend_api;
use crate::actix::api::retrieve_api::{get_point, get_points, scroll_points};
use crate::actix::api::search_api::config_search_api;
use crate::actix::api::service_api::config_service_api;
use crate::actix::api::shards_api::config_shards_api;
use crate::actix::api::snapshot_api::config_snapshots_api;
use crate::actix::api::update_api::config_update_api;
use crate::actix::api_key::{ApiKey, WhitelistItem};
use crate::common::auth::AuthKeys;
use crate::common::health;
use crate::common::http_client::HttpClient;
use crate::common::telemetry::TelemetryCollector;
use crate::settings::{max_web_workers, Settings};

const DEFAULT_STATIC_DIR: &str = "./static";
const WEB_UI_PATH: &str = "/dashboard";

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
) -> io::Result<()> {
    actix_web::rt::System::new().block_on(async {
        let toc_data = web::Data::from(dispatcher.toc().clone());
        let dispatcher_data = web::Data::from(dispatcher);
        let actix_telemetry_collector = telemetry_collector
            .lock()
            .await
            .actix_telemetry_collector
            .clone();
        let telemetry_collector_data = web::Data::from(telemetry_collector);
        let http_client = web::Data::new(HttpClient::from_settings(&settings)?);
        let health_checker = web::Data::new(health_checker);
        let auth_keys = AuthKeys::try_create(&settings.service);
        let static_folder = settings
            .service
            .static_content_dir
            .clone()
            .unwrap_or(DEFAULT_STATIC_DIR.to_string());

        let web_ui_enabled = settings.service.enable_static_content.unwrap_or(true);
        // validate that the static folder exists IF the web UI is enabled
        let web_ui_available = if web_ui_enabled {
            let static_folder = Path::new(&static_folder);
            if !static_folder.exists() || !static_folder.is_dir() {
                // enabled BUT folder does not exist
                log::warn!(
                    "Static content folder for Web UI '{}' does not exist",
                    static_folder.display(),
                );
                false
            } else {
                // enabled AND folder exists
                true
            }
        } else {
            // not enabled
            false
        };

        let mut api_key_whitelist = vec![
            WhitelistItem::exact("/"),
            WhitelistItem::exact("/healthz"),
            WhitelistItem::prefix("/readyz"),
            WhitelistItem::prefix("/livez"),
        ];
        if web_ui_available {
            api_key_whitelist.push(WhitelistItem::prefix(WEB_UI_PATH));
        }

        let upload_dir = dispatcher_data.upload_dir().unwrap();

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
                .wrap(Condition::new(
                    auth_keys.is_some(),
                    ApiKey::new(auth_keys.clone(), api_key_whitelist.clone()),
                ))
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
                .app_data(toc_data.clone())
                .app_data(telemetry_collector_data.clone())
                .app_data(http_client.clone())
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
                .configure(config_shards_api)
                // Ordering of services is important for correct path pattern matching
                // See: <https://github.com/qdrant/qdrant/issues/3543>
                .service(scroll_points)
                .service(count_points)
                .service(get_point)
                .service(get_points);

            if web_ui_available {
                app = app.service(
                    actix_files::Files::new(WEB_UI_PATH, &static_folder).index_file("index.html"),
                )
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
            server.bind_rustls_0_22(bind_addr, config)?
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
            format!("Format error in {name}: {}", err,)
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
