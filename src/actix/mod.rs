#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod actix_telemetry;
pub mod api;
#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod helpers;

use std::sync::Arc;

use ::api::grpc::models::{ApiResponse, ApiStatus, VersionInfo};
use actix_cors::Cors;
use actix_multipart::form::tempfile::TempFileConfig;
use actix_multipart::form::MultipartFormConfig;
use actix_web::middleware::{Compress, Condition, Logger};
use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use collection::operations::validation;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use storage::dispatcher::Dispatcher;

use crate::actix::api::cluster_api::config_cluster_api;
use crate::actix::api::collections_api::config_collections_api;
use crate::actix::api::count_api::count_points;
use crate::actix::api::recommend_api::config_recommend_api;
use crate::actix::api::retrieve_api::{get_point, get_points, scroll_points};
use crate::actix::api::search_api::config_search_api;
use crate::actix::api::service_api::config_service_api;
use crate::actix::api::snapshot_api::config_snapshots_api;
use crate::actix::api::update_api::config_update_api;
use crate::common::telemetry::TelemetryCollector;
use crate::settings::{max_web_workers, Settings};

#[get("/")]
pub async fn index() -> impl Responder {
    HttpResponse::Ok().json(VersionInfo::default())
}

#[allow(dead_code)]
pub fn init(
    dispatcher: Arc<Dispatcher>,
    telemetry_collector: Arc<tokio::sync::Mutex<TelemetryCollector>>,
    settings: Settings,
) -> std::io::Result<()> {
    actix_web::rt::System::new().block_on(async {
        let toc_data = web::Data::from(dispatcher.toc().clone());
        let dispatcher_data = web::Data::from(dispatcher);
        let actix_telemetry_collector = telemetry_collector
            .lock()
            .await
            .actix_telemetry_collector
            .clone();
        let telemetry_collector_data = web::Data::from(telemetry_collector);
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

            App::new()
                .wrap(Compress::default()) // Reads the `Accept-Encoding` header to negotiate which compression codec to use.
                .wrap(Condition::new(settings.service.enable_cors, cors))
                .wrap(Logger::default().exclude("/")) // Avoid logging healthcheck requests
                .wrap(actix_telemetry::ActixTelemetryTransform::new(
                    actix_telemetry_collector.clone(),
                ))
                .app_data(dispatcher_data.clone())
                .app_data(toc_data.clone())
                .app_data(telemetry_collector_data.clone())
                .app_data(validate_path_config)
                .app_data(validate_query_config)
                .app_data(validate_json_config)
                .app_data(TempFileConfig::default().directory(dispatcher_data.snapshots_path()))
                .app_data(MultipartFormConfig::default().total_limit(usize::MAX))
                .service(index)
                .configure(config_collections_api)
                .configure(config_snapshots_api)
                .configure(config_update_api)
                .configure(config_cluster_api)
                .configure(config_service_api)
                .configure(config_search_api)
                .configure(config_recommend_api)
                .service(get_point)
                .service(get_points)
                .service(scroll_points)
                .service(count_points)
        })
        .workers(max_web_workers(&settings));

        let bind_addr = format!("{}:{}", settings.service.host, settings.service.http_port);

        server = if settings.service.enable_tls {
            let mut acceptor = SslAcceptor::mozilla_modern_v5(SslMethod::tls())?;

            let tls_config = settings
                .tls
                .ok_or_else(Settings::tls_config_is_undefined_error)?;

            acceptor.set_private_key_file(&tls_config.key, SslFiletype::PEM)?;
            acceptor.set_certificate_chain_file(&tls_config.cert)?;
            acceptor.check_private_key()?;

            server.bind_openssl(bind_addr, acceptor)?
        } else {
            server.bind(bind_addr)?
        };

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
            validation::label_errors(&format!("Validation error in {name}"), errs)
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
