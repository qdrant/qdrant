use std::io;
use std::sync::Arc;

use actix_web::middleware::Compress;
use actix_web::{App, HttpServer, web};

use crate::actix::api::service_api::config_metrics_api;
use crate::actix::certificate_helpers;
use crate::common::telemetry::TelemetryCollector;
use crate::settings::Settings;

pub fn init_metrics(
    port: u16,
    telemetry_collector: Arc<tokio::sync::Mutex<TelemetryCollector>>,
    settings: Settings,
) -> io::Result<()> {
    actix_web::rt::System::new().block_on(async {
        let telemetry_collector_data = web::Data::from(telemetry_collector);
        let service_config = web::Data::new(settings.service.clone());

        let mut server = HttpServer::new(move || {
            App::new()
                .wrap(Compress::default())
                .app_data(telemetry_collector_data.clone())
                .app_data(service_config.clone())
                .configure(config_metrics_api)
        })
        .workers(1);

        let bind_addr = format!("{}:{}", settings.service.host, port);

        server = if settings.service.enable_tls {
            log::info!(
                "TLS enabled for metrics API (TTL: {})",
                settings
                    .tls
                    .as_ref()
                    .and_then(|tls| tls.cert_ttl)
                    .map(|ttl| ttl.to_string())
                    .unwrap_or_else(|| "none".into()),
            );

            let config = certificate_helpers::actix_tls_server_config(&settings)
                .map_err(io::Error::other)?;
            server.bind_rustls_0_23(bind_addr, config)?
        } else {
            log::info!("TLS disabled for metrics API");

            server.bind(bind_addr)?
        };

        log::info!("Qdrant metrics listening on {port}");
        server.run().await
    })
}
