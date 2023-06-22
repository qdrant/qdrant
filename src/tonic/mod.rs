mod api;
mod api_key;
mod logging;
mod tonic_telemetry;

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use ::api::grpc::models::VersionInfo;
use ::api::grpc::qdrant::collections_internal_server::CollectionsInternalServer;
use ::api::grpc::qdrant::collections_server::CollectionsServer;
use ::api::grpc::qdrant::points_internal_server::PointsInternalServer;
use ::api::grpc::qdrant::points_server::PointsServer;
use ::api::grpc::qdrant::qdrant_server::{Qdrant, QdrantServer};
use ::api::grpc::qdrant::snapshots_server::SnapshotsServer;
use ::api::grpc::qdrant::{HealthCheckReply, HealthCheckRequest};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use tokio::runtime::Handle;
use tokio::signal;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::common::helpers;
use crate::common::telemetry_ops::requests_telemetry::TonicTelemetryCollector;
use crate::settings::Settings;
use crate::tonic::api::collections_api::CollectionsService;
use crate::tonic::api::collections_internal_api::CollectionsInternalService;
use crate::tonic::api::points_api::PointsService;
use crate::tonic::api::points_internal_api::PointsInternalService;
use crate::tonic::api::snapshots_api::SnapshotsService;

#[derive(Default)]
pub struct QdrantService {}

#[tonic::async_trait]
impl Qdrant for QdrantService {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(VersionInfo::default().into()))
    }
}

#[cfg(not(unix))]
async fn wait_stop_signal(for_what: &str) {
    signal::ctrl_c().await.unwrap();
    log::debug!("Stopping {} on SIGINT", for_what)
}

#[cfg(unix)]
async fn wait_stop_signal(for_what: &str) {
    let mut term = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
    let mut inrt = signal::unix::signal(signal::unix::SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = term.recv() => log::debug!("Stopping {} on SIGTERM", for_what),
        _ = inrt.recv() => log::debug!("Stopping {} on SIGINT", for_what),
    }
}

pub fn init(
    dispatcher: Arc<Dispatcher>,
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    settings: Settings,
    grpc_port: u16,
    runtime: Handle,
) -> io::Result<()> {
    runtime.block_on(async {
        let socket =
            SocketAddr::from((settings.service.host.parse::<IpAddr>().unwrap(), grpc_port));

        let qdrant_service = QdrantService::default();
        let collections_service = CollectionsService::new(dispatcher.clone());
        let points_service = PointsService::new(dispatcher.toc().clone());
        let snapshot_service = SnapshotsService::new(dispatcher.clone());

        log::info!("Qdrant gRPC listening on {}", grpc_port);

        let mut server = Server::builder();

        if settings.service.enable_tls {
            log::info!("TLS enabled for gRPC API (TTL not supported)");

            let tls_server_config = helpers::load_tls_external_server_config(settings.tls()?)?;

            server = server
                .tls_config(tls_server_config)
                .map_err(helpers::tonic_error_to_io_error)?;
        } else {
            log::info!("TLS disabled for gRPC API");
        }

        // The stack of middleware that our service will be wrapped in
        let middleware_layer = tower::ServiceBuilder::new()
            .layer(logging::LoggingMiddlewareLayer::new())
            .layer(tonic_telemetry::TonicTelemetryLayer::new(
                telemetry_collector,
            ))
            .option_layer(
                settings
                    .service
                    .api_key
                    .map(api_key::ApiKeyMiddlewareLayer::new),
            )
            .into_inner();

        server
            .layer(middleware_layer)
            .add_service(
                QdrantServer::new(qdrant_service)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                CollectionsServer::new(collections_service)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                PointsServer::new(points_service)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                SnapshotsServer::new(snapshot_service)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(usize::MAX),
            )
            .serve_with_shutdown(socket, async {
                wait_stop_signal("gRPC service").await;
            })
            .await
            .map_err(helpers::tonic_error_to_io_error)
    })?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn init_internal(
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    host: String,
    internal_grpc_port: u16,
    tls_config: Option<ServerTlsConfig>,
    to_consensus: tokio::sync::mpsc::Sender<crate::consensus::Message>,
    runtime: Handle,
) -> std::io::Result<()> {
    use ::api::grpc::qdrant::raft_server::RaftServer;

    use crate::tonic::api::raft_api::RaftService;

    runtime
        .block_on(async {
            let socket = SocketAddr::from((host.parse::<IpAddr>().unwrap(), internal_grpc_port));

            let qdrant_service = QdrantService::default();
            let collections_internal_service = CollectionsInternalService::new(toc.clone());
            let points_internal_service = PointsInternalService::new(toc.clone());
            let raft_service = RaftService::new(to_consensus, consensus_state);

            log::debug!("Qdrant internal gRPC listening on {}", internal_grpc_port);

            let mut server = Server::builder()
                // Internally use a high limit for pending accept streams.
                // We can have a huge number of reset/dropped HTTP2 streams in our internal
                // communcation when there are a lot of clients dropping connections. This
                // internally causes an GOAWAY/ENHANCE_YOUR_CALM error breaking cluster consensus.
                // We prefer to keep more pending reset streams even though this may be expensive,
                // versus an internal error that is very hard to handle.
                // More info: <https://github.com/qdrant/qdrant/issues/1907>
                .http2_max_pending_accept_reset_streams(Some(1024));

            if let Some(config) = tls_config {
                log::info!("TLS enabled for internal gRPC API (TTL not supported)");

                server = server.tls_config(config)?;
            } else {
                log::info!("TLS disabled for internal gRPC API");
            };

            // The stack of middleware that our service will be wrapped in
            let middleware_layer = tower::ServiceBuilder::new()
                .layer(logging::LoggingMiddlewareLayer::new())
                .layer(tonic_telemetry::TonicTelemetryLayer::new(
                    telemetry_collector,
                ))
                .into_inner();

            server
                .layer(middleware_layer)
                .add_service(
                    QdrantServer::new(qdrant_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .max_decoding_message_size(usize::MAX),
                )
                .add_service(
                    CollectionsInternalServer::new(collections_internal_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .max_decoding_message_size(usize::MAX),
                )
                .add_service(
                    PointsInternalServer::new(points_internal_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .max_decoding_message_size(usize::MAX),
                )
                .add_service(
                    RaftServer::new(raft_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .max_decoding_message_size(usize::MAX),
                )
                .serve_with_shutdown(socket, async {
                    wait_stop_signal("internal gRPC").await;
                })
                .await
        })
        .unwrap();
    Ok(())
}
