mod api;
mod api_key;
mod logging;
mod tonic_telemetry;

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use ::api::grpc::grpc_health_v1::health_check_response::ServingStatus;
use ::api::grpc::grpc_health_v1::health_server::{Health, HealthServer};
use ::api::grpc::grpc_health_v1::{
    HealthCheckRequest as ProtocolHealthCheckRequest,
    HealthCheckResponse as ProtocolHealthCheckResponse,
};
use ::api::grpc::models::VersionInfo;
use ::api::grpc::qdrant::collections_internal_server::CollectionsInternalServer;
use ::api::grpc::qdrant::collections_server::CollectionsServer;
use ::api::grpc::qdrant::points_internal_server::PointsInternalServer;
use ::api::grpc::qdrant::points_server::PointsServer;
use ::api::grpc::qdrant::qdrant_internal_server::{QdrantInternal, QdrantInternalServer};
use ::api::grpc::qdrant::qdrant_server::{Qdrant, QdrantServer};
use ::api::grpc::qdrant::shard_snapshots_server::ShardSnapshotsServer;
use ::api::grpc::qdrant::snapshots_server::SnapshotsServer;
use ::api::grpc::qdrant::{
    HealthCheckReply, HealthCheckRequest, WaitOnConsensusCommitRequest,
    WaitOnConsensusCommitResponse,
};
use ::api::grpc::QDRANT_DESCRIPTOR_SET;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use tokio::runtime::Handle;
use tokio::signal;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::common::auth::AuthKeys;
use crate::common::helpers;
use crate::common::http_client::HttpClient;
use crate::common::telemetry_ops::requests_telemetry::TonicTelemetryCollector;
use crate::settings::Settings;
use crate::tonic::api::collections_api::CollectionsService;
use crate::tonic::api::collections_internal_api::CollectionsInternalService;
use crate::tonic::api::points_api::PointsService;
use crate::tonic::api::points_internal_api::PointsInternalService;
use crate::tonic::api::snapshots_api::{ShardSnapshotsService, SnapshotsService};

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

// Additional health check service that follows gRPC health check protocol as described in #2614
#[derive(Default)]
pub struct HealthService {}

#[tonic::async_trait]
impl Health for HealthService {
    async fn check(
        &self,
        _request: Request<ProtocolHealthCheckRequest>,
    ) -> Result<Response<ProtocolHealthCheckResponse>, Status> {
        let response = ProtocolHealthCheckResponse {
            status: ServingStatus::Serving as i32,
        };

        Ok(Response::new(response))
    }
}

pub struct QdrantInternalService {
    /// Qdrant settings
    settings: Settings,
    /// Consensus state
    consensus_state: ConsensusStateRef,
}

impl QdrantInternalService {
    fn new(settings: Settings, consensus_state: ConsensusStateRef) -> Self {
        Self {
            settings,
            consensus_state,
        }
    }
}

#[tonic::async_trait]
impl QdrantInternal for QdrantInternalService {
    async fn wait_on_consensus_commit(
        &self,
        request: Request<WaitOnConsensusCommitRequest>,
    ) -> Result<Response<WaitOnConsensusCommitResponse>, Status> {
        let request = request.into_inner();
        let commit = request.commit as u64;
        let term = request.term as u64;
        let timeout = Duration::from_secs(request.timeout as u64);
        let consensus_tick = Duration::from_millis(self.settings.cluster.consensus.tick_period_ms);
        let ok = self
            .consensus_state
            .wait_for_consensus_commit(commit, term, consensus_tick, timeout)
            .await
            .is_ok();
        Ok(Response::new(WaitOnConsensusCommitResponse { ok }))
    }
}

#[cfg(not(unix))]
async fn wait_stop_signal(for_what: &str) {
    signal::ctrl_c().await.unwrap();
    log::debug!("Stopping {for_what} on SIGINT");
}

#[cfg(unix)]
async fn wait_stop_signal(for_what: &str) {
    let mut term = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
    let mut inrt = signal::unix::signal(signal::unix::SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = term.recv() => log::debug!("Stopping {for_what} on SIGTERM"),
        _ = inrt.recv() => log::debug!("Stopping {for_what} on SIGINT"),
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
        let health_service = HealthService::default();
        let collections_service = CollectionsService::new(dispatcher.clone());
        let points_service = PointsService::new(dispatcher.clone());
        let snapshot_service = SnapshotsService::new(dispatcher.clone());

        // Only advertise the public services. By default, all services in QDRANT_DESCRIPTOR_SET
        // will be advertised, so explicitly list the services to be included.
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(QDRANT_DESCRIPTOR_SET)
            .with_service_name("qdrant.Collections")
            .with_service_name("qdrant.Points")
            .with_service_name("qdrant.Snapshots")
            .with_service_name("qdrant.Qdrant")
            .with_service_name("grpc.health.v1.Health")
            .build()
            .unwrap();

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
            .option_layer({
                AuthKeys::try_create(&settings.service).map(api_key::ApiKeyMiddlewareLayer::new)
            })
            .into_inner();

        server
            .layer(middleware_layer)
            .add_service(reflection_service)
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
            .add_service(
                HealthServer::new(health_service)
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
    settings: Settings,
    host: String,
    internal_grpc_port: u16,
    tls_config: Option<ServerTlsConfig>,
    to_consensus: tokio::sync::mpsc::Sender<crate::consensus::Message>,
    runtime: Handle,
) -> std::io::Result<()> {
    use ::api::grpc::qdrant::raft_server::RaftServer;

    use crate::tonic::api::raft_api::RaftService;

    let http_client = HttpClient::from_settings(&settings)?;

    runtime
        .block_on(async {
            let socket = SocketAddr::from((host.parse::<IpAddr>().unwrap(), internal_grpc_port));

            let qdrant_service = QdrantService::default();
            let qdrant_internal_service =
                QdrantInternalService::new(settings, consensus_state.clone());
            let collections_internal_service = CollectionsInternalService::new(toc.clone());
            let points_internal_service = PointsInternalService::new(toc.clone());
            let shard_snapshots_service = ShardSnapshotsService::new(toc.clone(), http_client);
            let raft_service = RaftService::new(to_consensus, consensus_state);

            log::debug!("Qdrant internal gRPC listening on {}", internal_grpc_port);

            let mut server = Server::builder()
                // Internally use a high limit for pending accept streams.
                // We can have a huge number of reset/dropped HTTP2 streams in our internal
                // communication when there are a lot of clients dropping connections. This
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
                    QdrantInternalServer::new(qdrant_internal_service)
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
                    ShardSnapshotsServer::new(shard_snapshots_service)
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
