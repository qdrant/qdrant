mod api;
mod tonic_telemetry;

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
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::common::telemetry_ops::requests_telemetry::TonicTelemetryCollector;
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

pub fn init(
    dispatcher: Arc<Dispatcher>,
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    host: String,
    grpc_port: u16,
    runtime: Handle,
) -> std::io::Result<()> {
    runtime
        .block_on(async {
            let socket = SocketAddr::from((host.parse::<IpAddr>().unwrap(), grpc_port));

            let qdrant_service = QdrantService::default();
            let collections_service = CollectionsService::new(dispatcher.clone());
            let points_service = PointsService::new(dispatcher.toc().clone());
            let snapshot_service = SnapshotsService::new(dispatcher.toc().clone());

            log::info!("Qdrant gRPC listening on {}", grpc_port);

            Server::builder()
                .layer(tonic_telemetry::TonicTelemetryLayer::new(
                    telemetry_collector,
                ))
                .add_service(
                    QdrantServer::new(qdrant_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    CollectionsServer::new(collections_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    PointsServer::new(points_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    SnapshotsServer::new(snapshot_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_shutdown(socket, async {
                    signal::ctrl_c().await.unwrap();
                    log::debug!("Stopping gRPC");
                })
                .await
        })
        .unwrap();
    Ok(())
}

pub fn init_internal(
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    host: String,
    internal_grpc_port: u16,
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

            Server::builder()
                .layer(tonic_telemetry::TonicTelemetryLayer::new(
                    telemetry_collector,
                ))
                .add_service(
                    QdrantServer::new(qdrant_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    CollectionsInternalServer::new(collections_internal_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    PointsInternalServer::new(points_internal_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    RaftServer::new(raft_service)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_shutdown(socket, async {
                    signal::ctrl_c().await.unwrap();
                    log::debug!("Stopping internal gRPC");
                })
                .await
        })
        .unwrap();
    Ok(())
}
