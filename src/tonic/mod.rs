mod api;

use crate::tonic::api::collections_api::CollectionsService;
#[cfg(feature = "consensus")]
use crate::tonic::api::collections_internal_api::CollectionsInternalService;
use crate::tonic::api::points_api::PointsService;
#[cfg(feature = "consensus")]
use crate::tonic::api::points_internal_api::PointsInternalService;
use ::api::grpc::models::VersionInfo;
#[cfg(feature = "consensus")]
use ::api::grpc::qdrant::collections_internal_server::CollectionsInternalServer;
use ::api::grpc::qdrant::collections_server::CollectionsServer;
#[cfg(feature = "consensus")]
use ::api::grpc::qdrant::points_internal_server::PointsInternalServer;
use ::api::grpc::qdrant::points_server::PointsServer;
use ::api::grpc::qdrant::qdrant_server::{Qdrant, QdrantServer};
use ::api::grpc::qdrant::{HealthCheckReply, HealthCheckRequest};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;
use tokio::{runtime, signal};
use tonic::{transport::Server, Request, Response, Status};

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

pub fn init(toc: Arc<TableOfContent>, host: String, grpc_port: u16) -> std::io::Result<()> {
    let tonic_runtime = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("tonic-{}", id)
        })
        .build()?;
    tonic_runtime
        .block_on(async {
            let socket = SocketAddr::from((host.parse::<IpAddr>().unwrap(), grpc_port));

            let service = QdrantService::default();
            let collections_service = CollectionsService::new(toc.clone());
            let points_service = PointsService::new(toc.clone());

            log::info!("Qdrant gRPC listening on {}", grpc_port);

            Server::builder()
                .add_service(QdrantServer::new(service))
                .add_service(CollectionsServer::new(collections_service))
                .add_service(PointsServer::new(points_service))
                .serve_with_shutdown(socket, async {
                    signal::ctrl_c().await.unwrap();
                    log::info!("Stopping gRPC");
                })
                .await
        })
        .unwrap();
    Ok(())
}

#[cfg(feature = "consensus")]
pub fn init_internal(
    toc: Arc<TableOfContent>,
    host: String,
    internal_grpc_port: u16,
    to_consensus: std::sync::mpsc::SyncSender<crate::consensus::Message>,
) -> std::io::Result<()> {
    use crate::tonic::api::raft_api::RaftService;
    use ::api::grpc::qdrant::raft_server::RaftServer;

    let tonic_runtime = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("tonic-internal-{}", id)
        })
        .build()?;
    tonic_runtime
        .block_on(async {
            let socket = SocketAddr::from((host.parse::<IpAddr>().unwrap(), internal_grpc_port));

            let service = QdrantService::default();
            let collections_internal_service = CollectionsInternalService::new(toc.clone());
            let points_internal_service = PointsInternalService::new(toc.clone());
            let raft_service = RaftService::new(to_consensus, toc.clone());

            log::info!("Qdrant internal gRPC listening on {}", internal_grpc_port);

            Server::builder()
                .add_service(QdrantServer::new(service))
                .add_service(CollectionsInternalServer::new(collections_internal_service))
                .add_service(PointsInternalServer::new(points_internal_service))
                .add_service(RaftServer::new(raft_service))
                .serve_with_shutdown(socket, async {
                    signal::ctrl_c().await.unwrap();
                    log::info!("Stopping internal gRPC");
                })
                .await
        })
        .unwrap();
    Ok(())
}
