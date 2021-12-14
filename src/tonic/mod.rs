mod api;
pub mod qdrant;

use crate::common::models::VersionInfo;
use crate::settings::Settings;
use crate::tonic::api::collections_api::CollectionsService;
use crate::tonic::api::points_api::PointsService;
use qdrant::collections_server::CollectionsServer;
use qdrant::points_server::PointsServer;
use qdrant::qdrant_server::{Qdrant, QdrantServer};
use qdrant::{HealthCheckReply, HealthCheckRequest};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;
use tokio::{runtime, signal};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Default)]
pub struct QdrantService {}

impl From<VersionInfo> for HealthCheckReply {
    fn from(info: VersionInfo) -> Self {
        HealthCheckReply {
            title: info.title,
            version: info.version,
        }
    }
}

#[tonic::async_trait]
impl Qdrant for QdrantService {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(VersionInfo::default().into()))
    }
}

pub fn init(toc: Arc<TableOfContent>, settings: Settings) -> std::io::Result<()> {
    let tonic_runtime = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    tonic_runtime
        .block_on(async {
            let socket = SocketAddr::from((
                settings.service.host.parse::<IpAddr>().unwrap(),
                settings.service.grpc_port,
            ));

            let service = QdrantService::default();
            let collections_service = CollectionsService::new(toc.clone());
            let points_service = PointsService::new(toc.clone());

            info!("qdrant grpc listening on {}", settings.service.grpc_port);

            Server::builder()
                .add_service(QdrantServer::new(service))
                .add_service(CollectionsServer::new(collections_service))
                .add_service(PointsServer::new(points_service))
                .serve_with_shutdown(socket, async {
                    signal::ctrl_c().await.unwrap();
                    info!("Stopping gRPC");
                })
                .await
        })
        .unwrap();
    Ok(())
}
