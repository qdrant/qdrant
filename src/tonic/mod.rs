use tonic::{transport::Server, Request, Response, Status};

use crate::common::models::VersionInfo;
use crate::settings::Settings;

use proto::qdrant_server::{Qdrant, QdrantServer};
use proto::{HealthCheckReply, HealthCheckRequest};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;
use tokio::runtime;

pub mod proto {
    tonic::include_proto!("qdrant");
}

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

pub fn init(_toc: Arc<TableOfContent>, settings: Settings) -> std::io::Result<()> {
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

            info!("qdrant grpc listening on {}", settings.service.grpc_port);

            Server::builder()
                .add_service(QdrantServer::new(service))
                .serve(socket)
                .await
        })
        .unwrap();
    Ok(())
}
