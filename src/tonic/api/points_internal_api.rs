use tonic::{Request, Response, Status};

use crate::tonic::api::points_common::upsert;
use api::grpc::qdrant::points_internal_server::PointsInternal;
use api::grpc::qdrant::{PointsOperationResponse, UpsertPointsInternal};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;

pub struct PointsInternalService {
    toc: Arc<TableOfContent>,
}

impl PointsInternalService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl PointsInternal for PointsInternalService {
    async fn upsert(
        &self,
        request: Request<UpsertPointsInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let UpsertPointsInternal {
            upsert_points,
            shard_id,
        } = request.into_inner();

        let upsert_points =
            upsert_points.ok_or_else(|| Status::invalid_argument("UpsertPoints is missing"))?;

        upsert(self.toc.as_ref(), upsert_points, Some(shard_id)).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_grpc() {
        // For running build from IDE
        eprintln!("hello");
    }
}
