use tonic::{Request, Response, Status};

use crate::tonic::api::points_common::{
    clear_payload, create_field_index, delete, delete_field_index, delete_payload, set_payload,
    upsert,
};
use api::grpc::qdrant::points_internal_server::PointsInternal;
use api::grpc::qdrant::{
    ClearPayloadPointsInternal, CreateFieldIndexCollectionInternal,
    DeleteFieldIndexCollectionInternal, DeletePayloadPointsInternal, DeletePointsInternal,
    PointsOperationResponse, SetPayloadPointsInternal, UpsertPointsInternal,
};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;

/// This API is intended for P2P communication within a distributed deployment.
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

    async fn delete(
        &self,
        request: Request<DeletePointsInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePointsInternal {
            delete_points,
            shard_id,
        } = request.into_inner();

        let delete_points =
            delete_points.ok_or_else(|| Status::invalid_argument("DeletePoints is missing"))?;

        delete(self.toc.as_ref(), delete_points, Some(shard_id)).await
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let SetPayloadPointsInternal {
            set_payload_points,
            shard_id,
        } = request.into_inner();

        let set_payload_points = set_payload_points
            .ok_or_else(|| Status::invalid_argument("SetPayloadPoints is missing"))?;

        set_payload(self.toc.as_ref(), set_payload_points, Some(shard_id)).await
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePayloadPointsInternal {
            delete_payload_points,
            shard_id,
        } = request.into_inner();

        let delete_payload_points = delete_payload_points
            .ok_or_else(|| Status::invalid_argument("DeletePayloadPoints is missing"))?;

        delete_payload(self.toc.as_ref(), delete_payload_points, Some(shard_id)).await
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let ClearPayloadPointsInternal {
            clear_payload_points,
            shard_id,
        } = request.into_inner();

        let clear_payload_points = clear_payload_points
            .ok_or_else(|| Status::invalid_argument("ClearPayloadPoints is missing"))?;

        clear_payload(self.toc.as_ref(), clear_payload_points, Some(shard_id)).await
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let CreateFieldIndexCollectionInternal {
            create_field_index_collection,
            shard_id,
        } = request.into_inner();

        let create_field_index_collection = create_field_index_collection
            .ok_or_else(|| Status::invalid_argument("CreateFieldIndexCollection is missing"))?;

        create_field_index(
            self.toc.as_ref(),
            create_field_index_collection,
            Some(shard_id),
        )
        .await
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeleteFieldIndexCollectionInternal {
            delete_field_index_collection,
            shard_id,
        } = request.into_inner();

        let delete_field_index_collection = delete_field_index_collection
            .ok_or_else(|| Status::invalid_argument("DeleteFieldIndexCollection is missing"))?;

        delete_field_index(
            self.toc.as_ref(),
            delete_field_index_collection,
            Some(shard_id),
        )
        .await
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
