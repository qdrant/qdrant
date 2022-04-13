use tonic::{Request, Response, Status};

use api::grpc::qdrant::points_server::Points;

use crate::tonic::api::points_common::{
    clear_payload, create_field_index, delete, delete_field_index, delete_payload, get, recommend,
    scroll, search, set_payload, upsert,
};
use api::grpc::qdrant::{
    ClearPayloadPoints, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePoints, GetPoints, GetResponse, PointsOperationResponse,
    RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse, SearchPoints, SearchResponse,
    SetPayloadPoints, UpsertPoints,
};
use std::sync::Arc;

use storage::content_manager::toc::TableOfContent;

pub struct PointsService {
    toc: Arc<TableOfContent>,
}

impl PointsService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl Points for PointsService {
    async fn upsert(
        &self,
        request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        upsert(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete(
        &self,
        request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        delete(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn get(&self, request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        get(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        set_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        delete_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        clear_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        create_field_index(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        delete_field_index(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn search(
        &self,
        request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        search(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn scroll(
        &self,
        request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        scroll(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn recommend(
        &self,
        request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        recommend(self.toc.as_ref(), request.into_inner(), None).await
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
