use std::sync::Arc;

use api::grpc::qdrant::points_server::Points;
use api::grpc::qdrant::{
    ClearPayloadPoints, CountPoints, CountResponse, CreateFieldIndexCollection,
    DeleteFieldIndexCollection, DeletePayloadPoints, DeletePoints, GetPoints, GetResponse,
    PointsOperationResponse, RecommendBatchPoints, RecommendBatchResponse, RecommendPoints,
    RecommendResponse, ScrollPoints, ScrollResponse, SearchBatchPoints, SearchBatchResponse,
    SearchPoints, SearchResponse, SetPayloadPoints, UpsertPoints,
};
use storage::content_manager::toc::TableOfContent;
use tonic::{Request, Response, Status};

use crate::tonic::api::points_common::{
    clear_payload, count, create_field_index, delete, delete_field_index, delete_payload, get,
    overwrite_payload, recommend, recommend_batch, scroll, search, search_batch, set_payload,
    upsert,
};

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

    async fn overwrite_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        overwrite_payload(self.toc.as_ref(), request.into_inner(), None).await
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

    async fn search_batch(
        &self,
        request: Request<SearchBatchPoints>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        let SearchBatchPoints {
            collection_name,
            search_points,
            read_consistency,
        } = request.into_inner();
        search_batch(
            self.toc.as_ref(),
            collection_name,
            search_points,
            read_consistency,
            None,
        )
        .await
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
        recommend(self.toc.as_ref(), request.into_inner()).await
    }

    async fn recommend_batch(
        &self,
        request: Request<RecommendBatchPoints>,
    ) -> Result<Response<RecommendBatchResponse>, Status> {
        let RecommendBatchPoints {
            collection_name,
            recommend_points,
            read_consistency,
        } = request.into_inner();
        recommend_batch(
            self.toc.as_ref(),
            collection_name,
            recommend_points,
            read_consistency,
        )
        .await
    }

    async fn count(
        &self,
        request: Request<CountPoints>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.toc.as_ref(), request.into_inner(), None).await
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
