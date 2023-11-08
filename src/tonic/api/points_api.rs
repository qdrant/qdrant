use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::points_server::Points;
use api::grpc::qdrant::{
    ClearPayloadPoints, CountPoints, CountResponse, CreateFieldIndexCollection,
    DeleteFieldIndexCollection, DeletePayloadPoints, DeletePointVectors, DeletePoints,
    DiscoverBatchPoints, DiscoverBatchResponse, DiscoverPoints, DiscoverResponse, GetPoints,
    GetResponse, PointsOperationResponse, RecommendBatchPoints, RecommendBatchResponse,
    RecommendGroupsResponse, RecommendPointGroups, RecommendPoints, RecommendResponse,
    ScrollPoints, ScrollResponse, SearchBatchPoints, SearchBatchResponse, SearchGroupsResponse,
    SearchPointGroups, SearchPoints, SearchResponse, SetPayloadPoints, UpdateBatchPoints,
    UpdateBatchResponse, UpdatePointVectors, UpsertPoints,
};
use storage::content_manager::toc::TableOfContent;
use tonic::{Request, Response, Status};

use super::points_common::{
    delete_vectors, discover, discover_batch, recommend_groups, search_groups, update_batch,
    update_vectors,
};
use super::validate;
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
        validate(request.get_ref())?;
        upsert(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete(
        &self,
        request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        delete(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn get(&self, request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        validate(request.get_ref())?;
        get(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn update_vectors(
        &self,
        request: Request<UpdatePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        update_vectors(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete_vectors(
        &self,
        request: Request<DeletePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        delete_vectors(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        set_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn overwrite_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        overwrite_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        delete_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        clear_payload(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn update_batch(
        &self,
        request: Request<UpdateBatchPoints>,
    ) -> Result<Response<UpdateBatchResponse>, Status> {
        validate(request.get_ref())?;
        update_batch(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        create_field_index(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;
        delete_field_index(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn search(
        &self,
        request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        validate(request.get_ref())?;
        search(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn search_batch(
        &self,
        request: Request<SearchBatchPoints>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        validate(request.get_ref())?;
        let SearchBatchPoints {
            collection_name,
            search_points,
            read_consistency,
            timeout,
        } = request.into_inner();

        let timeout = timeout.map(Duration::from_secs);

        search_batch(
            self.toc.as_ref(),
            collection_name,
            search_points,
            read_consistency,
            None,
            timeout,
        )
        .await
    }

    async fn search_groups(
        &self,
        request: Request<SearchPointGroups>,
    ) -> Result<Response<SearchGroupsResponse>, Status> {
        validate(request.get_ref())?;
        search_groups(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn scroll(
        &self,
        request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        validate(request.get_ref())?;
        scroll(self.toc.as_ref(), request.into_inner(), None).await
    }

    async fn recommend(
        &self,
        request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        validate(request.get_ref())?;
        recommend(self.toc.as_ref(), request.into_inner()).await
    }

    async fn recommend_batch(
        &self,
        request: Request<RecommendBatchPoints>,
    ) -> Result<Response<RecommendBatchResponse>, Status> {
        validate(request.get_ref())?;
        let RecommendBatchPoints {
            collection_name,
            recommend_points,
            read_consistency,
            timeout,
        } = request.into_inner();
        recommend_batch(
            self.toc.as_ref(),
            collection_name,
            recommend_points,
            read_consistency,
            timeout.map(Duration::from_secs),
        )
        .await
    }

    async fn recommend_groups(
        &self,
        request: Request<RecommendPointGroups>,
    ) -> Result<Response<RecommendGroupsResponse>, Status> {
        validate(request.get_ref())?;
        recommend_groups(self.toc.as_ref(), request.into_inner()).await
    }

    async fn discover(
        &self,
        request: Request<DiscoverPoints>,
    ) -> Result<Response<DiscoverResponse>, Status> {
        validate(request.get_ref())?;
        discover(self.toc.as_ref(), request.into_inner()).await
    }

    async fn discover_batch(
        &self,
        request: Request<DiscoverBatchPoints>,
    ) -> Result<Response<DiscoverBatchResponse>, Status> {
        validate(request.get_ref())?;
        let DiscoverBatchPoints {
            collection_name,
            discover_points,
            read_consistency,
            timeout,
        } = request.into_inner();
        discover_batch(
            self.toc.as_ref(),
            collection_name,
            discover_points,
            read_consistency,
            timeout.map(Duration::from_secs),
        )
        .await
    }

    async fn count(
        &self,
        request: Request<CountPoints>,
    ) -> Result<Response<CountResponse>, Status> {
        validate(request.get_ref())?;
        count(self.toc.as_ref(), request.into_inner(), None).await
    }
}
