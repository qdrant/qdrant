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
use collection::operations::types::CoreSearchRequest;
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status};

use super::points_common::{
    delete_vectors, discover, discover_batch, recommend_groups, search_groups, update_batch,
    update_vectors,
};
use super::validate;
use crate::tonic::api::points_common::{
    clear_payload, convert_shard_selector_for_read, core_search_batch, count, create_field_index,
    delete, delete_field_index, delete_payload, get, overwrite_payload, recommend, recommend_batch,
    scroll, search, set_payload, upsert,
};
use crate::tonic::auth::extract_claims;

pub struct PointsService {
    dispatcher: Arc<Dispatcher>,
}

impl PointsService {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[tonic::async_trait]
impl Points for PointsService {
    async fn upsert(
        &self,
        mut request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        upsert(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete(
        &self,
        mut request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        delete(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn get(&self, mut request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        get(self.dispatcher.as_ref(), request.into_inner(), None, claims).await
    }

    async fn update_vectors(
        &self,
        mut request: Request<UpdatePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        update_vectors(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_vectors(
        &self,
        mut request: Request<DeletePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        delete_vectors(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn set_payload(
        &self,
        mut request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        set_payload(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn overwrite_payload(
        &self,
        mut request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        overwrite_payload(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_payload(
        &self,
        mut request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        delete_payload(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn clear_payload(
        &self,
        mut request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        clear_payload(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn update_batch(
        &self,
        mut request: Request<UpdateBatchPoints>,
    ) -> Result<Response<UpdateBatchResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        update_batch(
            self.dispatcher.toc().clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
    }

    async fn create_field_index(
        &self,
        mut request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        create_field_index(
            self.dispatcher.clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_field_index(
        &self,
        mut request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        delete_field_index(
            self.dispatcher.clone(),
            request.into_inner(),
            None,
            None,
            claims,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn search(
        &self,
        mut request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        validate(request.get_ref())?;
        let claims = extract_claims(&mut request);
        search(self.dispatcher.as_ref(), request.into_inner(), None, claims).await
    }

    async fn search_batch(
        &self,
        mut request: Request<SearchBatchPoints>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        let SearchBatchPoints {
            collection_name,
            search_points,
            read_consistency,
            timeout,
        } = request.into_inner();

        let timeout = timeout.map(Duration::from_secs);

        let mut requests = Vec::new();

        for mut search_point in search_points {
            let shard_key = search_point.shard_key_selector.take();

            let shard_selector = convert_shard_selector_for_read(None, shard_key);
            let core_search_request = CoreSearchRequest::try_from(search_point)?;

            requests.push((core_search_request, shard_selector));
        }

        core_search_batch(
            self.dispatcher.as_ref(),
            collection_name,
            requests,
            read_consistency,
            claims,
            timeout,
        )
        .await
    }

    async fn search_groups(
        &self,
        mut request: Request<SearchPointGroups>,
    ) -> Result<Response<SearchGroupsResponse>, Status> {
        validate(request.get_ref())?;
        let claims = extract_claims(&mut request);
        search_groups(self.dispatcher.as_ref(), request.into_inner(), None, claims).await
    }

    async fn scroll(
        &self,
        mut request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        scroll(self.dispatcher.as_ref(), request.into_inner(), None, claims).await
    }

    async fn recommend(
        &self,
        mut request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        validate(request.get_ref())?;
        let claims = extract_claims(&mut request);
        recommend(self.dispatcher.as_ref(), request.into_inner(), claims).await
    }

    async fn recommend_batch(
        &self,
        mut request: Request<RecommendBatchPoints>,
    ) -> Result<Response<RecommendBatchResponse>, Status> {
        validate(request.get_ref())?;
        let claims = extract_claims(&mut request);
        let RecommendBatchPoints {
            collection_name,
            recommend_points,
            read_consistency,
            timeout,
        } = request.into_inner();
        recommend_batch(
            self.dispatcher.as_ref(),
            collection_name,
            recommend_points,
            read_consistency,
            claims,
            timeout.map(Duration::from_secs),
        )
        .await
    }

    async fn recommend_groups(
        &self,
        mut request: Request<RecommendPointGroups>,
    ) -> Result<Response<RecommendGroupsResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        recommend_groups(self.dispatcher.as_ref(), request.into_inner(), claims).await
    }

    async fn discover(
        &self,
        mut request: Request<DiscoverPoints>,
    ) -> Result<Response<DiscoverResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        discover(self.dispatcher.as_ref(), request.into_inner(), claims).await
    }

    async fn discover_batch(
        &self,
        mut request: Request<DiscoverBatchPoints>,
    ) -> Result<Response<DiscoverBatchResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        let DiscoverBatchPoints {
            collection_name,
            discover_points,
            read_consistency,
            timeout,
        } = request.into_inner();

        discover_batch(
            self.dispatcher.as_ref(),
            collection_name,
            discover_points,
            read_consistency,
            claims,
            timeout.map(Duration::from_secs),
        )
        .await
    }

    async fn count(
        &self,
        mut request: Request<CountPoints>,
    ) -> Result<Response<CountResponse>, Status> {
        validate(request.get_ref())?;

        let claims = extract_claims(&mut request);

        count(self.dispatcher.as_ref(), request.into_inner(), None, claims).await
    }
}
