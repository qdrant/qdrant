use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::points_server::Points;
use api::grpc::qdrant::{
    ClearPayloadPoints, CountPoints, CountResponse, CreateFieldIndexCollection,
    DeleteFieldIndexCollection, DeletePayloadPoints, DeletePointVectors, DeletePoints,
    DiscoverBatchPoints, DiscoverBatchResponse, DiscoverPoints, DiscoverResponse, FacetCounts,
    FacetResponse, GetPoints, GetResponse, PointsOperationResponse, QueryBatchPoints,
    QueryBatchResponse, QueryGroupsResponse, QueryPointGroups, QueryPoints, QueryResponse,
    RecommendBatchPoints, RecommendBatchResponse, RecommendGroupsResponse, RecommendPointGroups,
    RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse, SearchBatchPoints,
    SearchBatchResponse, SearchGroupsResponse, SearchMatrixOffsets, SearchMatrixOffsetsResponse,
    SearchMatrixPairs, SearchMatrixPairsResponse, SearchMatrixPoints, SearchPointGroups,
    SearchPoints, SearchResponse, SetPayloadPoints, UpdateBatchPoints, UpdateBatchResponse,
    UpdatePointVectors, UpsertPoints,
};
use collection::operations::types::CoreSearchRequest;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status};

use super::points_common::{
    delete_vectors, discover, discover_batch, facet, query, query_batch, query_groups,
    recommend_groups, scroll, search_groups, search_points_matrix, update_batch, update_vectors,
};
use super::validate;
use crate::settings::ServiceConfig;
use crate::tonic::api::points_common::{
    clear_payload, convert_shard_selector_for_read, core_search_batch, count, create_field_index,
    delete, delete_field_index, delete_payload, get, overwrite_payload, recommend, recommend_batch,
    search, set_payload, upsert,
};
use crate::tonic::auth::extract_access;
use crate::tonic::verification::StrictModeCheckedTocProvider;

pub struct PointsService {
    dispatcher: Arc<Dispatcher>,
    service_config: ServiceConfig,
}

impl PointsService {
    pub fn new(dispatcher: Arc<Dispatcher>, service_config: ServiceConfig) -> Self {
        Self {
            dispatcher,
            service_config,
        }
    }

    fn get_request_collection_hw_usage_counter(&self, collection_name: String) -> RequestHwCounter {
        let counter = HwMeasurementAcc::new_with_drain(
            &self.dispatcher.get_collection_hw_metrics(collection_name),
        );

        RequestHwCounter::new(counter, self.service_config.hardware_reporting(), false)
    }
}

#[tonic::async_trait]
impl Points for PointsService {
    async fn upsert(
        &self,
        mut request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        upsert(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete(
        &self,
        mut request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        delete(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn get(&self, mut request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        get(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
        )
        .await
    }

    async fn update_vectors(
        &self,
        mut request: Request<UpdatePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        // Nothing to verify here.

        let access = extract_access(&mut request);

        update_vectors(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_vectors(
        &self,
        mut request: Request<DeletePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        delete_vectors(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn set_payload(
        &self,
        mut request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        set_payload(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn overwrite_payload(
        &self,
        mut request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        overwrite_payload(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_payload(
        &self,
        mut request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        delete_payload(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn clear_payload(
        &self,
        mut request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        clear_payload(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn update_batch(
        &self,
        mut request: Request<UpdateBatchPoints>,
    ) -> Result<Response<UpdateBatchResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        update_batch(&self.dispatcher, request.into_inner(), None, None, access).await
    }

    async fn create_field_index(
        &self,
        mut request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        create_field_index(
            self.dispatcher.clone(),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn delete_field_index(
        &self,
        mut request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

        delete_field_index(
            self.dispatcher.clone(),
            request.into_inner(),
            None,
            None,
            access,
        )
        .await
        .map(|resp| resp.map(Into::into))
    }

    async fn search(
        &self,
        mut request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);

        let res = search(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn search_batch(
        &self,
        mut request: Request<SearchBatchPoints>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);

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

        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name.clone());

        let res = core_search_batch(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            &collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn search_groups(
        &self,
        mut request: Request<SearchPointGroups>,
    ) -> Result<Response<SearchGroupsResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let res = search_groups(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn scroll(
        &self,
        mut request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);
        scroll(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
        )
        .await
    }

    async fn recommend(
        &self,
        mut request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let res = recommend(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn recommend_batch(
        &self,
        mut request: Request<RecommendBatchPoints>,
    ) -> Result<Response<RecommendBatchResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let RecommendBatchPoints {
            collection_name,
            recommend_points,
            read_consistency,
            timeout,
        } = request.into_inner();

        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name.clone());

        let res = recommend_batch(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            &collection_name,
            recommend_points,
            read_consistency,
            access,
            timeout.map(Duration::from_secs),
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn recommend_groups(
        &self,
        mut request: Request<RecommendPointGroups>,
    ) -> Result<Response<RecommendGroupsResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);

        let res = recommend_groups(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn discover(
        &self,
        mut request: Request<DiscoverPoints>,
    ) -> Result<Response<DiscoverResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();

        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let res = discover(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn discover_batch(
        &self,
        mut request: Request<DiscoverBatchPoints>,
    ) -> Result<Response<DiscoverBatchResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let DiscoverBatchPoints {
            collection_name,
            discover_points,
            read_consistency,
            timeout,
        } = request.into_inner();

        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name.clone());
        let res = discover_batch(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            &collection_name,
            discover_points,
            read_consistency,
            access,
            timeout.map(Duration::from_secs),
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn count(
        &self,
        mut request: Request<CountPoints>,
    ) -> Result<Response<CountResponse>, Status> {
        validate(request.get_ref())?;

        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let res = count(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            &access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn query(
        &self,
        mut request: Request<QueryPoints>,
    ) -> Result<Response<QueryResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);

        let res = query(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn query_batch(
        &self,
        mut request: Request<QueryBatchPoints>,
    ) -> Result<Response<QueryBatchResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let request = request.into_inner();
        let QueryBatchPoints {
            collection_name,
            query_points,
            read_consistency,
            timeout,
        } = request;
        let timeout = timeout.map(Duration::from_secs);
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name.clone());
        let res = query_batch(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            &collection_name,
            query_points,
            read_consistency,
            access,
            timeout,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }

    async fn query_groups(
        &self,
        mut request: Request<QueryPointGroups>,
    ) -> Result<Response<QueryGroupsResponse>, Status> {
        let access = extract_access(&mut request);
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);

        let res = query_groups(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            None,
            access,
            hw_metrics,
        )
        .await?;

        Ok(res)
    }
    async fn facet(
        &self,
        mut request: Request<FacetCounts>,
    ) -> Result<Response<FacetResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        facet(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
        )
        .await
    }

    async fn search_matrix_pairs(
        &self,
        mut request: Request<SearchMatrixPoints>,
    ) -> Result<Response<SearchMatrixPairsResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let timing = Instant::now();
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let search_matrix_response = search_points_matrix(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
            hw_metrics.get_counter(),
        )
        .await?;

        let pairs_response = SearchMatrixPairsResponse {
            result: Some(SearchMatrixPairs::from(search_matrix_response)),
            time: timing.elapsed().as_secs_f64(),
            usage: hw_metrics.to_grpc_api(),
        };

        Ok(Response::new(pairs_response))
    }

    async fn search_matrix_offsets(
        &self,
        mut request: Request<SearchMatrixPoints>,
    ) -> Result<Response<SearchMatrixOffsetsResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let timing = Instant::now();
        let collection_name = request.get_ref().collection_name.clone();
        let hw_metrics = self.get_request_collection_hw_usage_counter(collection_name);
        let search_matrix_response = search_points_matrix(
            StrictModeCheckedTocProvider::new(&self.dispatcher),
            request.into_inner(),
            access,
            hw_metrics.get_counter(),
        )
        .await?;

        let offsets_response = SearchMatrixOffsetsResponse {
            result: Some(SearchMatrixOffsets::from(search_matrix_response)),
            time: timing.elapsed().as_secs_f64(),
            usage: hw_metrics.to_grpc_api(),
        };

        Ok(Response::new(offsets_response))
    }
}
