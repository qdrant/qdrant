use std::any;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::points_internal_server::PointsInternal;
use api::grpc::qdrant::{
    ClearPayloadPointsInternal, CoreSearchBatchPointsInternal, CountPointsInternal, CountResponse,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollectionInternal,
    DeletePayloadPointsInternal, DeletePointsInternal, DeleteVectorsInternal, FacetCountsInternal,
    FacetResponseInternal, GetPointsInternal, GetResponse, IntermediateResult,
    PointsOperationResponseInternal, QueryBatchPointsInternal, QueryBatchResponseInternal,
    QueryResultInternal, QueryShardPoints, RecommendPointsInternal, RecommendResponse,
    ScrollPointsInternal, ScrollResponse, SearchBatchResponse, SetPayloadPointsInternal,
    SyncPointsInternal, UpdateVectorsInternal, UpsertPointsInternal,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::shard_query::ShardQueryRequest;
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::json_path::JsonPath;
use segment::types::Filter;
use storage::content_manager::toc::TableOfContent;
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::rbac::Access;
use tonic::{Request, Response, Status};

use super::query_common::*;
use super::update_common::*;
use super::validate_and_log;
use crate::common::inference::extract_token;
use crate::common::update::InternalUpdateParams;
use crate::settings::ServiceConfig;
use crate::tonic::verification::{StrictModeCheckedInternalTocProvider, UncheckedTocProvider};

const FULL_ACCESS: Access = Access::full("Internal API");

/// This API is intended for P2P communication within a distributed deployment.
pub struct PointsInternalService {
    toc: Arc<TableOfContent>,
    service_config: ServiceConfig,
}

impl PointsInternalService {
    pub fn new(toc: Arc<TableOfContent>, service_config: ServiceConfig) -> Self {
        Self {
            toc,
            service_config,
        }
    }
}

pub async fn query_batch_internal(
    toc: &TableOfContent,
    collection_name: String,
    query_points: Vec<QueryShardPoints>,
    shard_selection: Option<ShardId>,
    timeout: Option<Duration>,
    request_hw_data: RequestHwCounter,
) -> Result<Response<QueryBatchResponseInternal>, Status> {
    let batch_requests: Vec<_> = query_points
        .into_iter()
        .map(ShardQueryRequest::try_from)
        .try_collect()?;

    let timing = Instant::now();

    // As this function is handling an internal request,
    // we can assume that shard_key is already resolved
    let shard_selection = match shard_selection {
        None => {
            debug_assert!(false, "Shard selection is expected for internal request");
            ShardSelectorInternal::All
        }
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
    };

    let batch_response = toc
        .query_batch_internal(
            &collection_name,
            batch_requests,
            shard_selection,
            timeout,
            request_hw_data.get_counter(),
        )
        .await?;

    let response = QueryBatchResponseInternal {
        results: batch_response
            .into_iter()
            .map(|response| QueryResultInternal {
                intermediate_results: response
                    .into_iter()
                    .map(|intermediate| IntermediateResult {
                        result: intermediate.into_iter().map(From::from).collect_vec(),
                    })
                    .collect_vec(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_data.to_grpc_api(),
    };

    Ok(Response::new(response))
}

async fn facet_counts_internal(
    toc: &TableOfContent,
    request: FacetCountsInternal,
    request_hw_data: RequestHwCounter,
) -> Result<Response<FacetResponseInternal>, Status> {
    let timing = Instant::now();

    let FacetCountsInternal {
        collection_name,
        key,
        filter,
        limit,
        exact,
        shard_id,
        timeout,
    } = request;

    let shard_selection = ShardSelectorInternal::ShardId(shard_id);

    let request = FacetParams {
        key: JsonPath::from_str(&key)
            .map_err(|_| Status::invalid_argument("Failed to parse facet key"))?,
        limit: limit as usize,
        filter: filter.map(Filter::try_from).transpose()?,
        exact,
    };

    let response = toc
        .facet_internal(
            &collection_name,
            request,
            shard_selection,
            timeout.map(Duration::from_secs),
            request_hw_data.get_counter(),
        )
        .await?;

    let FacetResponse { hits } = response;

    let response = FacetResponseInternal {
        hits: hits.into_iter().map(From::from).collect_vec(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_data.to_grpc_api(),
    };

    Ok(Response::new(response))
}

impl PointsInternalService {
    /// Generates a new `RequestHwCounter` for the request.
    /// This counter is indented to be used for internal requests.
    ///
    /// So, it collects the hardware usage to the collection's counter ONLY if it was not
    /// converted to a response.
    fn get_request_collection_hw_usage_counter_for_internal(
        &self,
        collection_name: String,
    ) -> RequestHwCounter {
        let counter = HwMeasurementAcc::new_with_metrics_drain(
            self.toc.get_collection_hw_metrics(collection_name),
        );

        RequestHwCounter::new(counter, self.service_config.hardware_reporting())
    }
}

#[tonic::async_trait]
impl PointsInternal for PointsInternalService {
    async fn upsert(
        &self,
        request: Request<UpsertPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let inference_token = extract_token(&request);

        let UpsertPointsInternal {
            upsert_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let upsert_points = extract_internal_request(upsert_points)?;

        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(
            upsert_points.collection_name.clone(),
        );

        upsert(
            StrictModeCheckedInternalTocProvider::new(&self.toc),
            upsert_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            inference_token,
            hw_data,
        )
        .await
    }

    async fn delete(
        &self,
        request: Request<DeletePointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let inference_token = extract_token(&request);

        let DeletePointsInternal {
            delete_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_points = extract_internal_request(delete_points)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            delete_points.collection_name.clone(),
        );

        delete(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            inference_token,
            hw_metrics,
        )
        .await
    }

    async fn update_vectors(
        &self,
        request: Request<UpdateVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let inference_token = extract_token(&request);

        let UpdateVectorsInternal {
            update_vectors: update_vectors_req,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let update_point_vectors = extract_internal_request(update_vectors_req)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            update_point_vectors.collection_name.clone(),
        );

        update_vectors(
            StrictModeCheckedInternalTocProvider::new(&self.toc),
            update_point_vectors,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            inference_token,
            hw_metrics,
        )
        .await
    }

    async fn delete_vectors(
        &self,
        request: Request<DeleteVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeleteVectorsInternal {
            delete_vectors: delete_vectors_req,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_point_vectors = extract_internal_request(delete_vectors_req)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            delete_point_vectors.collection_name.clone(),
        );

        delete_vectors(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_point_vectors,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            hw_metrics,
        )
        .await
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let SetPayloadPointsInternal {
            set_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let set_payload_points = extract_internal_request(set_payload_points)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            set_payload_points.collection_name.clone(),
        );

        set_payload(
            StrictModeCheckedInternalTocProvider::new(&self.toc),
            set_payload_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            hw_metrics,
        )
        .await
    }

    async fn overwrite_payload(
        &self,
        request: Request<SetPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let SetPayloadPointsInternal {
            set_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let set_payload_points = extract_internal_request(set_payload_points)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            set_payload_points.collection_name.clone(),
        );

        overwrite_payload(
            StrictModeCheckedInternalTocProvider::new(&self.toc),
            set_payload_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            hw_metrics,
        )
        .await
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeletePayloadPointsInternal {
            delete_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_payload_points = extract_internal_request(delete_payload_points)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            delete_payload_points.collection_name.clone(),
        );

        delete_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_payload_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            hw_metrics,
        )
        .await
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let ClearPayloadPointsInternal {
            clear_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let clear_payload_points = extract_internal_request(clear_payload_points)?;

        let hw_metrics = self.get_request_collection_hw_usage_counter_for_internal(
            clear_payload_points.collection_name.clone(),
        );

        clear_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            clear_payload_points,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            hw_metrics,
        )
        .await
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let CreateFieldIndexCollectionInternal {
            create_field_index_collection,
            shard_id,
            clock_tag,
        } = request.into_inner();

        create_field_index_internal(
            self.toc.clone(),
            extract_internal_request(create_field_index_collection)?,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
        )
        .await
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeleteFieldIndexCollectionInternal {
            delete_field_index_collection,
            shard_id,
            clock_tag,
        } = request.into_inner();

        delete_field_index_internal(
            self.toc.clone(),
            extract_internal_request(delete_field_index_collection)?,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
        )
        .await
    }

    async fn core_search_batch(
        &self,
        request: Request<CoreSearchBatchPointsInternal>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        validate_and_log(request.get_ref());

        let CoreSearchBatchPointsInternal {
            collection_name,
            search_points,
            shard_id,
            timeout,
        } = request.into_inner();

        let timeout = timeout.map(Duration::from_secs);

        // Individual `read_consistency` values are ignored by `core_search_batch`...
        //
        // search_points
        //     .iter_mut()
        //     .for_each(|search_points| search_points.read_consistency = None);

        let hw_data =
            self.get_request_collection_hw_usage_counter_for_internal(collection_name.clone());
        let res = core_search_list(
            self.toc.as_ref(),
            collection_name,
            search_points,
            None, // *Has* to be `None`!
            shard_id,
            FULL_ACCESS.clone(),
            timeout,
            hw_data,
        )
        .await?;

        Ok(res)
    }

    async fn recommend(
        &self,
        request: Request<RecommendPointsInternal>,
    ) -> Result<Response<RecommendResponse>, Status> {
        validate_and_log(request.get_ref());

        let RecommendPointsInternal {
            recommend_points,
            ..  // shard_id - is not used in internal API,
            // because it is transformed into regular search requests on the first node
        } = request.into_inner();

        let mut recommend_points = recommend_points
            .ok_or_else(|| Status::invalid_argument("RecommendPoints is missing"))?;

        recommend_points.read_consistency = None; // *Have* to be `None`!

        let collection_name = recommend_points.collection_name.clone();

        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(collection_name);
        let res = recommend(
            UncheckedTocProvider::new_unchecked(&self.toc),
            recommend_points,
            FULL_ACCESS.clone(),
            hw_data,
        )
        .await?;

        Ok(res)
    }

    async fn scroll(
        &self,
        request: Request<ScrollPointsInternal>,
    ) -> Result<Response<ScrollResponse>, Status> {
        validate_and_log(request.get_ref());

        let ScrollPointsInternal {
            scroll_points,
            shard_id,
        } = request.into_inner();

        let mut scroll_points =
            scroll_points.ok_or_else(|| Status::invalid_argument("ScrollPoints is missing"))?;

        scroll_points.read_consistency = None; // *Have* to be `None`!

        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(
            scroll_points.collection_name.clone(),
        );

        scroll(
            UncheckedTocProvider::new_unchecked(&self.toc),
            scroll_points,
            shard_id,
            FULL_ACCESS.clone(),
            hw_data,
        )
        .await
    }

    async fn get(
        &self,
        request: Request<GetPointsInternal>,
    ) -> Result<Response<GetResponse>, Status> {
        validate_and_log(request.get_ref());

        let GetPointsInternal {
            get_points,
            shard_id,
        } = request.into_inner();

        let mut get_points =
            get_points.ok_or_else(|| Status::invalid_argument("GetPoints is missing"))?;

        get_points.read_consistency = None; // *Have* to be `None`!

        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(
            get_points.collection_name.clone(),
        );

        get(
            UncheckedTocProvider::new_unchecked(&self.toc),
            get_points,
            shard_id,
            FULL_ACCESS.clone(),
            hw_data,
        )
        .await
    }

    async fn count(
        &self,
        request: Request<CountPointsInternal>,
    ) -> Result<Response<CountResponse>, Status> {
        validate_and_log(request.get_ref());

        let CountPointsInternal {
            count_points,
            shard_id,
        } = request.into_inner();

        let count_points =
            count_points.ok_or_else(|| Status::invalid_argument("CountPoints is missing"))?;
        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(
            count_points.collection_name.clone(),
        );
        let res = count(
            UncheckedTocProvider::new_unchecked(&self.toc),
            count_points,
            shard_id,
            &FULL_ACCESS,
            hw_data,
        )
        .await?;
        Ok(res)
    }

    async fn sync(
        &self,
        request: Request<SyncPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());
        let inference_token = extract_token(&request);

        let SyncPointsInternal {
            sync_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        sync(
            self.toc.clone(),
            extract_internal_request(sync_points)?,
            InternalUpdateParams::from_grpc(shard_id, clock_tag),
            FULL_ACCESS.clone(),
            inference_token,
        )
        .await
    }

    async fn query_batch(
        &self,
        request: Request<QueryBatchPointsInternal>,
    ) -> Result<Response<QueryBatchResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let QueryBatchPointsInternal {
            collection_name,
            shard_id,
            query_points,
            timeout,
        } = request.into_inner();

        let timeout = timeout.map(Duration::from_secs);

        let hw_data =
            self.get_request_collection_hw_usage_counter_for_internal(collection_name.clone());

        query_batch_internal(
            self.toc.as_ref(),
            collection_name,
            query_points,
            shard_id,
            timeout,
            hw_data,
        )
        .await
    }

    async fn facet(
        &self,
        request: Request<FacetCountsInternal>,
    ) -> Result<Response<FacetResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let request_inner = request.into_inner();
        let hw_data = self.get_request_collection_hw_usage_counter_for_internal(
            request_inner.collection_name.clone(),
        );
        facet_counts_internal(self.toc.as_ref(), request_inner, hw_data).await
    }
}

fn extract_internal_request<T>(request: Option<T>) -> Result<T, tonic::Status> {
    request.ok_or_else(|| {
        tonic::Status::invalid_argument(format!("{} is missing", any::type_name::<T>()))
    })
}
