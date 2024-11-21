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
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;
use tonic::{Request, Response, Status};

use super::points_common::{core_search_list, scroll};
use super::validate_and_log;
use crate::settings::ServiceConfig;
use crate::tonic::api::points_common::{
    clear_payload, count, create_field_index_internal, delete, delete_field_index_internal,
    delete_payload, delete_vectors, get, overwrite_payload, recommend, set_payload, sync,
    update_vectors, upsert,
};
use crate::tonic::verification::UncheckedTocProvider;

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
        )
        .await?;

    let FacetResponse { hits } = response;

    let response = FacetResponseInternal {
        hits: hits.into_iter().map(From::from).collect_vec(),
        time: timing.elapsed().as_secs_f64(),
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
        let counter =
            HwMeasurementAcc::new_with_drain(&self.toc.get_collection_hw_metrics(collection_name));

        RequestHwCounter::new(counter, self.service_config.hardware_reporting(), true)
    }
}

#[tonic::async_trait]
impl PointsInternal for PointsInternalService {
    async fn upsert(
        &self,
        request: Request<UpsertPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let UpsertPointsInternal {
            upsert_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let upsert_points =
            upsert_points.ok_or_else(|| Status::invalid_argument("UpsertPoints is missing"))?;

        upsert(
            UncheckedTocProvider::new_unchecked(&self.toc),
            upsert_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
        )
        .await
    }

    async fn delete(
        &self,
        request: Request<DeletePointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeletePointsInternal {
            delete_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_points =
            delete_points.ok_or_else(|| Status::invalid_argument("DeletePoints is missing"))?;

        delete(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
        )
        .await
    }

    async fn update_vectors(
        &self,
        request: Request<UpdateVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let request = request.into_inner();

        let shard_id = request.shard_id;
        let clock_tag = request.clock_tag;

        let update_point_vectors = request
            .update_vectors
            .ok_or_else(|| Status::invalid_argument("UpdateVectors is missing"))?;

        update_vectors(
            UncheckedTocProvider::new_unchecked(&self.toc),
            update_point_vectors,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
        )
        .await
    }

    async fn delete_vectors(
        &self,
        request: Request<DeleteVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let request = request.into_inner();

        let shard_id = request.shard_id;
        let clock_tag = request.clock_tag;

        let delete_point_vectors = request
            .delete_vectors
            .ok_or_else(|| Status::invalid_argument("DeleteVectors is missing"))?;

        delete_vectors(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_point_vectors,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        let set_payload_points = set_payload_points
            .ok_or_else(|| Status::invalid_argument("SetPayloadPoints is missing"))?;

        set_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            set_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        let set_payload_points = set_payload_points
            .ok_or_else(|| Status::invalid_argument("SetPayloadPoints is missing"))?;

        overwrite_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            set_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        let delete_payload_points = delete_payload_points
            .ok_or_else(|| Status::invalid_argument("DeletePayloadPoints is missing"))?;

        delete_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            delete_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        let clear_payload_points = clear_payload_points
            .ok_or_else(|| Status::invalid_argument("ClearPayloadPoints is missing"))?;

        clear_payload(
            UncheckedTocProvider::new_unchecked(&self.toc),
            clear_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        let create_field_index_collection = create_field_index_collection
            .ok_or_else(|| Status::invalid_argument("CreateFieldIndexCollection is missing"))?;

        create_field_index_internal(
            self.toc.clone(),
            create_field_index_collection,
            clock_tag.map(Into::into),
            shard_id,
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

        let delete_field_index_collection = delete_field_index_collection
            .ok_or_else(|| Status::invalid_argument("DeleteFieldIndexCollection is missing"))?;

        delete_field_index_internal(
            self.toc.clone(),
            delete_field_index_collection,
            clock_tag.map(Into::into),
            shard_id,
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

        scroll(
            UncheckedTocProvider::new_unchecked(&self.toc),
            scroll_points,
            shard_id,
            FULL_ACCESS.clone(),
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

        get(
            UncheckedTocProvider::new_unchecked(&self.toc),
            get_points,
            shard_id,
            FULL_ACCESS.clone(),
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

        let SyncPointsInternal {
            sync_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let sync_points =
            sync_points.ok_or_else(|| Status::invalid_argument("SyncPoints is missing"))?;
        sync(
            self.toc.clone(),
            sync_points,
            clock_tag.map(Into::into),
            shard_id,
            FULL_ACCESS.clone(),
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

        facet_counts_internal(self.toc.as_ref(), request.into_inner()).await
    }
}
