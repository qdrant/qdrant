use std::time::{Duration, Instant};

use api::conversions::json::json_path_from_proto;
use api::grpc::qdrant::{
    BatchResult, CoreSearchPoints, CountPoints, CountResponse, DiscoverBatchResponse,
    DiscoverPoints, DiscoverResponse, FacetCounts, FacetResponse, GetPoints, GetResponse,
    GroupsResult, QueryBatchResponse, QueryGroupsResponse, QueryPointGroups, QueryPoints,
    QueryResponse, ReadConsistency as ReadConsistencyGrpc, RecommendBatchResponse,
    RecommendGroupsResponse, RecommendPointGroups, RecommendPoints, RecommendResponse,
    ScrollPoints, ScrollResponse, SearchBatchResponse, SearchGroupsResponse, SearchMatrixPoints,
    SearchPointGroups, SearchPoints, SearchResponse,
};
use api::grpc::{InferenceUsage, Usage};
use api::rest::OrderByInterface;
use collection::collection::distance_matrix::{
    CollectionSearchMatrixRequest, CollectionSearchMatrixResponse,
};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::conversions::try_discover_request_from_grpc;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CoreSearchRequest, CoreSearchRequestBatch, PointRequestInternal, ScrollRequestInternal,
    default_exact_count,
};
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::facets::FacetParams;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, NamedQuery, VectorInternal};
use shard::query::query_enum::QueryEnum;
use storage::content_manager::toc::TableOfContent;
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::rbac::Access;
use tonic::{Response, Status};

use crate::common::inference::InferenceToken;
use crate::common::inference::query_requests_grpc::{
    convert_query_point_groups_from_grpc, convert_query_points_from_grpc,
};
use crate::common::query::*;
use crate::common::strict_mode::*;

pub(crate) fn convert_shard_selector_for_read(
    shard_id_selector: Option<ShardId>,
    shard_key_selector: Option<api::grpc::qdrant::ShardKeySelector>,
) -> Result<ShardSelectorInternal, Status> {
    let res = match (shard_id_selector, shard_key_selector) {
        (Some(shard_id), None) => ShardSelectorInternal::ShardId(shard_id),
        (None, Some(shard_key_selector)) => ShardSelectorInternal::try_from(shard_key_selector)?,
        (None, None) => ShardSelectorInternal::All,
        (Some(shard_id), Some(_)) => {
            debug_assert!(
                false,
                "Shard selection and shard key selector are mutually exclusive"
            );
            ShardSelectorInternal::ShardId(shard_id)
        }
    };
    Ok(res)
}

pub async fn search(
    toc_provider: impl CheckedTocProvider,
    search_points: SearchPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    hw_measurement_acc: RequestHwCounter,
) -> Result<Response<SearchResponse>, Status> {
    let SearchPoints {
        collection_name,
        vector,
        filter,
        limit,
        offset,
        with_payload,
        params,
        score_threshold,
        vector_name,
        with_vectors,
        read_consistency,
        timeout,
        shard_key_selector,
        sparse_indices,
    } = search_points;

    let vector_internal =
        VectorInternal::from_vector_and_indices(vector, sparse_indices.map(|v| v.data));

    let vector_struct =
        api::grpc::conversions::into_named_vector_struct(vector_name, vector_internal)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;

    let search_request = CoreSearchRequest {
        query: QueryEnum::Nearest(NamedQuery::from(vector_struct)),
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.unwrap_or_default() as usize,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: Some(
            with_vectors
                .map(|selector| selector.into())
                .unwrap_or_default(),
        ),
        score_threshold,
    };

    let toc = toc_provider
        .check_strict_mode(
            &search_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = do_core_search_points(
        toc,
        &collection_name,
        search_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        hw_measurement_acc.get_counter(),
    )
    .await?;

    let response = SearchResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(hw_measurement_acc.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn core_search_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    requests: Vec<(CoreSearchRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<SearchBatchResponse>, Status> {
    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();

    let scored_points = do_search_batch_points(
        toc,
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

#[allow(clippy::too_many_arguments)]
pub async fn core_search_list(
    toc: &TableOfContent,
    collection_name: String,
    search_points: Vec<CoreSearchPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    shard_selection: Option<ShardId>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<SearchBatchResponse>, Status> {
    let searches: Result<Vec<_>, Status> = search_points
        .into_iter()
        .map(CoreSearchRequest::try_from)
        .collect();

    let request = CoreSearchRequestBatch {
        searches: searches?,
    };

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

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let scored_points = toc
        .core_search_batch(
            &collection_name,
            request,
            read_consistency,
            shard_selection,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn search_groups(
    toc_provider: impl CheckedTocProvider,
    search_point_groups: SearchPointGroups,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<SearchGroupsResponse>, Status> {
    let search_groups_request = search_point_groups.clone().try_into()?;

    let SearchPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = search_point_groups;

    let toc = toc_provider
        .check_strict_mode(
            &search_groups_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;

    let timing = Instant::now();
    let groups_result = crate::common::query::do_search_point_groups(
        toc,
        &collection_name,
        search_groups_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        request_hw_counter.get_counter(),
    )
    .await?;

    let groups_result = GroupsResult::try_from(groups_result)
        .map_err(|e| Status::internal(format!("Failed to convert groups result: {e}")))?;

    let response = SearchGroupsResponse {
        result: Some(groups_result),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn recommend(
    toc_provider: impl CheckedTocProvider,
    recommend_points: RecommendPoints,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<RecommendResponse>, Status> {
    // extract a few fields from the request and convert to internal request
    let collection_name = recommend_points.collection_name.clone();
    let read_consistency = recommend_points.read_consistency.clone();
    let shard_key_selector = recommend_points.shard_key_selector.clone();
    let timeout = recommend_points.timeout;

    let request =
        collection::operations::types::RecommendRequestInternal::try_from(recommend_points)?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;
    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;
    let timeout = timeout.map(Duration::from_secs);

    let timing = Instant::now();
    let recommended_points = toc
        .recommend(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = RecommendResponse {
        result: recommended_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    recommend_points: Vec<RecommendPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<RecommendBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(recommend_points.len());

    for mut request in recommend_points {
        let shard_selector =
            convert_shard_selector_for_read(None, request.shard_key_selector.take())?;
        let internal_request: collection::operations::types::RecommendRequestInternal =
            request.try_into()?;
        requests.push((internal_request, shard_selector));
    }

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = toc
        .recommend_batch(
            collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = RecommendBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_groups(
    toc_provider: impl CheckedTocProvider,
    recommend_point_groups: RecommendPointGroups,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<RecommendGroupsResponse>, Status> {
    let recommend_groups_request = recommend_point_groups.clone().try_into()?;

    let RecommendPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = recommend_point_groups;

    let toc = toc_provider
        .check_strict_mode(
            &recommend_groups_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;

    let timing = Instant::now();
    let groups_result = crate::common::query::do_recommend_point_groups(
        toc,
        &collection_name,
        recommend_groups_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        request_hw_counter.get_counter(),
    )
    .await?;

    let groups_result = GroupsResult::try_from(groups_result)
        .map_err(|e| Status::internal(format!("Failed to convert groups result: {e}")))?;

    let response = RecommendGroupsResponse {
        result: Some(groups_result),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn discover(
    toc_provider: impl CheckedTocProvider,
    discover_points: DiscoverPoints,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<DiscoverResponse>, Status> {
    let (request, collection_name, read_consistency, timeout, shard_key_selector) =
        try_discover_request_from_grpc(discover_points)?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;

    let discovered_points = toc
        .discover(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = DiscoverResponse {
        result: discovered_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn discover_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    discover_points: Vec<DiscoverPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<DiscoverBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(discover_points.len());

    for discovery_request in discover_points {
        let (internal_request, _collection_name, _consistency, _timeout, shard_key_selector) =
            try_discover_request_from_grpc(discovery_request)?;
        let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;
        requests.push((internal_request, shard_selector));
    }

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();
    let scored_points = toc
        .discover_batch(
            collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = DiscoverBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn scroll(
    toc_provider: impl CheckedTocProvider,
    scroll_points: ScrollPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<ScrollResponse>, Status> {
    let ScrollPoints {
        collection_name,
        filter,
        offset,
        limit,
        with_payload,
        with_vectors,
        read_consistency,
        shard_key_selector,
        order_by,
        timeout,
    } = scroll_points;

    let scroll_request = ScrollRequestInternal {
        offset: offset.map(|o| o.try_into()).transpose()?,
        limit: limit.map(|l| l as usize),
        filter: filter.map(|f| f.try_into()).transpose()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vectors
            .map(|selector| selector.into())
            .unwrap_or_default(),
        order_by: order_by
            .map(OrderBy::try_from)
            .transpose()?
            .map(OrderByInterface::Struct),
    };

    let toc = toc_provider
        .check_strict_mode(
            &scroll_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;

    let timing = Instant::now();
    let scrolled_points = do_scroll_points(
        toc,
        &collection_name,
        scroll_request,
        read_consistency,
        timeout,
        shard_selector,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let points: Result<_, _> = scrolled_points
        .points
        .into_iter()
        .map(api::grpc::qdrant::RetrievedPoint::try_from)
        .collect();

    let points = points.map_err(|e| Status::internal(format!("Failed to convert points: {e}")))?;

    let response = ScrollResponse {
        next_page_offset: scrolled_points.next_page_offset.map(|n| n.into()),
        result: points,
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn count(
    toc_provider: impl CheckedTocProvider,
    count_points: CountPoints,
    shard_selection: Option<ShardId>,
    access: &Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<CountResponse>, Status> {
    let CountPoints {
        collection_name,
        filter,
        exact,
        read_consistency,
        shard_key_selector,
        timeout,
    } = count_points;

    let count_request = collection::operations::types::CountRequestInternal {
        filter: filter.map(|f| f.try_into()).transpose()?,
        exact: exact.unwrap_or_else(default_exact_count),
    };

    let toc = toc_provider
        .check_strict_mode(
            &count_request,
            &collection_name,
            timeout.map(|i| i as usize),
            access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;

    let timing = Instant::now();

    let count_result = do_count_points(
        toc,
        &collection_name,
        count_request,
        read_consistency,
        timeout,
        shard_selector,
        access.clone(),
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = CountResponse {
        result: Some(count_result.into()),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn get(
    toc_provider: impl CheckedTocProvider,
    get_points: GetPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<GetResponse>, Status> {
    let GetPoints {
        collection_name,
        ids,
        with_payload,
        with_vectors,
        read_consistency,
        shard_key_selector,
        timeout,
    } = get_points;

    let point_request = PointRequestInternal {
        ids: ids
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vectors
            .map(|selector| selector.into())
            .unwrap_or_default(),
    };
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;

    let timing = Instant::now();

    let toc = toc_provider
        .check_strict_mode(
            &point_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);

    let records = do_get_points(
        toc,
        &collection_name,
        point_request,
        read_consistency,
        timeout,
        shard_selector,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = GetResponse {
        result: records.into_iter().map(|point| point.into()).collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn query(
    toc_provider: impl CheckedTocProvider,
    query_points: QueryPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
    inference_token: InferenceToken,
) -> Result<Response<QueryResponse>, Status> {
    let shard_key_selector = query_points.shard_key_selector.clone();
    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;
    let read_consistency = query_points
        .read_consistency
        .clone()
        .map(TryFrom::try_from)
        .transpose()?;
    let collection_name = query_points.collection_name.clone();
    let timeout = query_points.timeout;
    let (request, inference_usage) =
        convert_query_points_from_grpc(query_points, inference_token).await?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);

    let timing = Instant::now();
    let scored_points = do_query_points(
        toc,
        &collection_name,
        request,
        read_consistency,
        shard_selector,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = QueryResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::new(request_hw_counter.to_grpc_api(), Some(inference_usage)).into_non_empty(),
    };

    Ok(Response::new(response))
}

#[allow(clippy::too_many_arguments)]
pub async fn query_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    points: Vec<QueryPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
    inference_token: InferenceToken,
) -> Result<Response<QueryBatchResponse>, Status> {
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;
    let mut requests = Vec::with_capacity(points.len());
    let mut total_inference_usage = InferenceUsage::default();

    for query_points in points {
        let shard_key_selector = query_points.shard_key_selector.clone();
        let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;
        let (request, usage) =
            convert_query_points_from_grpc(query_points, inference_token.clone()).await?;
        total_inference_usage.merge(usage);
        requests.push((request, shard_selector));
    }

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();
    let scored_points = do_query_batch_points(
        toc,
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = QueryBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::new(
            request_hw_counter.to_grpc_api(),
            total_inference_usage.into_non_empty(),
        )
        .into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn query_groups(
    toc_provider: impl CheckedTocProvider,
    query_points: QueryPointGroups,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
    inference_token: InferenceToken,
) -> Result<Response<QueryGroupsResponse>, Status> {
    let shard_key_selector = query_points.shard_key_selector.clone();
    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector)?;
    let read_consistency = query_points
        .read_consistency
        .clone()
        .map(TryFrom::try_from)
        .transpose()?;
    let timeout = query_points.timeout;
    let collection_name = query_points.collection_name.clone();
    let (request, inference_usage) =
        convert_query_point_groups_from_grpc(query_points, inference_token).await?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let timing = Instant::now();

    let groups_result = do_query_point_groups(
        toc,
        &collection_name,
        request,
        read_consistency,
        shard_selector,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let grpc_group_result = GroupsResult::try_from(groups_result)
        .map_err(|err| Status::internal(format!("failed to convert result: {err}")))?;

    let response = QueryGroupsResponse {
        result: Some(grpc_group_result),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::new(request_hw_counter.to_grpc_api(), Some(inference_usage)).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn facet(
    toc_provider: impl CheckedTocProvider,
    facet_counts: FacetCounts,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<FacetResponse>, Status> {
    let FacetCounts {
        collection_name,
        key,
        filter,
        exact,
        limit,
        read_consistency,
        shard_key_selector,
        timeout,
    } = facet_counts;

    let facet_request = FacetParams {
        key: json_path_from_proto(&key)?,
        filter: filter.map(TryInto::try_into).transpose()?,
        limit: limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse limit param into usize"))?
            .unwrap_or(FacetParams::DEFAULT_LIMIT),
        exact: exact.unwrap_or(FacetParams::DEFAULT_EXACT),
    };

    let toc = toc_provider
        .check_strict_mode(
            &facet_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;

    let timing = Instant::now();
    let facet_response = toc
        .facet(
            &collection_name,
            facet_request,
            shard_selector,
            read_consistency,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let segment::data_types::facets::FacetResponse { hits } = facet_response;

    let response = FacetResponse {
        hits: hits.into_iter().map(From::from).collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: Usage::from_hardware_usage(request_hw_counter.to_grpc_api()).into_non_empty(),
    };

    Ok(Response::new(response))
}

pub async fn search_points_matrix(
    toc_provider: impl CheckedTocProvider,
    search_matrix_points: SearchMatrixPoints,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<CollectionSearchMatrixResponse, Status> {
    let SearchMatrixPoints {
        collection_name,
        filter,
        sample,
        limit,
        using,
        read_consistency,
        shard_key_selector,
        timeout,
    } = search_matrix_points;

    let search_matrix_request = CollectionSearchMatrixRequest {
        filter: filter.map(TryInto::try_into).transpose()?,
        sample_size: sample
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse 'sample' param into usize"))?
            .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_SAMPLE),
        limit_per_sample: limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse 'limit' param into usize"))?
            .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_LIMIT_PER_SAMPLE),
        using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_owned()),
    };

    let toc = toc_provider
        .check_strict_mode(
            &search_matrix_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector)?;

    let search_matrix_response = toc
        .search_points_matrix(
            &collection_name,
            search_matrix_request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            hw_measurement_acc,
        )
        .await?;

    Ok(search_matrix_response)
}
