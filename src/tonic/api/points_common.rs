use std::time::Instant;

use api::grpc::conversions::proto_to_payloads;
use api::grpc::qdrant::payload_index_params::IndexParams;
use api::grpc::qdrant::{
    BatchResult, ClearPayloadPoints, CountPoints, CountResponse, CreateFieldIndexCollection,
    DeleteFieldIndexCollection, DeletePayloadPoints, DeletePoints, FieldType, GetPoints,
    GetResponse, PayloadIndexParams, PointsOperationResponse, RecommendBatchResponse,
    RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse, SearchBatchResponse,
    SearchPoints, SearchResponse, SetPayloadPoints, UpsertPoints,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::PointInsertOperations;
use collection::operations::types::{
    default_exact_count, PointRequest, RecommendRequestBatch, ScrollRequest, SearchRequest,
    SearchRequestBatch,
};
use collection::shard::ShardId;
use segment::types::{PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType};
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{Response, Status};

use crate::common::points::{
    do_clear_payload, do_count_points, do_create_index, do_delete_index, do_delete_payload,
    do_delete_points, do_get_points, do_scroll_points, do_search_batch_points, do_search_points,
    do_set_payload, do_upsert_points, CreateFieldIndex,
};

pub fn points_operation_response(
    timing: Instant,
    update_result: collection::operations::types::UpdateResult,
) -> PointsOperationResponse {
    PointsOperationResponse {
        result: Some(update_result.into()),
        time: timing.elapsed().as_secs_f64(),
    }
}

pub async fn upsert(
    toc: &TableOfContent,
    upsert_points: UpsertPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let UpsertPoints {
        collection_name,
        wait,
        points,
    } = upsert_points;
    let points = points
        .into_iter()
        .map(|point| point.try_into())
        .collect::<Result<_, _>>()?;
    let operation = PointInsertOperations::PointsList(points);
    let timing = Instant::now();
    let result = do_upsert_points(
        toc,
        &collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete(
    toc: &TableOfContent,
    delete_points: DeletePoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeletePoints {
        collection_name,
        wait,
        points,
    } = delete_points;

    let points_selector = match points {
        None => return Err(Status::invalid_argument("PointSelector is missing")),
        Some(p) => p.try_into()?,
    };

    let timing = Instant::now();
    let result = do_delete_points(
        toc,
        &collection_name,
        points_selector,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn set_payload(
    toc: &TableOfContent,
    set_payload_points: SetPayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let SetPayloadPoints {
        collection_name,
        wait,
        payload,
        points,
    } = set_payload_points;

    let operation = collection::operations::payload_ops::SetPayload {
        payload: proto_to_payloads(payload)?,
        points: points
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
    };

    let timing = Instant::now();
    let result = do_set_payload(
        toc,
        &collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_payload(
    toc: &TableOfContent,
    delete_payload_points: DeletePayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeletePayloadPoints {
        collection_name,
        wait,
        keys,
        points,
    } = delete_payload_points;

    let operation = DeletePayload {
        keys,
        points: points
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
    };

    let timing = Instant::now();
    let result = do_delete_payload(
        toc,
        &collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn clear_payload(
    toc: &TableOfContent,
    clear_payload_points: ClearPayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let ClearPayloadPoints {
        collection_name,
        wait,
        points,
    } = clear_payload_points;

    let points_selector = match points {
        None => return Err(Status::invalid_argument("PointSelector is missing")),
        Some(p) => p.try_into()?,
    };

    let timing = Instant::now();
    let result = do_clear_payload(
        toc,
        &collection_name,
        points_selector,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn create_field_index(
    toc: &TableOfContent,
    create_field_index_collection: CreateFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let CreateFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        field_type,
        field_index_params,
    } = create_field_index_collection;

    let filed_type_parsed = field_type
        .map(FieldType::from_i32)
        .ok_or_else(|| Status::invalid_argument("cannot convert field_type"))?;

    let field_schema = match (filed_type_parsed, field_index_params) {
        (
            Some(v),
            Some(PayloadIndexParams {
                index_params: Some(IndexParams::TextIndexParams(text_index_params)),
            }),
        ) => match v {
            FieldType::Text => Some(PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(
                text_index_params.try_into()?,
            ))),
            _ => {
                return Err(Status::invalid_argument(
                    "field_type and field_index_params do not match",
                ))
            }
        },
        (Some(v), None | Some(PayloadIndexParams { index_params: None })) => match v {
            FieldType::Keyword => Some(PayloadSchemaType::Keyword.into()),
            FieldType::Integer => Some(PayloadSchemaType::Integer.into()),
            FieldType::Float => Some(PayloadSchemaType::Float.into()),
            FieldType::Geo => Some(PayloadSchemaType::Geo.into()),
            FieldType::Text => Some(PayloadSchemaType::Text.into()),
        },
        (None, Some(_)) => return Err(Status::invalid_argument("field type is missing")),
        (None, None) => None,
    };

    let operation = CreateFieldIndex {
        field_name,
        field_schema,
    };

    let timing = Instant::now();
    let result = do_create_index(
        toc,
        &collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_field_index(
    toc: &TableOfContent,
    delete_field_index_collection: DeleteFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeleteFieldIndexCollection {
        collection_name,
        wait,
        field_name,
    } = delete_field_index_collection;

    let timing = Instant::now();
    let result = do_delete_index(
        toc,
        &collection_name,
        field_name,
        shard_selection,
        wait.unwrap_or(false),
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn search(
    toc: &TableOfContent,
    search_points: SearchPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<SearchResponse>, Status> {
    let SearchPoints {
        collection_name,
        vector,
        filter,
        limit,
        offset,
        with_vector,
        with_payload,
        params,
        score_threshold,
        vector_name,
    } = search_points;

    let search_request = SearchRequest {
        vector,
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.unwrap_or_default() as usize,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vector.unwrap_or(false),
        score_threshold,
        vector_name,
    };

    let timing = Instant::now();
    let scored_points = do_search_points(toc, &collection_name, search_request, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = SearchResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn search_batch(
    toc: &TableOfContent,
    collection_name: String,
    search_points: Vec<SearchPoints>,
    shard_selection: Option<ShardId>,
) -> Result<Response<SearchBatchResponse>, Status> {
    let searches: Result<Vec<_>, Status> = search_points
        .into_iter()
        .map(|search_point| search_point.try_into())
        .collect();
    let search_requests = SearchRequestBatch {
        searches: searches?,
    };

    let timing = Instant::now();
    let scored_points =
        do_search_batch_points(toc, &collection_name, search_requests, shard_selection)
            .await
            .map_err(error_to_status)?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn recommend(
    toc: &TableOfContent,
    recommend_points: RecommendPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<RecommendResponse>, Status> {
    let RecommendPoints {
        collection_name,
        positive,
        negative,
        filter,
        limit,
        offset,
        with_vector,
        with_payload,
        params,
        score_threshold,
        vector_name,
    } = recommend_points;

    let request = collection::operations::types::RecommendRequest {
        positive: positive
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
        negative: negative
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.unwrap_or_default() as usize,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vector.unwrap_or(false),
        score_threshold,
        vector_name,
    };

    let timing = Instant::now();
    let recommended_points = toc
        .recommend(&collection_name, request, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = RecommendResponse {
        result: recommended_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_batch(
    toc: &TableOfContent,
    collection_name: String,
    recommend_points: Vec<RecommendPoints>,
    shard_selection: Option<ShardId>,
) -> Result<Response<RecommendBatchResponse>, Status> {
    let searches: Result<Vec<_>, Status> = recommend_points
        .into_iter()
        .map(|recommend_point| recommend_point.try_into())
        .collect();
    let recommend_batch = RecommendRequestBatch {
        searches: searches?,
    };

    let timing = Instant::now();
    let scored_points = toc
        .recommend_batch(&collection_name, recommend_batch, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = RecommendBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn scroll(
    toc: &TableOfContent,
    scroll_points: ScrollPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<ScrollResponse>, Status> {
    let ScrollPoints {
        collection_name,
        filter,
        offset,
        limit,
        with_vector,
        with_payload,
    } = scroll_points;

    let scroll_request = ScrollRequest {
        offset: offset.map(|o| o.try_into()).transpose()?,
        limit: limit.map(|l| l as usize),
        filter: filter.map(|f| f.try_into()).transpose()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vector.unwrap_or(false),
    };

    let timing = Instant::now();
    let scrolled_points = do_scroll_points(toc, &collection_name, scroll_request, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = ScrollResponse {
        next_page_offset: scrolled_points.next_page_offset.map(|n| n.into()),
        result: scrolled_points
            .points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn count(
    toc: &TableOfContent,
    count_points: CountPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<CountResponse>, Status> {
    let CountPoints {
        collection_name,
        filter,
        exact,
    } = count_points;

    let count_request = collection::operations::types::CountRequest {
        filter: filter.map(|f| f.try_into()).transpose()?,
        exact: exact.unwrap_or_else(default_exact_count),
    };

    let timing = Instant::now();
    let count_result = do_count_points(toc, &collection_name, count_request, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = CountResponse {
        result: Some(count_result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn get(
    toc: &TableOfContent,
    get_points: GetPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<GetResponse>, Status> {
    let GetPoints {
        collection_name,
        ids,
        with_vector,
        with_payload,
    } = get_points;

    let point_request = PointRequest {
        ids: ids
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vector.unwrap_or(false),
    };

    let timing = Instant::now();

    let records = do_get_points(toc, &collection_name, point_request, shard_selection)
        .await
        .map_err(error_to_status)?;

    let response = GetResponse {
        result: records.into_iter().map(|point| point.into()).collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}
