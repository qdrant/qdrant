use crate::common::points::{
    do_clear_payload, do_create_index, do_delete_index, do_delete_payload, do_delete_points,
    do_set_payload, do_update_points, CreateFieldIndex,
};
use api::grpc::conversions::proto_to_payloads;
use api::grpc::qdrant::{
    ClearPayloadPoints, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePoints, FieldType, PointsOperationResponse, SetPayloadPoints,
    UpsertPoints,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsList};
use collection::operations::CollectionUpdateOperations;
use collection::shard::ShardId;
use segment::types::PayloadSchemaType;
use std::time::Instant;
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{Response, Status};

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

    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(PointsList { points }),
    ));

    let timing = Instant::now();
    let result = do_update_points(
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
    } = create_field_index_collection;

    let field_type = match field_type {
        None => None,
        Some(f) => match FieldType::from_i32(f) {
            None => return Err(Status::invalid_argument("cannot convert field_type")),
            Some(v) => match v {
                FieldType::Keyword => Some(PayloadSchemaType::Keyword),
                FieldType::Integer => Some(PayloadSchemaType::Integer),
                FieldType::Float => Some(PayloadSchemaType::Float),
                FieldType::Geo => Some(PayloadSchemaType::Geo),
            },
        },
    };

    let operation = CreateFieldIndex {
        field_name,
        field_type,
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
