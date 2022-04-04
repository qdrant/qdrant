use crate::common::points::do_update_points;
use api::grpc::qdrant::{PointsOperationResponse, UpsertPoints};
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsList};
use collection::operations::CollectionUpdateOperations;
use collection::shard::ShardId;
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
