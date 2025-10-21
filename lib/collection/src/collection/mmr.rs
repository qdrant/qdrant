use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::ScoredPoint;
use shard::query::MmrInternal;
use shard::query::mmr::mmr_from_points_with_vector as mmr_from_points_with_vector_impl;
use tokio::runtime::Handle;

use crate::config::CollectionParams;
use crate::operations::types::{CollectionError, CollectionResult};

pub async fn mmr_from_points_with_vector(
    collection_params: &CollectionParams,
    points_with_vector: impl IntoIterator<Item = ScoredPoint> + Send + 'static,
    mmr: MmrInternal,
    limit: usize,
    search_runtime_handle: &Handle,
    timeout: Duration,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<ScoredPoint>> {
    let distance = collection_params.get_distance(&mmr.using)?;
    let multivector_config = collection_params
        .vectors
        .get_params(&mmr.using)
        .and_then(|vector_params| vector_params.multivector_config);

    let task = search_runtime_handle.spawn_blocking(move || {
        mmr_from_points_with_vector_impl(
            points_with_vector,
            mmr,
            distance,
            multivector_config,
            limit,
            hw_measurement_acc,
        )
    });

    let result = tokio::time::timeout(timeout, task)
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "mmr"))???;

    Ok(result)
}
