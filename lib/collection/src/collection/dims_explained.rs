//! Collection-level support for per-dimension score explanations and
//! dimension-focused re-scoring.

use std::time::Duration;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::dims_explained::DimsExplainedCalculator;
use segment::data_types::vectors::{Named, VectorInternal, VectorRef, VectorStructInternal};
use segment::types::{PointIdType, ScoredPoint, WithPayloadInterface, WithVector};
use shard::query::dims_focus::dims_focus_rescore as dims_focus_rescore_impl;
use shard::query::query_enum::QueryEnum;
use shard::query::{DimsExplainedInternal, DimsFocusInternal, ScoringQuery, ShardQueryRequest};
use tokio_util::task::AbortOnDropHandle;

use super::Collection;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::config::CollectionParams;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult, PointRequestInternal};

/// Re-scores points on a subset of vector dimensions in the search runtime.
///
/// Optionally attaches per-dimension explanations to each point.
#[allow(clippy::too_many_arguments)]
pub async fn dims_focus_rescore(
    collection_params: &CollectionParams,
    points_with_vector: impl IntoIterator<Item = ScoredPoint> + Send + 'static,
    focus: DimsFocusInternal,
    dims_explained: Option<DimsExplainedInternal>,
    limit: usize,
    search_runtime_handle: &AdaptiveSearchHandle,
    timeout: Duration,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<ScoredPoint>> {
    let distance = collection_params.get_distance(&focus.using)?;

    let cpu_utilization = hw_measurement_acc.cpu_utilization();
    let handle = search_runtime_handle.spawn_blocking(move || {
        cpu_utilization.measure(|| {
            dims_focus_rescore_impl(
                points_with_vector,
                focus,
                distance,
                dims_explained,
                limit,
                hw_measurement_acc,
            )
        })
    });
    let task = AbortOnDropHandle::new(handle);

    let result = tokio::time::timeout(timeout, task)
        .await
        .map_err(|_| CollectionError::timeout(timeout, "dims_focus"))???;

    Ok(result)
}

impl Collection {
    /// Attaches per-dimension score explanations to search results, if requested.
    ///
    /// Only handles plain nearest neighbor queries. For dimension-focused queries
    /// the explanations are attached during re-scoring, so those are passed through.
    ///
    /// Vectors of the result points are re-used when present, otherwise they are
    /// retrieved. Retrieved vectors are not attached to the response.
    pub(super) async fn fill_results_with_dims_explained(
        &self,
        mut results: Vec<ScoredPoint>,
        request: &ShardQueryRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let Some(explain) = request.dims_explained else {
            return Ok(results);
        };

        let (query_vector, using) = match request.query.as_ref() {
            Some(ScoringQuery::Vector(QueryEnum::Nearest(named))) => match &named.query {
                VectorInternal::Dense(query_vector) => {
                    (query_vector.clone(), named.get_name().to_owned())
                }
                // Guarded against by request validation
                VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => return Ok(results),
            },
            // Explanations are attached during the dims-focused re-scoring
            Some(ScoringQuery::DimsFocus(_)) => return Ok(results),
            // Guarded against by request validation
            _ => return Ok(results),
        };

        // Retrieve vectors which are not already part of the results
        let missing_ids: Vec<PointIdType> = results
            .iter()
            .filter(|point| {
                point
                    .vector
                    .as_ref()
                    .and_then(|vector| vector.get(&using))
                    .is_none()
            })
            .map(|point| point.id)
            .collect();

        let retrieved_vectors: AHashMap<PointIdType, VectorStructInternal> =
            if missing_ids.is_empty() {
                AHashMap::new()
            } else {
                let retrieve_request = PointRequestInternal {
                    ids: missing_ids,
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: WithVector::from(using.clone()),
                };
                self.retrieve(
                    retrieve_request,
                    read_consistency,
                    shard_selection,
                    timeout,
                    hw_measurement_acc.clone(),
                )
                .await?
                .into_iter()
                .filter_map(|record| record.vector.map(|vector| (record.id, vector)))
                .collect()
            };

        let distance = self
            .collection_config
            .read()
            .await
            .params
            .get_distance(&using)?;
        let calculator = DimsExplainedCalculator::new(query_vector, distance);

        let mut explained_points = 0;
        for point in &mut results {
            let dims_explained = {
                let vector_ref = point
                    .vector
                    .as_ref()
                    .and_then(|vector| vector.get(&using))
                    .or_else(|| {
                        retrieved_vectors
                            .get(&point.id)
                            .and_then(|vector| vector.get(&using))
                    });

                match vector_ref {
                    Some(VectorRef::Dense(point_vector)) => {
                        explained_points += 1;
                        Some(calculator.top_contributions(point_vector, explain.top))
                    }
                    // Point may have been deleted between search and retrieve,
                    // or the vector is not dense.
                    _ => None,
                }
            };
            point.dims_explained = dims_explained;
        }

        hw_measurement_acc
            .get_counter_cell()
            .cpu_counter()
            .incr_delta(explained_points * calculator.query_len());

        Ok(results)
    }
}
