use std::cmp;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::iterator_ext::IteratorExt;
use segment::common::operation_error::OperationResult;
use segment::data_types::modifier::Modifier;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::QueryVector;
use segment::entry::ReadSegmentEntry;
use segment::types::{DEFAULT_FULL_SCAN_THRESHOLD, ScoredPoint, WithPayload};
use shard::common::stopping_guard::StoppingGuard;
use shard::query::query_context::init_query_context;
use shard::search::CoreSearchRequest;
use shard::search_result_aggregator::BatchResultAggregator;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        let limit_with_offset = search.limit_with_offset();
        let is_stopped_guard = StoppingGuard::new();
        let searches = [search];
        let query_context = init_query_context(
            &searches,
            DEFAULT_FULL_SCAN_THRESHOLD,
            &is_stopped_guard,
            HwMeasurementAcc::disposable_edge(),
            |vector_name| {
                self.config
                    .sparse_vectors
                    .get(vector_name)
                    .is_some_and(|v| v.modifier == Some(Modifier::Idf))
            },
        )?;
        let [search] = searches;
        let Some(context) = fill_query_context_over(
            query_context,
            &self.segments,
            &is_stopped_guard.get_is_stopped(),
        )?
        else {
            // No segments to search
            return Ok(vec![]);
        };

        let CoreSearchRequest {
            query,
            filter,
            params,
            limit: _,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = search;

        let vector_name = query.get_vector_name().to_string();
        let query_vector = QueryVector::from(query);
        let with_payload = WithPayload::from(with_payload.unwrap_or_default());
        let with_vector = with_vector.unwrap_or_default();

        // Search every segment in parallel on the shard's search pool. Each task derives its own
        // per-segment query context from the shared `context`.
        let points_by_segment = self.par_map_segments(|segment| {
            let batched_points = segment.read_segment().search_batch(
                &vector_name,
                &[&query_vector],
                &with_payload,
                &with_vector,
                filter.as_ref(),
                limit_with_offset,
                params.as_ref(),
                &context.get_segment_query_context(),
            )?;

            debug_assert_eq!(batched_points.len(), 1);

            let [points] = batched_points
                .try_into()
                .expect("single batched search result");

            Ok(points)
        })?;

        let mut aggregator = BatchResultAggregator::new([limit_with_offset]);
        aggregator.update_point_versions(points_by_segment.iter().flatten());

        for points in points_by_segment {
            aggregator.update_batch_results(0, points);
        }

        let [mut points] = aggregator
            .into_topk()
            .try_into()
            .expect("single batched search result");

        let distance = {
            if let Some(dense) = self.config.vectors.get(&vector_name) {
                dense.distance
            } else if self.config.sparse_vectors.contains_key(&vector_name) {
                segment::types::Distance::Dot
            } else {
                return Err(
                    segment::common::operation_error::OperationError::service_error(format!(
                        "vector config for '{vector_name}' does not exist"
                    )),
                );
            }
        };

        match &query_vector {
            QueryVector::Nearest(_) => {
                for point in &mut points {
                    point.score = distance.postprocess_score(point.score);
                }
            }
            QueryVector::RecommendBestScore(_) => (),
            QueryVector::RecommendSumScores(_) => (),
            QueryVector::Discover(_) => (),
            QueryVector::Context(_) => (),
            QueryVector::FeedbackNaive(_) => (),
        }

        if let Some(score_threshold) = score_threshold {
            debug_assert!(
                points.is_sorted_by(|left, right| distance.is_ordered(left.score, right.score)),
            );

            let below_threshold = points
                .iter()
                .enumerate()
                .find(|(_, point)| !distance.check_threshold(point.score, score_threshold));

            if let Some((below_threshold_idx, _)) = below_threshold {
                points.truncate(below_threshold_idx);
            }
        }

        let _ = points.drain(..cmp::min(points.len(), offset));

        Ok(points)
    }
}

/// Fill a [`QueryContext`] from a pre-collected snapshot of read handles.
///
/// Read-handle equivalent of [`shard::query::query_context::fill_query_context`], which is hard-typed
/// to a `LockedSegmentHolder`. Returns `None` when there are no segments to search.
fn fill_query_context_over<H: ReadSegmentHandle>(
    mut query_context: QueryContext,
    segments: &[H],
    is_stopped: &AtomicBool,
) -> OperationResult<Option<QueryContext>> {
    if segments.is_empty() {
        return Ok(None);
    }

    for segment in segments.iter().stop_if(is_stopped) {
        segment
            .read_segment()
            .fill_query_context(&mut query_context)?;
    }

    Ok(Some(query_context))
}
