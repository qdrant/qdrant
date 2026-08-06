use std::cmp;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::iterator_ext::IteratorExt;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::modifier::Modifier;
use segment::data_types::query_context::QueryContext;
use segment::entry::ReadSegmentEntry;
use segment::types::{DEFAULT_FULL_SCAN_THRESHOLD, Distance, ScoredPoint};
use shard::common::stopping_guard::StoppingGuard;
use shard::query::query_context::init_query_context;
use shard::query::query_enum::QueryEnum;
use shard::search::{CoreSearchRequest, group_search_batches};
use shard::search_result_aggregator::BatchResultAggregator;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        let [points] =
            self.search_batch(&[search])?
                .try_into()
                .map_err(|unconverted: Vec<_>| {
                    OperationError::service_error(format!(
                        "unexpected search batch size: expected 1, received {}",
                        unconverted.len(),
                    ))
                })?;

        Ok(points)
    }

    /// Run a whole batch of core searches in a single pass over the segments.
    ///
    /// Requests that agree on everything but their query vector are handed to each segment as one
    /// [`search_batch`](ReadSegmentEntry::search_batch) call, so the work that does not depend on
    /// the query vector — evaluating the filter into a candidate set, picking the index to use — is
    /// paid once per group instead of once per request. The segments are also visited (and the
    /// query context built) once for the whole batch rather than once per request.
    ///
    /// Returns one result list per request, in request order.
    pub(crate) fn search_batch(
        &self,
        searches: &[CoreSearchRequest],
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        if searches.is_empty() {
            return Ok(Vec::new());
        }

        let is_stopped_guard = StoppingGuard::new();
        let query_context = init_query_context(
            searches,
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

        // Resolved up front so an unknown vector name fails the batch before any search runs.
        let distances = searches
            .iter()
            .map(|search| self.config.get_distance(search.query.get_vector_name()))
            .collect::<OperationResult<Vec<_>>>()?;

        let Some(context) = fill_query_context_over(
            query_context,
            &self.segments,
            &is_stopped_guard.get_is_stopped(),
        )?
        else {
            // No segments to search
            return Ok(vec![Vec::new(); searches.len()]);
        };

        // Grouping depends on the requests only, so it is computed once and shared by all segments.
        let groups = group_search_batches(searches);

        // Search every segment in parallel on the shard's search pool. Each task derives its own
        // per-segment query context from the shared `context`.
        let points_by_segment = self.par_map_segments(|segment| {
            let segment_query_context = context.get_segment_query_context();
            let segment = segment.read_segment();

            let mut points_by_request = Vec::with_capacity(searches.len());
            for group in &groups {
                let query_vectors: Vec<_> = group.query_vectors.iter().collect();
                let batched_points = segment.search_batch(
                    group.params.vector_name,
                    &query_vectors,
                    &group.params.with_payload,
                    &group.params.with_vector,
                    group.params.filter,
                    group.params.top,
                    group.params.params,
                    &segment_query_context,
                )?;

                debug_assert_eq!(batched_points.len(), group.query_vectors.len());
                points_by_request.extend(batched_points);
            }

            Ok(points_by_request)
        })?;

        let mut aggregator =
            BatchResultAggregator::new(searches.iter().map(|search| search.offset + search.limit));
        aggregator.update_point_versions(points_by_segment.iter().flatten().flatten());

        for points_by_request in points_by_segment {
            for (request_idx, points) in points_by_request.into_iter().enumerate() {
                aggregator.update_batch_results(request_idx, points);
            }
        }

        // One aggregator was created per request, so the top-k lists line up with `searches`.
        let mut points_by_request = aggregator.into_topk();
        debug_assert_eq!(points_by_request.len(), searches.len());

        for ((points, search), distance) in
            points_by_request.iter_mut().zip(searches).zip(distances)
        {
            postprocess_scores(points, search, distance);
        }

        Ok(points_by_request)
    }
}

/// Turn the raw segment scores of a single request into the scores the caller expects: apply the
/// distance's score postprocessing, cut off at the score threshold and skip the requested offset.
fn postprocess_scores(
    points: &mut Vec<ScoredPoint>,
    search: &CoreSearchRequest,
    distance: Distance,
) {
    match &search.query {
        // Only plain nearest-neighbour scores are raw segment distances; every other query
        // already produces a comparable score of its own.
        QueryEnum::Nearest(_) => {
            for point in points.iter_mut() {
                point.score = distance.postprocess_score(point.score);
            }
        }
        QueryEnum::RecommendBestScore(_) => (),
        QueryEnum::RecommendSumScores(_) => (),
        QueryEnum::Discover(_) => (),
        QueryEnum::Context(_) => (),
        QueryEnum::FeedbackNaive(_) => (),
    }

    if let Some(score_threshold) = search.score_threshold {
        debug_assert!(
            points.is_sorted_by(|left, right| distance.is_ordered(left.score, right.score)),
        );

        let below_threshold = points
            .iter()
            .position(|point| !distance.check_threshold(point.score, score_threshold));

        if let Some(below_threshold_idx) = below_threshold {
            points.truncate(below_threshold_idx);
        }
    }

    let _ = points.drain(..cmp::min(points.len(), search.offset));
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
