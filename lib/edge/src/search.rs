use std::cmp;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::OperationResult;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::QueryVector;
use segment::types::{DEFAULT_FULL_SCAN_THRESHOLD, ScoredPoint, WithPayload};
use shard::search::CoreSearchRequest;
use shard::search_result_aggregator::BatchResultAggregator;

use crate::Shard;

impl Shard {
    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        let segments: Vec<_> = self
            .segments
            .read()
            .non_appendable_then_appendable_segments()
            .collect();

        let CoreSearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = search;

        let vector_name = query.get_vector_name().to_string();
        let query_vector = QueryVector::from(query);
        let with_payload = WithPayload::from(with_payload.unwrap_or_default());
        let with_vector = with_vector.unwrap_or_default();

        let context =
            QueryContext::new(DEFAULT_FULL_SCAN_THRESHOLD, HwMeasurementAcc::disposable());

        let mut points_by_segment = Vec::with_capacity(segments.len());

        for segment in segments {
            let batched_points = segment.get().read().search_batch(
                &vector_name,
                &[&query_vector],
                &with_payload,
                &with_vector,
                filter.as_ref(),
                offset + limit,
                params.as_ref(),
                &context.get_segment_query_context(),
            )?;

            debug_assert_eq!(batched_points.len(), 1);

            let [points] = batched_points
                .try_into()
                .expect("single batched search result");

            points_by_segment.push(points);
        }

        let mut aggregator = BatchResultAggregator::new([offset + limit]);
        aggregator.update_point_versions(points_by_segment.iter().flatten());

        for points in points_by_segment {
            aggregator.update_batch_results(0, points);
        }

        let [mut points] = aggregator
            .into_topk()
            .try_into()
            .expect("single batched search result");

        let distance = self
            .config
            .vector_data
            .get(&vector_name)
            .expect("vector config exist")
            .distance;

        match &query_vector {
            QueryVector::Nearest(_) => {
                for point in &mut points {
                    point.score = distance.postprocess_score(point.score);
                }
            }
            QueryVector::RecommendBestScore(_) => (),
            QueryVector::RecommendSumScores(_) => (),
            QueryVector::Discovery(_) => (),
            QueryVector::Context(_) => (),
            QueryVector::FeedbackSimple(_) => (),
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
