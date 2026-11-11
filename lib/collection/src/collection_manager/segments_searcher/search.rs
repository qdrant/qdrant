use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use common::types::ScoreType;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, TryStreamExt};
use ordered_float::Float;
use segment::data_types::query_context::QueryContext;
use segment::types::ScoredPoint;
use shard::search::CoreSearchRequestBatch;
use shard::search_result_aggregator::BatchResultAggregator;
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio_util::task::AbortOnDropHandle;

use super::segment_search::search_in_segment;
use super::{
    BatchOffset, BatchSearchResult, SegmentOffset, SegmentSearchExecutedResult, SegmentsSearcher,
};
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{CollectionError, CollectionResult};

impl SegmentsSearcher {
    /// Execute searches in parallel and return results in the same order as the searches were provided
    async fn execute_searches(
        searches: Vec<AbortOnDropHandle<SegmentSearchExecutedResult>>,
    ) -> CollectionResult<(BatchSearchResult, Vec<Vec<bool>>)> {
        let results_len = searches.len();

        let mut search_results_per_segment_res = FuturesUnordered::new();
        for (idx, search) in searches.into_iter().enumerate() {
            // map the result to include the request index for later reordering
            let result_with_request_index = search.map(move |res| res.map(|s| (idx, s)));
            search_results_per_segment_res.push(result_with_request_index);
        }

        let mut search_results_per_segment = vec![Vec::new(); results_len];
        let mut further_searches_per_segment = vec![Vec::new(); results_len];
        // process results as they come in and store them in the correct order
        while let Some((idx, search_result)) = search_results_per_segment_res.try_next().await? {
            let (search_results, further_searches) = search_result?;
            debug_assert!(search_results.len() == further_searches.len());
            search_results_per_segment[idx] = search_results;
            further_searches_per_segment[idx] = further_searches;
        }
        Ok((search_results_per_segment, further_searches_per_segment))
    }

    /// Processes search result of `[segment_size x batch_size]`.
    ///
    /// # Arguments
    /// * `search_result` - `[segment_size x batch_size]`
    /// * `limits` - `[batch_size]` - how many results to return for each batched request
    /// * `further_results` - `[segment_size x batch_size]` - whether we can search further in the segment
    ///
    /// Returns batch results aggregated by `[batch_size]` and list of queries, grouped by segment to re-run
    pub(crate) fn process_search_result_step1(
        search_result: BatchSearchResult,
        limits: Vec<usize>,
        further_results: &[Vec<bool>],
    ) -> (
        BatchResultAggregator,
        AHashMap<SegmentOffset, Vec<BatchOffset>>,
    ) {
        let number_segments = search_result.len();
        let batch_size = limits.len();

        // The lowest scored element must be larger or equal to the worst scored element in each segment.
        // Otherwise, the sampling is invalid and some points might be missing.
        // e.g. with 3 segments with the following sampled ranges:
        // s1 - [0.91 -> 0.87]
        // s2 - [0.92 -> 0.86]
        // s3 - [0.93 -> 0.85]
        // If the top merged scores result range is [0.93 -> 0.86] then we do not know if s1 could have contributed more points at the lower part between [0.87 -> 0.86]
        // In that case, we need to re-run the search without sampling on that segment.

        // Initialize result aggregators for each batched request
        let mut result_aggregator = BatchResultAggregator::new(limits.iter().copied());
        result_aggregator.update_point_versions(search_result.iter().flatten().flatten());

        // Therefore we need to track the lowest scored element per segment for each batch
        let mut lowest_scores_per_request: Vec<Vec<ScoreType>> = vec![
            vec![f32::max_value(); batch_size]; // initial max score value for each batch
            number_segments
        ];

        let mut retrieved_points_per_request: Vec<Vec<BatchOffset>> = vec![
            vec![0; batch_size]; // initial max score value for each batch
            number_segments
        ];

        // Batch results merged from all segments
        for (segment_idx, segment_result) in search_result.into_iter().enumerate() {
            // merge results for each batch search request across segments
            for (batch_req_idx, query_res) in segment_result.into_iter().enumerate() {
                retrieved_points_per_request[segment_idx][batch_req_idx] = query_res.len();
                lowest_scores_per_request[segment_idx][batch_req_idx] = query_res
                    .last()
                    .map(|x| x.score)
                    .unwrap_or_else(f32::min_value);
                result_aggregator.update_batch_results(batch_req_idx, query_res);
            }
        }

        // segment id -> list of batch ids
        let mut searches_to_rerun: AHashMap<SegmentOffset, Vec<BatchOffset>> = AHashMap::new();

        // Check if we want to re-run the search without sampling on some segments
        for (batch_id, required_limit) in limits.into_iter().enumerate() {
            let lowest_batch_score_opt = result_aggregator.batch_lowest_scores(batch_id);

            // If there are no results, we do not need to re-run the search
            if let Some(lowest_batch_score) = lowest_batch_score_opt {
                for segment_id in 0..number_segments {
                    let segment_lowest_score = lowest_scores_per_request[segment_id][batch_id];
                    let retrieved_points = retrieved_points_per_request[segment_id][batch_id];
                    let have_further_results = further_results[segment_id][batch_id];

                    if have_further_results
                        && retrieved_points < required_limit
                        && segment_lowest_score >= lowest_batch_score
                    {
                        log::debug!(
                            "Search to re-run without sampling on segment_id: {segment_id} segment_lowest_score: {segment_lowest_score}, lowest_batch_score: {lowest_batch_score}, retrieved_points: {retrieved_points}, required_limit: {required_limit}",
                        );
                        // It is possible, that current segment can have better results than
                        // the lowest score in the batch. In that case, we need to re-run the search
                        // without sampling on that segment.
                        searches_to_rerun
                            .entry(segment_id)
                            .or_default()
                            .push(batch_id);
                    }
                }
            }
        }

        (result_aggregator, searches_to_rerun)
    }

    pub async fn search(
        segments: LockedSegmentHolder,
        batch_request: Arc<CoreSearchRequestBatch>,
        runtime_handle: &AdaptiveSearchHandle,
        sampling_enabled: bool,
        query_context: QueryContext,
        timeout: Duration,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let start = Instant::now();
        let query_context_arc = Arc::new(query_context);

        // Using block to ensure `segments` variable is dropped in the end of it
        let (locked_segments, searches): (Vec<_>, Vec<_>) = {
            let segments: Vec<_> = {
                // Unfortunately, we have to do `segments.read()` twice, once in blocking task
                // and once here, due to `Send` bounds :/
                let Some(segments_lock) = segments.try_read_for(timeout) else {
                    return Err(CollectionError::timeout(timeout, "search"));
                };

                // Collect the segments first so we don't lock the segment holder during the operations.
                segments_lock
                    .non_appendable_then_appendable_segments()
                    .collect()
            };

            // Probabilistic sampling for the `limit` parameter avoids over-fetching points from segments.
            // e.g. 10 segments with limit 1000 would fetch 10000 points in total and discard 9000 points.
            // With probabilistic sampling we determine a smaller sampling limit for each segment.
            // Use probabilistic sampling if:
            // - sampling is enabled
            // - more than 1 segment
            // - segments are not empty
            let use_sampling = sampling_enabled
                && segments.len() > 1
                && query_context_arc.available_point_count() > 0;

            segments
                .into_iter()
                .map(|segment| {
                    let query_context_arc_segment = query_context_arc.clone();
                    // update timeout
                    let timeout = timeout.saturating_sub(start.elapsed());
                    let search = runtime_handle.spawn_blocking({
                        let (segment, batch_request) = (segment.clone(), batch_request.clone());
                        let cpu_utilization = query_context_arc.handoff().cpu_utilization();
                        move || {
                            cpu_utilization.measure(|| {
                                let segment_query_context =
                                    query_context_arc_segment.get_segment_query_context();

                                search_in_segment(
                                    segment,
                                    batch_request,
                                    use_sampling,
                                    &segment_query_context,
                                    timeout,
                                )
                            })
                        }
                    });

                    // We MUST wrap the search handle in AbortOnDropHandle to ensure that we skip
                    // all searches for futures that are already dropped. Not using this allows
                    // users to create a humongous queue of search tasks, even though the searches
                    // are already invalidated.
                    // See: <https://github.com/qdrant/qdrant/pull/7530>
                    let search = AbortOnDropHandle::new(search);

                    (segment, search)
                })
                .unzip()
        };

        // perform search on all segments concurrently
        // the resulting Vec is in the same order as the segment searches were provided.
        let (all_search_results_per_segment, further_results) =
            Self::execute_searches(searches).await?;
        debug_assert!(all_search_results_per_segment.len() == locked_segments.len());

        let (mut result_aggregator, searches_to_rerun) = Self::process_search_result_step1(
            all_search_results_per_segment,
            batch_request
                .searches
                .iter()
                .map(|request| request.limit + request.offset)
                .collect(),
            &further_results,
        );
        // The second step of the search is to re-run the search without sampling on some segments
        // Expected that this stage will be executed rarely
        if !searches_to_rerun.is_empty() {
            // TODO notify telemetry of failing sampling
            // Ensure consistent order of segment ids
            let searches_to_rerun: Vec<(SegmentOffset, Vec<BatchOffset>)> =
                searches_to_rerun.into_iter().collect();

            let secondary_searches: Vec<_> = {
                let mut res = vec![];
                for (segment_id, batch_ids) in searches_to_rerun.iter() {
                    let query_context_arc_segment = query_context_arc.clone();
                    let segment = locked_segments[*segment_id].clone();
                    let partial_batch_request = Arc::new(CoreSearchRequestBatch {
                        searches: batch_ids
                            .iter()
                            .map(|batch_id| batch_request.searches[*batch_id].clone())
                            .collect(),
                    });
                    // update timeout
                    let timeout = timeout.saturating_sub(start.elapsed());
                    let cpu_utilization = query_context_arc.handoff().cpu_utilization();
                    let handle = runtime_handle.spawn_blocking(move || {
                        cpu_utilization.measure(|| {
                            let segment_query_context =
                                query_context_arc_segment.get_segment_query_context();

                            search_in_segment(
                                segment,
                                partial_batch_request,
                                false,
                                &segment_query_context,
                                timeout,
                            )
                        })
                    });

                    // We MUST wrap the search handle in AbortOnDropHandle to ensure that we skip
                    // all searches for futures that are already dropped. Not using this allows
                    // users to create a humongous queue of search tasks, even though the searches
                    // are already invalidated.
                    // See: <https://github.com/qdrant/qdrant/pull/7530>
                    let handle = AbortOnDropHandle::new(handle);

                    res.push(handle);
                }
                res
            };

            let (secondary_search_results_per_segment, _) =
                Self::execute_searches(secondary_searches).await?;

            result_aggregator.update_point_versions(
                secondary_search_results_per_segment
                    .iter()
                    .flatten()
                    .flatten(),
            );

            for ((_segment_id, batch_ids), segments_result) in searches_to_rerun
                .into_iter()
                .zip(secondary_search_results_per_segment)
            {
                for (batch_id, secondary_batch_result) in batch_ids.into_iter().zip(segments_result)
                {
                    result_aggregator.update_batch_results(batch_id, secondary_batch_result);
                }
            }
        }

        let top_scores: Vec<_> = result_aggregator.into_topk();
        Ok(top_scores)
    }
}
