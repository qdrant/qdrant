use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::ScoreType;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, TryStreamExt};
use itertools::Itertools;
use ordered_float::Float;
use segment::common::operation_error::OperationError;
use segment::data_types::modifier::Modifier;
use segment::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use segment::data_types::vectors::QueryVector;
use segment::types::{
    Filter, Indexes, PointIdType, ScoredPoint, SearchParams, SegmentConfig, VectorName,
    WithPayload, WithPayloadInterface, WithVector,
};
use shard::common::stopping_guard::StoppingGuard;
use shard::query::query_context::{fill_query_context, init_query_context};
use shard::query::query_enum::QueryEnum;
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::retrieve_blocking;
use shard::search::CoreSearchRequestBatch;
use shard::search_result_aggregator::BatchResultAggregator;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::collection_manager::probabilistic_search_sampling::find_search_sampling_over_point_distribution;
use crate::config::CollectionConfigInternal;
use crate::operations::types::CollectionResult;
use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;

type BatchOffset = usize;
type SegmentOffset = usize;

// batch -> point for one segment
type SegmentBatchSearchResult = Vec<Vec<ScoredPoint>>;
// Segment -> batch -> point
type BatchSearchResult = Vec<SegmentBatchSearchResult>;

// Result of batch search in one segment
type SegmentSearchExecutedResult = CollectionResult<(SegmentBatchSearchResult, Vec<bool>)>;

/// Simple implementation of segment manager
///  - rebuild segment for memory optimization purposes
#[derive(Default)]
pub struct SegmentsSearcher;

impl SegmentsSearcher {
    /// Execute searches in parallel and return results in the same order as the searches were provided
    async fn execute_searches(
        searches: Vec<JoinHandle<SegmentSearchExecutedResult>>,
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
    /// * `further_searches` - `[segment_size x batch_size]` - whether we can search further in the segment
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
                result_aggregator.update_batch_results(batch_req_idx, query_res.into_iter());
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

    pub async fn prepare_query_context(
        segments: LockedSegmentHolder,
        batch_request: &CoreSearchRequestBatch,
        collection_config: &CollectionConfigInternal,
        is_stopped_guard: &StoppingGuard,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Option<QueryContext>> {
        let indexing_threshold_kb = collection_config
            .optimizer_config
            .indexing_threshold
            .unwrap_or(DEFAULT_INDEXING_THRESHOLD_KB);
        let full_scan_threshold_kb = collection_config.hnsw_config.full_scan_threshold;
        let search_optimized_threshold_kb = indexing_threshold_kb.max(full_scan_threshold_kb);

        let query_context = init_query_context(
            &batch_request.searches,
            search_optimized_threshold_kb,
            is_stopped_guard,
            hw_measurement_acc,
            |vector_name| {
                collection_config
                    .params
                    .get_sparse_vector_params_opt(vector_name)
                    .map(|params| params.modifier == Some(Modifier::Idf))
                    .unwrap_or(false)
            },
        );

        // Do blocking calls in a blocking task: `segment.get().read()` calls might block async runtime
        let task = tokio::task::spawn_blocking(move || fill_query_context(query_context, segments))
            .await?;
        Ok(task)
    }

    pub async fn search(
        segments: LockedSegmentHolder,
        batch_request: Arc<CoreSearchRequestBatch>,
        runtime_handle: &Handle,
        sampling_enabled: bool,
        query_context: QueryContext,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let query_context_arc = Arc::new(query_context);

        // Using block to ensure `segments` variable is dropped in the end of it
        let (locked_segments, searches): (Vec<_>, Vec<_>) = {
            // Unfortunately, we have to do `segments.read()` twice, once in blocking task
            // and once here, due to `Send` bounds :/
            let segments_lock = segments.read();
            let segments = segments_lock.non_appendable_then_appendable_segments();

            // Probabilistic sampling for the `limit` parameter avoids over-fetching points from segments.
            // e.g. 10 segments with limit 1000 would fetch 10000 points in total and discard 9000 points.
            // With probabilistic sampling we determine a smaller sampling limit for each segment.
            // Use probabilistic sampling if:
            // - sampling is enabled
            // - more than 1 segment
            // - segments are not empty
            let use_sampling = sampling_enabled
                && segments_lock.len() > 1
                && query_context_arc.available_point_count() > 0;

            segments
                .map(|segment| {
                    let query_context_arc_segment = query_context_arc.clone();

                    let search = runtime_handle.spawn_blocking({
                        let (segment, batch_request) = (segment.clone(), batch_request.clone());
                        move || {
                            let segment_query_context =
                                query_context_arc_segment.get_segment_query_context();

                            search_in_segment(
                                segment,
                                batch_request,
                                use_sampling,
                                &segment_query_context,
                            )
                        }
                    });
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

                    res.push(runtime_handle.spawn_blocking(move || {
                        let segment_query_context =
                            query_context_arc_segment.get_segment_query_context();

                        search_in_segment(
                            segment,
                            partial_batch_request,
                            false,
                            &segment_query_context,
                        )
                    }))
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
                .zip(secondary_search_results_per_segment.into_iter())
            {
                for (batch_id, secondary_batch_result) in
                    batch_ids.into_iter().zip(segments_result.into_iter())
                {
                    result_aggregator
                        .update_batch_results(batch_id, secondary_batch_result.into_iter());
                }
            }
        }

        let top_scores: Vec<_> = result_aggregator.into_topk();
        Ok(top_scores)
    }

    /// Retrieve records for the given points ids from the segments
    /// - if payload is enabled, payload will be fetched
    /// - if vector is enabled, vector will be fetched
    ///
    /// The points ids can contain duplicates, the records will be fetched only once
    ///
    /// If an id is not found in the segments, it won't be included in the output.
    pub async fn retrieve(
        segments: LockedSegmentHolder,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        runtime_handle: &Handle,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<AHashMap<PointIdType, RecordInternal>> {
        let stopping_guard = StoppingGuard::new();
        let points = runtime_handle
            .spawn_blocking({
                let segments = segments.clone();
                let points = points.to_vec();
                let with_payload = with_payload.clone();
                let with_vector = with_vector.clone();
                let is_stopped = stopping_guard.get_is_stopped();
                // TODO create one Task per segment level retrieve
                move || {
                    retrieve_blocking(
                        segments,
                        &points,
                        &with_payload,
                        &with_vector,
                        &is_stopped,
                        hw_measurement_acc,
                    )
                }
            })
            .await??;
        Ok(points)
    }

    pub async fn read_filtered(
        segments: LockedSegmentHolder,
        filter: Option<&Filter>,
        runtime_handle: &Handle,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let stopping_guard = StoppingGuard::new();
        // cloning filter spawning task
        let filter = filter.cloned();
        runtime_handle
            .spawn_blocking(move || {
                let is_stopped = stopping_guard.get_is_stopped();
                let segments = segments.read();
                let hw_counter = hw_measurement_acc.get_counter_cell();
                let all_points: BTreeSet<_> = segments
                    .non_appendable_then_appendable_segments()
                    .flat_map(|segment| {
                        segment.get().read().read_filtered(
                            None,
                            None,
                            filter.as_ref(),
                            &is_stopped,
                            &hw_counter,
                        )
                    })
                    .collect();
                Ok(all_points)
            })
            .await?
    }

    /// Rescore results with a formula that can reference payload values.
    ///
    /// Aggregates rescores from the segments.
    pub async fn rescore_with_formula(
        segments: LockedSegmentHolder,
        arc_ctx: Arc<FormulaContext>,
        runtime_handle: &Handle,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let limit = arc_ctx.limit;

        let mut futures = {
            let segments_guard = segments.read();
            segments_guard
                .non_appendable_then_appendable_segments()
                .map(|segment| {
                    runtime_handle.spawn_blocking({
                        let segment = segment.clone();
                        let arc_ctx = arc_ctx.clone();
                        let hw_counter = hw_measurement_acc.get_counter_cell();
                        move || {
                            segment
                                .get()
                                .read()
                                .rescore_with_formula(arc_ctx, &hw_counter)
                        }
                    })
                })
                .collect::<FuturesUnordered<_>>()
        };

        let mut segments_results = Vec::with_capacity(futures.len());
        while let Some(result) = futures.try_next().await? {
            segments_results.push(result?)
        }

        // use aggregator with only one "batch"
        let mut aggregator = BatchResultAggregator::new(std::iter::once(limit));
        aggregator.update_point_versions(segments_results.iter().flatten());
        aggregator.update_batch_results(0, segments_results.into_iter().flatten());
        let top =
            aggregator.into_topk().into_iter().next().ok_or_else(|| {
                OperationError::service_error("expected first result of aggregator")
            })?;

        Ok(top)
    }
}

#[derive(PartialEq, Default, Debug)]
pub enum SearchType {
    #[default]
    Nearest,
    RecommendBestScore,
    RecommendSumScores,
    Discover,
    Context,
    FeedbackSimple,
}

impl From<&QueryEnum> for SearchType {
    fn from(query: &QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(_) => Self::Nearest,
            QueryEnum::RecommendBestScore(_) => Self::RecommendBestScore,
            QueryEnum::RecommendSumScores(_) => Self::RecommendSumScores,
            QueryEnum::Discover(_) => Self::Discover,
            QueryEnum::Context(_) => Self::Context,
            QueryEnum::FeedbackSimple(_) => Self::FeedbackSimple,
        }
    }
}

#[derive(PartialEq, Default, Debug)]
struct BatchSearchParams<'a> {
    pub search_type: SearchType,
    pub vector_name: &'a VectorName,
    pub filter: Option<&'a Filter>,
    pub with_payload: WithPayload,
    pub with_vector: WithVector,
    pub top: usize,
    pub params: Option<&'a SearchParams>,
}

/// Returns suggested search sampling size for a given number of points and required limit.
fn sampling_limit(
    limit: usize,
    ef_limit: Option<usize>,
    segment_points: usize,
    total_points: usize,
) -> usize {
    // shortcut empty segment
    if segment_points == 0 {
        return 0;
    }
    let segment_probability = segment_points as f64 / total_points as f64;
    let poisson_sampling =
        find_search_sampling_over_point_distribution(limit as f64, segment_probability);

    // if no ef_limit was found, it is a plain index => sampling optimization is not needed.
    let effective = ef_limit.map_or(limit, |ef_limit| {
        effective_limit(limit, ef_limit, poisson_sampling)
    });
    log::trace!(
        "sampling: {effective}, poisson: {poisson_sampling} segment_probability: {segment_probability}, segment_points: {segment_points}, total_points: {total_points}",
    );
    effective
}

/// Determines the effective ef limit value for the given parameters.
fn effective_limit(limit: usize, ef_limit: usize, poisson_sampling: usize) -> usize {
    // Prefer the highest of poisson_sampling/ef_limit, but never be higher than limit
    poisson_sampling.max(ef_limit).min(limit)
}

/// Process sequentially contiguous batches
///
/// # Arguments
///
/// * `segment` - Locked segment to search in
/// * `request` - Batch of search requests
/// * `use_sampling` - If true, try to use probabilistic sampling
/// * `query_context` - Additional context for the search
///
/// # Returns
///
/// Collection Result of:
/// * Vector of ScoredPoints for each request in the batch
/// * Vector of boolean indicating if the segment have further points to search
fn search_in_segment(
    segment: LockedSegment,
    request: Arc<CoreSearchRequestBatch>,
    use_sampling: bool,
    segment_query_context: &SegmentQueryContext,
) -> CollectionResult<(Vec<Vec<ScoredPoint>>, Vec<bool>)> {
    let batch_size = request.searches.len();

    let mut result: Vec<Vec<ScoredPoint>> = Vec::with_capacity(batch_size);
    let mut further_results: Vec<bool> = Vec::with_capacity(batch_size); // if segment have more points to return
    let mut vectors_batch: Vec<QueryVector> = vec![];
    let mut prev_params = BatchSearchParams::default();

    for search_query in &request.searches {
        let with_payload_interface = search_query
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));

        let params = BatchSearchParams {
            search_type: search_query.query.as_ref().into(),
            vector_name: search_query.query.get_vector_name(),
            filter: search_query.filter.as_ref(),
            with_payload: WithPayload::from(with_payload_interface),
            with_vector: search_query.with_vector.clone().unwrap_or_default(),
            top: search_query.limit + search_query.offset,
            params: search_query.params.as_ref(),
        };

        let query = search_query.query.clone().into();

        // same params enables batching (cmp expensive on large filters)
        if params == prev_params {
            vectors_batch.push(query);
        } else {
            // different params means different batches
            // execute what has been batched so far
            if !vectors_batch.is_empty() {
                let (mut res, mut further) = execute_batch_search(
                    &segment,
                    &vectors_batch,
                    &prev_params,
                    use_sampling,
                    segment_query_context,
                )?;
                further_results.append(&mut further);
                result.append(&mut res);
                vectors_batch.clear()
            }
            // start new batch for current search query
            vectors_batch.push(query);
            prev_params = params;
        }
    }

    // run last batch if any
    if !vectors_batch.is_empty() {
        let (mut res, mut further) = execute_batch_search(
            &segment,
            &vectors_batch,
            &prev_params,
            use_sampling,
            segment_query_context,
        )?;
        further_results.append(&mut further);
        result.append(&mut res);
    }

    Ok((result, further_results))
}

fn execute_batch_search(
    segment: &LockedSegment,
    vectors_batch: &[QueryVector],
    search_params: &BatchSearchParams,
    use_sampling: bool,
    segment_query_context: &SegmentQueryContext,
) -> CollectionResult<(Vec<Vec<ScoredPoint>>, Vec<bool>)> {
    let locked_segment = segment.get();
    let read_segment = locked_segment.read();

    let segment_points = read_segment.available_point_count();
    let segment_config = read_segment.config();

    let top = if use_sampling {
        let ef_limit = search_params
            .params
            .and_then(|p| p.hnsw_ef)
            .or_else(|| get_hnsw_ef_construct(segment_config, search_params.vector_name));
        sampling_limit(
            search_params.top,
            ef_limit,
            segment_points,
            segment_query_context.available_point_count(),
        )
    } else {
        search_params.top
    };

    let vectors_batch = &vectors_batch.iter().collect_vec();
    let res = read_segment.search_batch(
        search_params.vector_name,
        vectors_batch,
        &search_params.with_payload,
        &search_params.with_vector,
        search_params.filter,
        top,
        search_params.params,
        segment_query_context,
    )?;

    let further_results = res
        .iter()
        .map(|batch_result| batch_result.len() == top)
        .collect();

    Ok((res, further_results))
}

/// Find the HNSW ef_construct for a named vector
///
/// If the given named vector has no HNSW index, `None` is returned.
fn get_hnsw_ef_construct(config: &SegmentConfig, vector_name: &VectorName) -> Option<usize> {
    config
        .vector_data
        .get(vector_name)
        .and_then(|config| match &config.index {
            Indexes::Plain {} => None,
            Indexes::Hnsw(hnsw) => Some(hnsw),
        })
        .map(|hnsw| hnsw.ef_construct)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use ahash::AHashSet;
    use api::rest::SearchRequestInternal;
    use common::counter::hardware_counter::HardwareCounterCell;
    use parking_lot::RwLock;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::fixtures::index_fixtures::random_vector;
    use segment::index::VectorIndexEnum;
    use segment::types::{Condition, HasIdCondition};
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{build_test_holder, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::operations::types::CoreSearchRequest;
    use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;

    #[test]
    fn test_is_indexed_enough_condition() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment1 = random_segment(dir.path(), 10, 200, 256);

        let vector_index = segment1
            .vector_data
            .get(DEFAULT_VECTOR_NAME)
            .unwrap()
            .vector_index
            .clone();

        let vector_index_borrow = vector_index.borrow();

        let hw_counter = HardwareCounterCell::new();

        match &*vector_index_borrow {
            VectorIndexEnum::Plain(plain_index) => {
                let res_1 = plain_index.is_small_enough_for_unindexed_search(25, None, &hw_counter);
                assert!(!res_1);

                let res_2 =
                    plain_index.is_small_enough_for_unindexed_search(225, None, &hw_counter);
                assert!(res_2);

                let ids: AHashSet<_> = vec![1, 2].into_iter().map(PointIdType::from).collect();

                let ids_filter = Filter::new_must(Condition::HasId(HasIdCondition::from(ids)));

                let res_3 = plain_index.is_small_enough_for_unindexed_search(
                    25,
                    Some(&ids_filter),
                    &hw_counter,
                );
                assert!(res_3);
            }
            _ => panic!("Expected plain index"),
        }
    }

    #[tokio::test]
    async fn test_segments_search() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment_holder = build_test_holder(dir.path());

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let req = CoreSearchRequest {
            query: query.into(),
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            limit: 5,
            score_threshold: None,
            offset: 0,
        };

        let batch_request = CoreSearchRequestBatch {
            searches: vec![req],
        };

        let hw_acc = HwMeasurementAcc::new();
        let result = SegmentsSearcher::search(
            Arc::new(segment_holder),
            Arc::new(batch_request),
            &Handle::current(),
            true,
            QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, hw_acc),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        // eprintln!("result = {:?}", &result);

        assert_eq!(result.len(), 5);

        assert!(result[0].id == 3.into() || result[0].id == 11.into());
        assert!(result[1].id == 3.into() || result[1].id == 11.into());
    }

    #[tokio::test]
    async fn test_segments_search_sampling() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment1 = random_segment(dir.path(), 10, 2000, 4);
        let segment2 = random_segment(dir.path(), 10, 4000, 4);

        let mut holder = SegmentHolder::default();

        let _sid1 = holder.add_new(segment1);
        let _sid2 = holder.add_new(segment2);

        let segment_holder = Arc::new(RwLock::new(holder));

        let mut rnd = rand::rng();

        for _ in 0..100 {
            let req1 = SearchRequestInternal {
                vector: random_vector(&mut rnd, 4).into(),
                limit: 150, // more than LOWER_SEARCH_LIMIT_SAMPLING
                offset: None,
                with_payload: None,
                with_vector: None,
                filter: None,
                params: None,
                score_threshold: None,
            };
            let req2 = SearchRequestInternal {
                vector: random_vector(&mut rnd, 4).into(),
                limit: 50, // less than LOWER_SEARCH_LIMIT_SAMPLING
                offset: None,
                filter: None,
                params: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            };

            let batch_request = CoreSearchRequestBatch {
                searches: vec![req1.into(), req2.into()],
            };

            let batch_request = Arc::new(batch_request);

            let hw_measurement_acc = HwMeasurementAcc::new();
            let query_context =
                QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, hw_measurement_acc.clone());

            let result_no_sampling = SegmentsSearcher::search(
                segment_holder.clone(),
                batch_request.clone(),
                &Handle::current(),
                false,
                query_context,
            )
            .await
            .unwrap();

            assert_ne!(hw_measurement_acc.get_cpu(), 0);

            let hw_measurement_acc = HwMeasurementAcc::new();
            let query_context =
                QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, hw_measurement_acc.clone());

            assert!(!result_no_sampling.is_empty());

            let result_sampling = SegmentsSearcher::search(
                segment_holder.clone(),
                batch_request,
                &Handle::current(),
                true,
                query_context,
            )
            .await
            .unwrap();
            assert!(!result_sampling.is_empty());

            assert_ne!(hw_measurement_acc.get_cpu(), 0);

            // assert equivalence in depth
            assert_eq!(result_no_sampling[0].len(), result_sampling[0].len());
            assert_eq!(result_no_sampling[1].len(), result_sampling[1].len());

            for (no_sampling, sampling) in
                result_no_sampling[0].iter().zip(result_sampling[0].iter())
            {
                assert_eq!(no_sampling.score, sampling.score); // different IDs may have same scores
            }
        }
    }

    #[test]
    fn test_retrieve() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment_holder = build_test_holder(dir.path());
        let records = retrieve_blocking(
            Arc::new(segment_holder),
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
            &AtomicBool::new(false),
            HwMeasurementAcc::new(),
        )
        .unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_sampling_limit() {
        assert_eq!(sampling_limit(1000, None, 464530, 35103551), 1000);
    }

    #[test]
    fn test_sampling_limit_ef() {
        assert_eq!(sampling_limit(1000, Some(100), 464530, 35103551), 100);
    }

    #[test]
    fn test_sampling_limit_high() {
        assert_eq!(sampling_limit(1000000, None, 464530, 35103551), 1000000);
    }

    /// Tests whether calculating the effective ef limit value is correct.
    ///
    /// Because there was confusion about what the effective value should be for some input
    /// combinations, we decided to write this tests to ensure correctness.
    ///
    /// See: <https://github.com/qdrant/qdrant/pull/1694>
    #[test]
    fn test_effective_limit() {
        // Test cases to assert: (limit, ef_limit, poisson_sampling, effective)
        let tests = [
            (1000, 128, 150, 150),
            (1000, 128, 110, 128),
            (130, 128, 150, 130),
            (130, 128, 110, 128),
            (50, 128, 150, 50),
            (50, 128, 110, 50),
            (500, 1000, 300, 500),
            (500, 400, 300, 400),
            (1000, 0, 150, 150),
            (1000, 0, 110, 110),
        ];
        tests.into_iter().for_each(|(limit, ef_limit, poisson_sampling, effective)| assert_eq!(
            effective_limit(limit, ef_limit, poisson_sampling),
            effective,
            "effective limit for [limit: {limit}, ef_limit: {ef_limit}, poisson_sampling: {poisson_sampling}] must be {effective}",
        ));
    }
}
