use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::types::ScoreType;
use futures::future::try_join_all;
use itertools::Itertools;
use ordered_float::Float;
use parking_lot::RwLock;
use segment::common::operation_error::OperationError;
use segment::common::BYTES_IN_KB;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::QueryVector;
use segment::entry::entry_point::SegmentEntry;
use segment::types::{
    Filter, Indexes, PointIdType, ScoredPoint, SearchParams, SegmentConfig, SeqNumberType,
    WithPayload, WithPayloadInterface, WithVector, VECTOR_ELEMENT_SIZE,
};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::probabilistic_segment_search_sampling::find_search_sampling_over_point_distribution;
use crate::collection_manager::search_result_aggregator::BatchResultAggregator;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequestBatch, QueryEnum, Record,
};

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
pub struct SegmentsSearcher {}

impl SegmentsSearcher {
    async fn execute_searches(
        searches: Vec<JoinHandle<SegmentSearchExecutedResult>>,
    ) -> CollectionResult<(BatchSearchResult, Vec<Vec<bool>>)> {
        let searches = try_join_all(searches);
        let search_results_per_segment_res = searches.await?;

        let mut search_results_per_segment = vec![];
        let mut further_searches_per_segment = vec![];
        for search_result in search_results_per_segment_res {
            let (search_results, further_searches) = search_result?;
            debug_assert!(search_results.len() == further_searches.len());
            search_results_per_segment.push(search_results);
            further_searches_per_segment.push(further_searches);
        }
        Ok((search_results_per_segment, further_searches_per_segment))
    }

    /// Processes search result of [segment_size x batch_size]
    ///
    /// # Arguments
    /// * search_result - [segment_size x batch_size]
    /// * limits - [batch_size] - how many results to return for each batched request
    /// * further_searches - [segment_size x batch_size] - whether we can search further in the segment
    ///
    /// Returns batch results aggregated by [batch_size] and list of queries, grouped by segment to re-run
    pub(crate) fn process_search_result_step1(
        search_result: BatchSearchResult,
        limits: Vec<usize>,
        further_results: Vec<Vec<bool>>,
    ) -> (
        BatchResultAggregator,
        HashMap<SegmentOffset, Vec<BatchOffset>>,
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
        result_aggregator.update_point_versions(&search_result);

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
        let mut searches_to_rerun: HashMap<SegmentOffset, Vec<BatchOffset>> = HashMap::new();

        // Check if we want to re-run the search without sampling on some segments
        for (batch_id, required_limit) in limits.iter().copied().enumerate() {
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
                        log::debug!("Search to re-run without sampling on segment_id: {segment_id} segment_lowest_score: {segment_lowest_score}, lowest_batch_score: {lowest_batch_score}, retrieved_points: {retrieved_points}, required_limit: {required_limit}");
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
        segments: Arc<RwLock<SegmentHolder>>,
        batch_request: Arc<CoreSearchRequestBatch>,
        runtime_handle: &Handle,
        sampling_enabled: bool,
        is_stopped: Arc<AtomicBool>,
        search_optimized_threshold_kb: usize,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Do blocking calls in a blocking task: `segment.get().read()` calls might block async runtime
        let task = {
            let segments = segments.clone();

            tokio::task::spawn_blocking(move || {
                let segments = segments.read();

                if !segments.is_empty() {
                    let available_point_count = segments
                        .iter()
                        .map(|(_, segment)| segment.get().read().available_point_count())
                        .sum();

                    Some(available_point_count)
                } else {
                    None
                }
            })
        };

        let Some(available_point_count) = task.await? else {
            return Ok(Vec::new());
        };

        // Using block to ensure `segments` variable is dropped in the end of it
        let (locked_segments, searches): (Vec<_>, Vec<_>) = {
            // Unfortunately, we have to do `segments.read()` twice, once in blocking task
            // and once here, due to `Send` bounds :/
            let segments = segments.read();

            // Probabilistic sampling for the `limit` parameter avoids over-fetching points from segments.
            // e.g. 10 segments with limit 1000 would fetch 10000 points in total and discard 9000 points.
            // With probabilistic sampling we determine a smaller sampling limit for each segment.
            // Use probabilistic sampling if:
            // - sampling is enabled
            // - more than 1 segment
            // - segments are not empty
            let use_sampling = sampling_enabled && segments.len() > 1 && available_point_count > 0;

            segments
                .iter()
                .map(|(_id, segment)| {
                    let search = runtime_handle.spawn_blocking({
                        let (segment, batch_request) = (segment.clone(), batch_request.clone());
                        let is_stopped_clone = is_stopped.clone();
                        move || {
                            search_in_segment(
                                segment,
                                batch_request,
                                available_point_count,
                                use_sampling,
                                &is_stopped_clone,
                                search_optimized_threshold_kb,
                            )
                        }
                    });
                    (segment.clone(), search)
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
            further_results,
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
                    let segment = locked_segments[*segment_id].clone();
                    let partial_batch_request = Arc::new(CoreSearchRequestBatch {
                        searches: batch_ids
                            .iter()
                            .map(|batch_id| batch_request.searches[*batch_id].clone())
                            .collect(),
                    });
                    let is_stopped_clone = is_stopped.clone();
                    res.push(runtime_handle.spawn_blocking(move || {
                        search_in_segment(
                            segment,
                            partial_batch_request,
                            0,
                            false,
                            &is_stopped_clone,
                            search_optimized_threshold_kb,
                        )
                    }))
                }
                res
            };

            let (secondary_search_results_per_segment, _) =
                Self::execute_searches(secondary_searches).await?;

            result_aggregator.update_point_versions(&secondary_search_results_per_segment);

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

    pub fn retrieve(
        segments: &RwLock<SegmentHolder>,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        let mut point_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut point_records: HashMap<PointIdType, Record> = Default::default();

        segments.read().read_points(points, |id, segment| {
            let version = segment.point_version(id).ok_or_else(|| {
                OperationError::service_error(format!("No version for point {id}"))
            })?;
            // If this point was not found yet or this segment have later version
            if !point_version.contains_key(&id) || point_version[&id] < version {
                point_records.insert(
                    id,
                    Record {
                        id,
                        payload: if with_payload.enable {
                            if let Some(selector) = &with_payload.payload_selector {
                                Some(selector.process(segment.payload(id)?))
                            } else {
                                Some(segment.payload(id)?)
                            }
                        } else {
                            None
                        },
                        vector: match with_vector {
                            WithVector::Bool(true) => Some(segment.all_vectors(id)?.into()),
                            WithVector::Bool(false) => None,
                            WithVector::Selector(vector_names) => {
                                let mut selected_vectors = NamedVectors::default();
                                for vector_name in vector_names {
                                    if let Some(vector) = segment.vector(vector_name, id)? {
                                        selected_vectors.insert(vector_name.into(), vector);
                                    }
                                }
                                Some(selected_vectors.into())
                            }
                        },
                        shard_key: None,
                    },
                );
                point_version.insert(id, version);
            }
            Ok(true)
        })?;

        // Restore the order the ids came in
        let ordered_records = points
            .iter()
            .flat_map(|point| point_records.remove(point))
            .collect();

        Ok(ordered_records)
    }
}

#[derive(PartialEq, Default, Debug)]
pub enum SearchType {
    #[default]
    Nearest,
    RecommendBestScore,
    Discover,
    Context,
}

impl From<&QueryEnum> for SearchType {
    fn from(query: &QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(_) => Self::Nearest,
            QueryEnum::RecommendBestScore(_) => Self::RecommendBestScore,
            QueryEnum::Discover(_) => Self::Discover,
            QueryEnum::Context(_) => Self::Context,
        }
    }
}

#[derive(PartialEq, Default, Debug)]
struct BatchSearchParams<'a> {
    pub search_type: SearchType,
    pub vector_name: &'a str,
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
        find_search_sampling_over_point_distribution(limit as f64, segment_probability)
            .unwrap_or(limit);

    // if no ef_limit was found, it is a plain index => sampling optimization is not needed.
    let effective = ef_limit.map_or(limit, |ef_limit| {
        effective_limit(limit, ef_limit, poisson_sampling)
    });
    log::trace!("sampling: {effective}, poisson: {poisson_sampling} segment_probability: {segment_probability}, segment_points: {segment_points}, total_points: {total_points}");
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
/// * `total_points` - Number of points in all segments combined
/// * `use_sampling` - If true, try to use probabilistic sampling
/// * `is_stopped` - Atomic bool to check if search is stopped
/// * `indexing_threshold` - If `indexed_only` is enabled, the search will skip
///                          segments with more than this number Kb of un-indexed vectors
///
/// # Returns
///
/// Collection Result of:
/// * Vector of ScoredPoints for each request in the batch
/// * Vector of boolean indicating if the segment have further points to search
fn search_in_segment(
    segment: LockedSegment,
    request: Arc<CoreSearchRequestBatch>,
    total_points: usize,
    use_sampling: bool,
    is_stopped: &AtomicBool,
    search_optimized_threshold_kb: usize,
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

        // same params enables batching
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
                    total_points,
                    is_stopped,
                    search_optimized_threshold_kb,
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
            total_points,
            is_stopped,
            search_optimized_threshold_kb,
        )?;
        further_results.append(&mut further);
        result.append(&mut res);
    }

    Ok((result, further_results))
}

fn execute_batch_search(
    segment: &LockedSegment,
    vectors_batch: &Vec<QueryVector>,
    search_params: &BatchSearchParams,
    use_sampling: bool,
    total_points: usize,
    is_stopped: &AtomicBool,
    search_optimized_threshold_kb: usize,
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
        sampling_limit(search_params.top, ef_limit, segment_points, total_points)
    } else {
        search_params.top
    };

    let ignore_plain_index = search_params
        .params
        .map(|p| p.indexed_only)
        .unwrap_or(false);
    if ignore_plain_index
        && !is_search_optimized(
            read_segment.deref(),
            search_optimized_threshold_kb,
            search_params.vector_name,
        )?
    {
        let batch_len = vectors_batch.len();
        return Ok((vec![vec![]; batch_len], vec![false; batch_len]));
    }
    let vectors_batch = &vectors_batch.iter().collect_vec();
    let res = read_segment.search_batch(
        search_params.vector_name,
        vectors_batch,
        &search_params.with_payload,
        &search_params.with_vector,
        search_params.filter,
        top,
        search_params.params,
        is_stopped,
    )?;

    let further_results = res
        .iter()
        .map(|batch_result| batch_result.len() == top)
        .collect();

    Ok((res, further_results))
}

/// Check if the segment is indexed enough to be searched with `indexed_only` parameter
fn is_search_optimized(
    segment: &dyn SegmentEntry,
    search_optimized_threshold_kb: usize,
    vector_name: &str,
) -> CollectionResult<bool> {
    let segment_info = segment.info();
    let vector_name_error =
        || CollectionError::bad_request(format!("Vector {} doesn't exist", vector_name));

    let vector_data_info = segment_info
        .vector_data
        .get(vector_name)
        .ok_or_else(vector_name_error)?;

    // check only dense vectors because sparse vectors are always indexed
    let vector_size = segment
        .config()
        .vector_data
        .get(vector_name)
        .ok_or_else(vector_name_error)?
        .size;

    let vector_size_bytes = vector_size * VECTOR_ELEMENT_SIZE;

    let unindexed_vectors = vector_data_info
        .num_vectors
        .saturating_sub(vector_data_info.num_indexed_vectors);

    let unindexed_volume = vector_size_bytes.saturating_mul(unindexed_vectors);

    let indexing_threshold_bytes = search_optimized_threshold_kb * BYTES_IN_KB;

    // Examples:
    // Threshold = 20_000 Kb
    // Indexed vectors: 100000
    // Total vectors: 100010
    // unindexed_volume = 100010 - 100000 = 10
    // Result: true

    // Threshold = 20_000 Kb
    // Indexed vectors: 0
    // Total vectors: 100000
    // unindexed_volume = 100000
    // Result: false
    Ok(unindexed_volume < indexing_threshold_bytes)
}

/// Find the HNSW ef_construct for a named vector
///
/// If the given named vector has no HNSW index, `None` is returned.
fn get_hnsw_ef_construct(config: &SegmentConfig, vector_name: &str) -> Option<usize> {
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
    use segment::fixtures::index_fixtures::random_vector;
    use segment::types::SegmentType;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{
        build_test_holder, optimize_segment, random_segment,
    };
    use crate::operations::types::{CoreSearchRequest, SearchRequestInternal};
    use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;

    #[test]
    fn test_is_indexed_enough_condition() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment1 = random_segment(dir.path(), 10, 200, 256);

        let res_1 = is_search_optimized(&segment1, 25, "").unwrap();

        assert!(!res_1);

        let res_2 = is_search_optimized(&segment1, 225, "").unwrap();

        assert!(res_2);

        let indexed_segment = optimize_segment(segment1);

        let indexed_segment_get = indexed_segment.get();
        let indexed_segment_read = indexed_segment_get.read();

        assert_eq!(
            indexed_segment_read.info().segment_type,
            SegmentType::Indexed
        );

        let res_3 = is_search_optimized(indexed_segment_read.deref(), 25, "").unwrap();

        assert!(res_3);
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

        let result = SegmentsSearcher::search(
            Arc::new(segment_holder),
            Arc::new(batch_request),
            &Handle::current(),
            true,
            Arc::new(AtomicBool::new(false)),
            DEFAULT_INDEXING_THRESHOLD_KB,
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

        let _sid1 = holder.add(segment1);
        let _sid2 = holder.add(segment2);

        let segment_holder = Arc::new(RwLock::new(holder));

        let mut rnd = rand::thread_rng();

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

            let result_no_sampling = SegmentsSearcher::search(
                segment_holder.clone(),
                batch_request.clone(),
                &Handle::current(),
                false,
                Arc::new(false.into()),
                DEFAULT_INDEXING_THRESHOLD_KB,
            )
            .await
            .unwrap();

            assert!(!result_no_sampling.is_empty());

            let result_sampling = SegmentsSearcher::search(
                segment_holder.clone(),
                batch_request,
                &Handle::current(),
                true,
                Arc::new(false.into()),
                DEFAULT_INDEXING_THRESHOLD_KB,
            )
            .await
            .unwrap();
            assert!(!result_sampling.is_empty());

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

        let records = SegmentsSearcher::retrieve(
            &segment_holder,
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
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
