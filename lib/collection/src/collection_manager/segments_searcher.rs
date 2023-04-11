use std::collections::HashMap;
use std::sync::Arc;

use futures::future::try_join_all;
use ordered_float::Float;
use parking_lot::RwLock;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorElementType;
use segment::entry::entry_point::OperationError;
use segment::types::{
    Filter, Indexes, PointIdType, ScoreType, ScoredPoint, SearchParams, SegmentConfig,
    SeqNumberType, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::probabilistic_segment_search_sampling::find_search_sampling_over_point_distribution;
use crate::collection_manager::search_result_aggregator::BatchResultAggregator;
use crate::operations::types::{CollectionResult, Record, SearchRequestBatch};

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
                            .or_insert_with(Vec::new)
                            .push(batch_id);
                    }
                }
            }
        }

        (result_aggregator, searches_to_rerun)
    }

    pub async fn search(
        segments: &RwLock<SegmentHolder>,
        batch_request: Arc<SearchRequestBatch>,
        runtime_handle: &Handle,
        sampling_enabled: bool,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Using { } block to ensure segments variable is dropped in the end of it
        // and is not transferred across the all_searches.await? boundary as it
        // does not impl Send trait
        let (locked_segments, searches): (Vec<_>, Vec<_>) = {
            let segments = segments.read();

            let some_segment = segments.iter().next();

            if some_segment.is_none() {
                return Ok(vec![]);
            }

            // Probabilistic sampling for the `limit` parameter avoids over-fetching points from segments.
            // e.g. 10 segments with limit 1000 would fetch 10000 points in total and discard 9000 points.
            // With probabilistic sampling we determine a smaller sampling limit for each segment.
            // Use probabilistic sampling if:
            // - sampling is enabled
            // - more than 1 segment
            // - segments are not empty
            let total_points_segments = segments
                .iter()
                .map(|(_, segment)| segment.get().read().points_count())
                .sum();
            let use_sampling = sampling_enabled && segments.len() > 1 && total_points_segments > 0;

            segments
                .iter()
                .map(|(_id, segment)| {
                    (
                        segment.clone(),
                        search_in_segment(
                            segment.clone(),
                            batch_request.clone(),
                            total_points_segments,
                            use_sampling,
                        ),
                    )
                })
                .map(|(segment, f)| (segment, runtime_handle.spawn(f)))
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
                    let partial_batch_request = Arc::new(SearchRequestBatch {
                        searches: batch_ids
                            .iter()
                            .map(|batch_id| batch_request.searches[*batch_id].clone())
                            .collect(),
                    });

                    let search = search_in_segment(segment, partial_batch_request, 0, false);
                    res.push(runtime_handle.spawn(search))
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

    pub async fn retrieve(
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
                                    selected_vectors.insert(
                                        vector_name.clone(),
                                        segment.vector(vector_name, id)?,
                                    );
                                }
                                Some(selected_vectors.into())
                            }
                        },
                    },
                );
                point_version.insert(id, version);
            }
            Ok(true)
        })?;
        Ok(point_records.into_values().collect())
    }
}

#[derive(PartialEq, Default)]
struct BatchSearchParams<'a> {
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
    let res = if poisson_sampling > limit {
        // sampling cannot be greater than limit
        return limit;
    } else {
        // sampling should not be less than ef_limit
        poisson_sampling.max(ef_limit.unwrap_or(0))
    };
    log::trace!("sampling: {res}, poisson: {poisson_sampling} segment_probability: {segment_probability}, segment_points: {segment_points}, total_points: {total_points}");
    res
}

/// Process sequentially contiguous batches
///
/// # Arguments
///
/// * `segment` - Locked segment to search in
/// * `request` - Batch of search requests
/// * `total_points` - Number of points in all segments combined
/// * `use_sampling` - If true, try to use probabilistic sampling
///
/// # Returns
///
/// Collection Result of:
/// * Vector of ScoredPoints for each request in the batch
/// * Vector of boolean indicating if the segment have further points to search
async fn search_in_segment(
    segment: LockedSegment,
    request: Arc<SearchRequestBatch>,
    total_points: usize,
    use_sampling: bool,
) -> CollectionResult<(Vec<Vec<ScoredPoint>>, Vec<bool>)> {
    let batch_size = request.searches.len();

    let mut result: Vec<Vec<ScoredPoint>> = Vec::with_capacity(batch_size);
    let mut further_results: Vec<bool> = Vec::with_capacity(batch_size); // true if segment have more points to return
    let mut vectors_batch: Vec<&[VectorElementType]> = vec![];
    let mut prev_params = BatchSearchParams::default();

    for search_query in &request.searches {
        let with_payload_interface = search_query
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));

        let params = BatchSearchParams {
            vector_name: search_query.vector.get_name(),
            filter: search_query.filter.as_ref(),
            with_payload: WithPayload::from(with_payload_interface),
            with_vector: search_query.with_vector.clone().unwrap_or_default(),
            top: search_query.limit + search_query.offset,
            params: search_query.params.as_ref(),
        };

        // same params enables batching
        if params == prev_params {
            vectors_batch.push(search_query.vector.get_vector().as_slice());
        } else {
            // different params means different batches
            // execute what has been batched so far
            if !vectors_batch.is_empty() {
                let locked_segment = segment.get();
                let read_segment = locked_segment.read();
                let segment_points = read_segment.points_count();
                let top = if use_sampling {
                    let ef_limit = prev_params.params.and_then(|p| p.hnsw_ef).or_else(|| {
                        get_hnsw_ef_construct(read_segment.config(), prev_params.vector_name)
                    });
                    sampling_limit(prev_params.top, ef_limit, segment_points, total_points)
                } else {
                    prev_params.top
                };

                let mut res = read_segment.search_batch(
                    prev_params.vector_name,
                    &vectors_batch,
                    &prev_params.with_payload,
                    &prev_params.with_vector,
                    prev_params.filter,
                    top,
                    prev_params.params,
                )?;
                for batch_result in &res {
                    further_results.push(batch_result.len() == top);
                }
                result.append(&mut res);
                // clear current batch
                vectors_batch.clear();
            }
            // start new batch for current search query
            vectors_batch.push(search_query.vector.get_vector().as_slice());
            prev_params = params;
        }
    }

    // run last batch if any
    if !vectors_batch.is_empty() {
        let locked_segment = segment.get();
        let read_segment = locked_segment.read();
        let segment_points = read_segment.points_count();
        let top = if use_sampling {
            let ef_limit = prev_params
                .params
                .and_then(|p| p.hnsw_ef)
                .or_else(|| get_hnsw_ef_construct(read_segment.config(), prev_params.vector_name));
            sampling_limit(prev_params.top, ef_limit, segment_points, total_points)
        } else {
            prev_params.top
        };
        let mut res = read_segment.search_batch(
            prev_params.vector_name,
            &vectors_batch,
            &prev_params.with_payload,
            &prev_params.with_vector,
            prev_params.filter,
            top,
            prev_params.params,
        )?;
        for batch_result in &res {
            further_results.push(batch_result.len() == top);
        }
        result.append(&mut res);
    }

    Ok((result, further_results))
}

/// Find the maximum segment or vector specific HNSW ef_construct in this config
///
/// If the index is `Plain`, `None` is returned.
fn get_hnsw_ef_construct(config: SegmentConfig, vector_name: &str) -> Option<usize> {
    match config.index {
        Indexes::Plain {} => None,
        Indexes::Hnsw(hnsw_config) => Some(
            config
                .vector_data
                .get(vector_name)
                .and_then(|c| c.hnsw_config)
                .map(|c| c.ef_construct)
                .unwrap_or(hnsw_config.ef_construct),
        ),
    }
}

#[cfg(test)]
mod tests {
    use segment::fixtures::index_fixtures::random_vector;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{build_test_holder, random_segment};
    use crate::operations::types::SearchRequest;

    #[tokio::test]
    async fn test_segments_search() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment_holder = build_test_holder(dir.path());

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let req = SearchRequest {
            vector: query.into(),
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            limit: 5,
            score_threshold: None,
            offset: 0,
        };

        let batch_request = SearchRequestBatch {
            searches: vec![req],
        };

        let result = SegmentsSearcher::search(
            &segment_holder,
            Arc::new(batch_request),
            &Handle::current(),
            true,
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

        let segment_holder = RwLock::new(holder);

        let mut rnd = rand::thread_rng();

        for _ in 0..100 {
            let req1 = SearchRequest {
                vector: random_vector(&mut rnd, 4).into(),
                limit: 150, // more than LOWER_SEARCH_LIMIT_SAMPLING
                offset: 0,
                with_payload: None,
                with_vector: None,
                filter: None,
                params: None,
                score_threshold: None,
            };
            let req2 = SearchRequest {
                vector: random_vector(&mut rnd, 4).into(),
                limit: 50, // less than LOWER_SEARCH_LIMIT_SAMPLING
                offset: 0,
                filter: None,
                params: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            };

            let batch_request = SearchRequestBatch {
                searches: vec![req1, req2],
            };

            let result_no_sampling = SegmentsSearcher::search(
                &segment_holder,
                Arc::new(batch_request.clone()),
                &Handle::current(),
                false,
            )
            .await
            .unwrap();

            assert!(!result_no_sampling.is_empty());

            let result_sampling = SegmentsSearcher::search(
                &segment_holder,
                Arc::new(batch_request),
                &Handle::current(),
                true,
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

    #[tokio::test]
    async fn test_retrieve() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment_holder = build_test_holder(dir.path());

        let records = SegmentsSearcher::retrieve(
            &segment_holder,
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
        )
        .await
        .unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_sampling_limit() {
        assert_eq!(sampling_limit(1000, None, 464530, 35103551), 30);
    }

    #[test]
    fn test_sampling_limit_ef() {
        assert_eq!(sampling_limit(1000, Some(100), 464530, 35103551), 100);
    }

    #[test]
    fn test_sampling_limit_high() {
        assert_eq!(sampling_limit(1000000, None, 464530, 35103551), 1000000);
    }
}
