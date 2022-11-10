use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use itertools::Itertools;
use ordered_float::Float;
use parking_lot::RwLock;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorElementType;
use segment::entry::entry_point::OperationError;
use segment::spaces::tools::peek_top_largest_iterable;
use segment::types::{
    Filter, PointIdType, ScoreType, ScoredPoint, SearchParams, SeqNumberType, WithPayload,
    WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::probabilistic_segment_search_sampling::find_search_sampling_over_point_distribution;
use crate::operations::types::{CollectionResult, Record, SearchRequestBatch};

/// Search `Limit` lower threshold above which we will use sampling.
const LOWER_SEARCH_LIMIT_SAMPLING: usize = 100;

/// Simple implementation of segment manager
///  - rebuild segment for memory optimization purposes
#[derive(Default)]
pub struct SegmentsSearcher {}

impl SegmentsSearcher {
    pub async fn search(
        segments: &RwLock<SegmentHolder>,
        request: Arc<SearchRequestBatch>,
        runtime_handle: &Handle,
        sampling_enabled: bool,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Using { } block to ensure segments variable is dropped in the end of it
        // and is not transferred across the all_searches.await? boundary as it
        // does not impl Send trait
        let searches: Vec<_> = {
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
            let mut total_points_segments = 0;
            let mut use_sampling = false;
            if sampling_enabled && segments.len() > 1 {
                for (_, segment) in segments.iter() {
                    total_points_segments += segment.get().read().points_count();
                }
                if total_points_segments > 0 {
                    use_sampling = true;
                }
            }

            segments
                .iter()
                .map(|(_id, segment)| {
                    let request = if use_sampling {
                        // division per 0 protected by `use_sampling` flag
                        let segment_probability =
                            segment.get().read().points_count() / total_points_segments;
                        let mut new_request = (*request).clone();
                        for search in new_request.searches.iter_mut() {
                            // Apply sampling only if the limit is above the threshold
                            if search.limit > LOWER_SEARCH_LIMIT_SAMPLING {
                                let sampling = find_search_sampling_over_point_distribution(
                                    search.limit as f64,
                                    segment_probability as f64,
                                );
                                // Make sure that sampling is not larger than the initial limit
                                match sampling {
                                    Some(sampling) if sampling < search.limit => {
                                        // set sampling as new limit
                                        search.limit = sampling as usize;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Arc::new(new_request)
                    } else {
                        // use original request if no sampling is needed
                        request.clone()
                    };
                    search_in_segment(segment.clone(), request)
                })
                .map(|f| runtime_handle.spawn(f))
                .collect()
        };

        // perform search on all segments concurrently
        // the resulting Vec is in the same order as the segment searches were provided.
        let all_searches = try_join_all(searches);
        let all_search_results_per_segment: Vec<CollectionResult<Vec<Vec<ScoredPoint>>>> =
            all_searches.await?;

        // shortcut on first segment error
        match all_search_results_per_segment
            .iter()
            .filter_map(|res| res.to_owned().err())
            .next()
        {
            None => {}
            Some(error) => return Err(error),
        }

        // The lowest scored element must be larger or equal to the worst scored element in each segment.
        // Otherwise, the sampling is invalid and some points might be missing.
        // e.g. with 3 segments with the following sampled ranges:
        // s1 - [0.91 -> 0.87]
        // s2 - [0.92 -> 0.86]
        // s3 - [0.93 -> 0.85]
        // If the top merged scores result range is [0.93 -> 0.86] then we do not know if s1 could have contributed more points at the lower part between [0.87 -> 0.86]
        // In that case, we need to re-run the search without sampling on that segment.

        // Therefore we need to track the lowest scored element per segment for each batch
        let mut lowest_scores_per_batch_request_per_segment: Vec<Vec<ScoreType>> = vec![
            vec![f32::max_value(); request.searches.len()]; // initial max score value for each batch
                all_search_results_per_segment.len() // number of segments
            ];

        // Batch results merged from all segments
        let mut merged_results_per_batch: Vec<Vec<ScoredPoint>> =
            vec![vec![]; request.searches.len()];
        for (segment_idx, segment_result) in all_search_results_per_segment.into_iter().enumerate()
        {
            let segment_result = segment_result?;
            // merge results for each batch search request across segments
            for (batch_req_idx, query_res) in segment_result.into_iter().enumerate() {
                // keep track of the lowest score per batch per segment to validate that the sampling is correct
                let current_lowest_score =
                    lowest_scores_per_batch_request_per_segment[segment_idx][batch_req_idx];
                // the last element is the lowest scored element
                if let Some(lowest_score_point) = query_res.last() {
                    if lowest_score_point.score < current_lowest_score {
                        lowest_scores_per_batch_request_per_segment[segment_idx][batch_req_idx] =
                            lowest_score_point.score;
                    }
                } else {
                    // if the batch has no results, then the lowest score for the batch is set to a large min value
                    lowest_scores_per_batch_request_per_segment[segment_idx][batch_req_idx] =
                        f32::min_value();
                }
                // merge results per batch
                merged_results_per_batch[batch_req_idx].extend(query_res);
            }
        }

        let top_scores = merged_results_per_batch
            .into_iter()
            .enumerate()
            .zip(request.searches.iter())
            .map(|((batch_req_idx, all_search_results_per_vector), req)| {
                let mut seen_idx: HashSet<PointIdType> = HashSet::new();
                let batch_result = peek_top_largest_iterable(
                    all_search_results_per_vector
                        .into_iter()
                        .sorted_by_key(|a| (a.id, 1 - a.version as i64)) // Prefer higher version first
                        .dedup_by(|a, b| a.id == b.id) // Keep only highest version
                        .filter(|scored| {
                            let res = seen_idx.contains(&scored.id);
                            seen_idx.insert(scored.id);
                            !res
                        }),
                    req.limit + req.offset,
                );
                for (segment_idx, segment_batch_result) in
                    lowest_scores_per_batch_request_per_segment
                        .iter()
                        .enumerate()
                {
                    // validate that no potential points are missing due to sampling
                    if let Some(lowest_score_for_batch) =
                        batch_result.last().map(|scored_point| scored_point.score)
                    {
                        let lowest_score_for_batch_on_segment = segment_batch_result[batch_req_idx];
                        if lowest_score_for_batch_on_segment > lowest_score_for_batch {
                            eprintln!(
                                "Sampling failed for segment {} and batch request {}: min segment {} vs min batch {}",
                                segment_idx,
                                batch_req_idx,
                                lowest_score_for_batch_on_segment,
                                lowest_score_for_batch
                            );
                            eprintln!("{:?}", segment_batch_result);
                            eprintln!("{:?}", batch_result);
                            // TODO add telemetry for failing sampling search query
                            todo!("re-run search without sampling on that segment for this batch");
                        }
                    }
                }
                batch_result
            })
            .collect();

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
                OperationError::service_error(&format!("No version for point {}", id))
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
        Ok(point_records.into_iter().map(|(_, r)| r).collect())
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

/// Process sequentially contiguous batches
async fn search_in_segment(
    segment: LockedSegment,
    request: Arc<SearchRequestBatch>,
) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
    let mut result: Vec<Vec<ScoredPoint>> = vec![];
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
                let mut res = segment.get().read().search_batch(
                    prev_params.vector_name,
                    &vectors_batch,
                    &prev_params.with_payload,
                    &prev_params.with_vector,
                    prev_params.filter,
                    prev_params.top,
                    prev_params.params,
                )?;
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
        let mut res = segment.get().read().search_batch(
            prev_params.vector_name,
            &vectors_batch,
            &prev_params.with_payload,
            &prev_params.with_vector,
            prev_params.filter,
            prev_params.top,
            prev_params.params,
        )?;
        result.append(&mut res);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;
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

        // contains 2 segments to test sampling
        let segment_holder = build_test_holder(dir.path());

        let req1 = SearchRequest {
            vector: vec![1.0, 1.0, 1.0, 1.0].into(),
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            limit: 5,
            score_threshold: None,
            offset: 0,
        };

        let req2 = SearchRequest {
            vector: vec![2.0, 1.0, 1.0, 2.0].into(),
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            limit: 2,
            score_threshold: None,
            offset: 0,
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
        assert_eq!(result_no_sampling, result_sampling);
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
}
