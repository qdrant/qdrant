use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorElementType;
use segment::entry::entry_point::OperationError;
use segment::spaces::tools::peek_top_largest_iterable;
use segment::types::{
    Filter, PointIdType, ScoredPoint, SearchParams, SeqNumberType, WithPayload,
    WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::operations::types::{CollectionResult, Record, SearchRequestBatch};

/// Simple implementation of segment manager
///  - rebuild segment for memory optimization purposes
#[derive(Default)]
pub struct SegmentsSearcher {}

impl SegmentsSearcher {
    pub async fn search(
        segments: &RwLock<SegmentHolder>,
        request: Arc<SearchRequestBatch>,
        runtime_handle: &Handle,
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

            segments
                .iter()
                .map(|(_id, segment)| search_in_segment(segment.clone(), request.clone()))
                .map(|f| runtime_handle.spawn(f))
                .collect()
        };

        // perform search on all segments concurrently
        let all_searches = try_join_all(searches);
        let all_search_results: Vec<CollectionResult<Vec<Vec<ScoredPoint>>>> = all_searches.await?;

        match all_search_results
            .iter()
            .filter_map(|res| res.to_owned().err())
            .next()
        {
            None => {}
            Some(error) => return Err(error),
        }

        let mut merged_results: Vec<Vec<ScoredPoint>> = vec![vec![]; request.searches.len()];
        for segment_result in all_search_results {
            let segment_result = segment_result.unwrap();
            for (idx, query_res) in segment_result.into_iter().enumerate() {
                merged_results[idx].extend(query_res);
            }
        }

        let top_scores = merged_results
            .into_iter()
            .zip(request.searches.iter())
            .map(|(all_search_results_per_vector, req)| {
                let mut seen_idx: HashSet<PointIdType> = HashSet::new();
                peek_top_largest_iterable(
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
                )
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
            with_vector: search_query.with_vector.clone(),
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
            with_vector: false.into(),
            filter: None,
            params: None,
            limit: 5,
            score_threshold: None,
            offset: 0,
        };

        let batch_request = SearchRequestBatch {
            searches: vec![req],
        };

        let result =
            SegmentsSearcher::search(&segment_holder, Arc::new(batch_request), &Handle::current())
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
