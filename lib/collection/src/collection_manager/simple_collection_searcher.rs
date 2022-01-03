use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::entry::entry_point::OperationError;
use tokio::runtime::Handle;

use segment::spaces::tools::peek_top_scores_iterable;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType, WithPayload, WithPayloadInterface};

use crate::collection_manager::collection_managers::CollectionSearcher;
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::operations::types::CollectionResult;
use crate::operations::types::{Record, SearchRequest};

/// Simple implementation of segment manager
///  - rebuild segment for memory optimization purposes
#[derive(Default)]
pub struct SimpleCollectionSearcher {}

impl SimpleCollectionSearcher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl CollectionSearcher for SimpleCollectionSearcher {
    async fn search(
        &self,
        segments: &RwLock<SegmentHolder>,
        request: Arc<SearchRequest>,
        runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
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

        let all_searches = try_join_all(searches);
        let all_search_results = all_searches.await?;

        match all_search_results
            .iter()
            .filter_map(|res| res.to_owned().err())
            .next()
        {
            None => {}
            Some(error) => return Err(error),
        }

        let mut seen_idx: HashSet<PointIdType> = HashSet::new();

        let top_scores = peek_top_scores_iterable(
            all_search_results
                .into_iter()
                .flat_map(Result::unwrap) // already checked for errors
                .sorted_by_key(|a| (a.id, 1 - a.version as i64)) // Prefer higher version first
                .dedup_by(|a, b| a.id == b.id) // Keep only highest version
                .filter(|scored| {
                    let res = seen_idx.contains(&scored.id);
                    seen_idx.insert(scored.id);
                    !res
                }),
            request.top,
        );

        Ok(top_scores)
    }

    async fn retrieve(
        &self,
        segments: &RwLock<SegmentHolder>,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        let mut point_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut point_records: HashMap<PointIdType, Record> = Default::default();

        segments.read().read_points(points, |id, segment| {
            let version = segment
                .point_version(id)
                .ok_or(OperationError::ServiceError {
                    description: format!("No version for point {}", id),
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
                        vector: if with_vector {
                            Some(segment.vector(id)?)
                        } else {
                            None
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

async fn search_in_segment(
    segment: LockedSegment,
    request: Arc<SearchRequest>,
) -> CollectionResult<Vec<ScoredPoint>> {
    let with_payload_interface = request
        .with_payload
        .as_ref()
        .unwrap_or(&WithPayloadInterface::Bool(false));
    let with_payload = WithPayload::from(with_payload_interface);

    let with_vector = request.with_vector.unwrap_or(false);

    let res = segment.get().read().search(
        &request.vector,
        &with_payload,
        with_vector,
        request.filter.as_ref(),
        request.top,
        request.params.as_ref(),
    )?;

    Ok(res)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::collection_manager::fixtures::build_test_holder;

    use super::*;

    #[tokio::test]
    async fn test_segments_search() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment_holder = build_test_holder(dir.path());

        let searcher = SimpleCollectionSearcher::new();

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let req = Arc::new(SearchRequest {
            vector: query,
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            top: 5,
        });

        let result = searcher
            .search(&segment_holder, req, &Handle::current())
            .await
            .unwrap();

        // eprintln!("result = {:?}", &result);

        assert_eq!(result.len(), 5);

        assert!(result[0].id == 3 || result[0].id == 11);
        assert!(result[1].id == 3 || result[1].id == 11);
    }

    #[tokio::test]
    async fn test_retrieve() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment_holder = build_test_holder(dir.path());

        let searcher = SimpleCollectionSearcher::new();

        let records = searcher
            .retrieve(&segment_holder, &[1, 2, 3], &WithPayload::from(true), true)
            .await
            .unwrap();
        assert_eq!(records.len(), 3);
    }
}
