use crate::operations::types::CollectionResult;
use crate::operations::types::{Record, SearchRequest};
use crate::segment_manager::holders::segment_holder::{LockedSegment, LockedSegmentHolder};
use crate::segment_manager::segment_managers::SegmentSearcher;
use futures::future::try_join_all;
use segment::spaces::tools::peek_top_scores_iterable;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Handle;

/// Simple implementation of segment manager
///  - owens segments
///  - rebuild segment for memory optimization purposes
///  - Holds information regarding id mapping to segments
///
pub struct SimpleSegmentSearcher {
    pub segments: LockedSegmentHolder,
    pub runtime_handle: Handle,
}

impl SimpleSegmentSearcher {
    pub fn new(segments: LockedSegmentHolder, runtime_handle: Handle) -> Self {
        SimpleSegmentSearcher {
            segments,
            runtime_handle,
        }
    }

    pub async fn search_in_segment(
        segment: LockedSegment,
        request: Arc<SearchRequest>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let res = segment.get().read().search(
            &request.vector,
            request.filter.as_ref(),
            request.top,
            request.params.as_ref(),
        )?;

        Ok(res)
    }
}

#[async_trait::async_trait]
impl SegmentSearcher for SimpleSegmentSearcher {
    async fn search(&self, request: Arc<SearchRequest>) -> CollectionResult<Vec<ScoredPoint>> {
        // Using { } block to ensure segments variable is dropped in the end of it
        // and is not transferred across the all_searches.await? boundary as it
        // does not impl Send trait
        let searches: Vec<_> = {
            let segments = self.segments.read();

            let some_segment = segments.iter().next();

            if some_segment.is_none() {
                return Ok(vec![]);
            }

            segments
                .iter()
                .map(|(_id, segment)| {
                    SimpleSegmentSearcher::search_in_segment(segment.clone(), request.clone())
                })
                .map(|f| self.runtime_handle.spawn(f))
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
                .map(|x| x.unwrap())
                .flatten()
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
        points: &[PointIdType],
        with_payload: bool,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        let mut point_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut point_records: HashMap<PointIdType, Record> = Default::default();

        self.segments.read().read_points(points, |id, segment| {
            // If this point was not found yet or this segment have later version
            if !point_version.contains_key(&id) || point_version[&id] < segment.version() {
                point_records.insert(
                    id,
                    Record {
                        id,
                        payload: if with_payload {
                            Some(segment.payload(id)?)
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
                point_version.insert(id, segment.version());
            }
            Ok(true)
        })?;
        Ok(point_records.into_iter().map(|(_, r)| r).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::build_test_holder;
    use parking_lot::RwLock;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_segments_search() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment_holder = build_test_holder(dir.path());

        let searcher =
            SimpleSegmentSearcher::new(Arc::new(RwLock::new(segment_holder)), Handle::current());

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let req = Arc::new(SearchRequest {
            vector: query,
            filter: None,
            params: None,
            top: 5,
        });

        let result = searcher.search(req).await.unwrap();

        // eprintln!("result = {:?}", &result);

        assert_eq!(result.len(), 5);

        assert!(result[0].id == 3 || result[0].id == 11);
        assert!(result[1].id == 3 || result[1].id == 11);
    }

    #[tokio::test]
    async fn test_retrieve() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment_holder = build_test_holder(dir.path());

        let searcher =
            SimpleSegmentSearcher::new(Arc::new(RwLock::new(segment_holder)), Handle::current());

        let records = searcher.retrieve(&[1, 2, 3], true, true).await.unwrap();
        assert_eq!(records.len(), 3);
    }
}
