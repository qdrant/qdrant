use crate::segment_manager::holders::segment_holder::{SegmentHolder, LockedSegment};
use std::sync::{RwLock, Arc};
use crate::segment_manager::segment_managers::{SegmentSearcher};
use crate::collection::{OperationResult, CollectionError};
use segment::types::{ScoredPoint, Distance, PointIdType, SeqNumberType};
use tokio::runtime::Handle;
use std::collections::{HashSet, HashMap};
use segment::spaces::tools::peek_top_scores_iterable;
use futures::future::try_join_all;
use crate::operations::types::{Record, CollectionInfo, SearchRequest};

/// Simple implementation of segment manager
///  - owens segments
///  - rebuild segment for memory optimization purposes
///  - Holds information regarding id mapping to segments
///
pub struct SimpleSegmentSearcher {
    pub segments: Arc<RwLock<SegmentHolder>>,
    pub distance: Distance,
    pub runtime_handle: Handle,
}

impl SimpleSegmentSearcher {
    pub fn new(segments: Arc<RwLock<SegmentHolder>>, runtime_handle: Handle, distance: Distance) -> Self {
        return SimpleSegmentSearcher {
            segments,
            distance,
            runtime_handle,
        };
    }

    pub async fn search_in_segment(
        segment: LockedSegment,
        request: Arc<SearchRequest>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let segment = segment.0.read()
            .or(Err(CollectionError::ServiceError { error: "Unable to unlock segment".to_owned() }))?;

        let res = segment.search(
            &request.vector,
            request.filter.as_ref(),
            request.top,
            request.params.as_ref(),
        )?;

        Ok(res)
    }
}

impl SegmentSearcher for SimpleSegmentSearcher {
    fn info(&self) -> OperationResult<CollectionInfo> {
        unimplemented!()
    }

    fn search(
        &self,
        request: Arc<SearchRequest>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let segments = self.segments.read().unwrap();

        let searches: Vec<_> = segments
            .iter()
            .map(|(_id, segment)|
                SimpleSegmentSearcher::search_in_segment(LockedSegment(segment.0.clone()), request.clone())
            )
            .map(|f| self.runtime_handle.spawn(f))
            .collect();


        let all_searches = try_join_all(searches);
        let all_search_results = self.runtime_handle.block_on(all_searches)?;

        match all_search_results.iter()
            .filter_map(|res| res.to_owned().err())
            .next() {
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
                    let res = seen_idx.contains(&scored.idx);
                    seen_idx.insert(scored.idx);
                    !res
                }),
            request.top,
            &self.distance,
        );

        Ok(top_scores)
    }

    fn retrieve(&self, points: &Vec<PointIdType>, with_payload: bool, with_vector: bool) -> OperationResult<Vec<Record>> {
        let mut point_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut point_records: HashMap<PointIdType, Record> = Default::default();

        self.segments.read().unwrap().read_points(points, |id, segment| {
            // If this point was not found yet or this segment have later version
            if !point_version.contains_key(&id) || point_version[&id] < segment.version() {
                point_records.insert(id, Record {
                    id,
                    payload: if with_payload { Some(segment.payload(id)?) } else { None },
                    vector: if with_vector { Some(segment.vector(id)?) } else { None },
                });
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
    use tokio::runtime::Runtime;
    use tokio::runtime;
    use crate::segment_manager::fixtures::build_test_holder;

    #[test]
    fn test_segments_search() {
        let segment_holder = build_test_holder();

        let threaded_rt1: Runtime = runtime::Builder::new()
            .threaded_scheduler()
            .max_threads(2)
            .build().unwrap();


        let searcher = SimpleSegmentSearcher::new(
            Arc::new(RwLock::new(segment_holder)),
            threaded_rt1.handle().clone(),
            Distance::Dot,
        );

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let req = Arc::new(SearchRequest {
            vector: query,
            filter: None,
            params: None,
            top: 5,
        });

        let result = searcher.search(req).unwrap();

        // eprintln!("result = {:?}", &result);

        assert_eq!(result.len(), 5);

        assert!(result[0].idx == 3 || result[0].idx == 11);
        assert!(result[1].idx == 3 || result[1].idx == 11);
    }

    #[test]
    fn test_retrieve() {
        let segment_holder = build_test_holder();

        let threaded_rt1: Runtime = runtime::Builder::new()
            .threaded_scheduler()
            .max_threads(2)
            .build().unwrap();

        let searcher = SimpleSegmentSearcher::new(
            Arc::new(RwLock::new(segment_holder)),
            threaded_rt1.handle().clone(),
            Distance::Dot,
        );

        let records = searcher.retrieve(&vec![1, 2, 3], true, true).unwrap();

        assert_eq!(records.len(), 3);
    }
}