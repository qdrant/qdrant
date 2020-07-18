use std::sync::{RwLock, Arc};
use std::collections::{HashMap, HashSet};
use segment::entry::entry_point::SegmentEntry;
use segment::types::{SeqNumberType, PointIdType, Filter, SearchParams, VectorElementType, Distance, ScoredPoint};
use crate::segment_manager::segment_manager::SegmentManager;
use crate::collection::{OperationResult, CollectionInfo};
use crate::operations::index_def::Indexes;
use crate::operations::CollectionUpdateOperations;
use tokio::runtime::Handle;
use futures::future::try_join_all;
use segment::spaces::tools::peek_top_scores_iterable;
use segment::segment::Segment;

type SegmentId = usize;
type SegmentAliasId = usize;

type LockedSegment = Arc<RwLock<dyn SegmentEntry>>;


/// Simple implementation of segment manager
///  - owens segments
///  - rebuild segment for memory optimization purposes
///  - Holds information regarding id mapping to segments
struct SimpleSegmentManager {
    segments: RwLock<HashMap<SegmentId, LockedSegment>>,
    max_segments: usize,
    version: SeqNumberType,
    index_params: Indexes,
    distance: Distance,
    runtime_handle: Handle
}

impl SimpleSegmentManager {
    pub fn new(segments: Vec<Segment>, handle: Handle) -> Self {

        let mut stored_segment: HashMap<SegmentId, LockedSegment> = HashMap::new();
        for (idx, segment) in segments.into_iter().enumerate() {
            stored_segment.insert(idx, Arc::new(RwLock::new(segment)));
        }

        return SimpleSegmentManager {
            segments: RwLock::new(stored_segment),
            max_segments: 1000,
            version: 0,
            index_params: Indexes::Plain {},
            distance: Distance::Dot,
            runtime_handle: handle
        }

    }

    pub async fn search_in_segment(
        segment: LockedSegment,
        vector: &Vec<VectorElementType>,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,

    ) -> Result<Vec<ScoredPoint>, String> {
        segment.read()
            .or(Err("Unable to unlock segment".to_owned()))
            .and_then(|s| Ok(s.search(vector, filter, top, params)))
    }
}

impl SegmentManager for SimpleSegmentManager {
    fn update(&self, op_num: SeqNumberType, operation: CollectionUpdateOperations) -> OperationResult<bool> {
        unimplemented!()
    }

    fn info(&self) -> OperationResult<CollectionInfo> {
        unimplemented!()
    }

    fn search(
        &self,
        vector: &Vec<VectorElementType>,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<ScoredPoint> {

        let searches: Vec<_> = self.segments
            .read()
            .unwrap()
            .iter()
            .map(|(_id, segment)|
                SimpleSegmentManager::search_in_segment(segment.clone(), vector, filter, top, params))
            .collect();

        let all_searches = try_join_all(searches);
        let all_search_results: Vec<Vec<ScoredPoint>> = self.runtime_handle.block_on(all_searches).unwrap();
        let mut seen_idx: HashSet<PointIdType> = HashSet::new();

        peek_top_scores_iterable(
            all_search_results
                .iter()
                .flatten()
                .filter(|scored| {
                    let res = seen_idx.contains(&scored.idx);
                    seen_idx.insert(scored.idx);
                    !res
                })
                .cloned(),
            top,
            &self.distance
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use tokio::runtime::Runtime;
    use tokio::runtime;

    #[test]
    fn test_segments_search() {
                let tmp_path = Path::new("/tmp/qdrant/segment");
        let mut segment1 = build_simple_segment(tmp_path, 4, Distance::Dot);
        let mut segment2 = build_simple_segment(tmp_path, 4, Distance::Dot);


        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];


        let vec11 = vec![1.0, 1.0, 1.0, 1.0];
        let vec12 = vec![1.0, 1.0, 1.0, 0.0];
        let vec13 = vec![1.0, 0.0, 1.0, 1.0];
        let vec14 = vec![1.0, 0.0, 0.0, 1.0];
        let vec15 = vec![1.0, 1.0, 0.0, 0.0];


        segment1.upsert_point(1, 1, &vec1);
        segment1.upsert_point(2, 2, &vec2);
        segment1.upsert_point(3, 3, &vec3);
        segment1.upsert_point(4, 4, &vec4);
        segment1.upsert_point(5, 5, &vec5);

        /// Intentional point duplication
        segment2.upsert_point(7, 4, &vec4);
        segment2.upsert_point(8, 5, &vec5);

        segment2.upsert_point(11, 11, &vec11);
        segment2.upsert_point(12, 12, &vec12);
        segment2.upsert_point(13, 13, &vec13);
        segment2.upsert_point(14, 14, &vec14);
        segment2.upsert_point(15, 15, &vec15);

        let threaded_rt1: Runtime = runtime::Builder::new()
            .threaded_scheduler()
            .max_threads(2)
            .build().unwrap();


        let manager = SimpleSegmentManager::new(
            vec![segment1, segment2],
            threaded_rt1.handle().clone()
        );

        let query = vec![1.0, 1.0, 1.0, 1.0];

        let result = manager.search(
            &query,
            None,
            5,
            None
        );

        eprintln!("result = {:?}", &result);

        assert_eq!(result.len(), 5);

        assert!(result[0].idx == 3 || result[0].idx == 11);
        assert!(result[1].idx == 3 || result[1].idx == 11);
    }
}