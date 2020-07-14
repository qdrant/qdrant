use std::sync::RwLock;
use std::collections::HashMap;
use segment::entry::entry_point::SegmentEntry;
use segment::types::{SeqNumberType, PointIdType, Filter, ScoreType, SearchParams, VectorElementType, Distance};
use crate::segment_manager::segment_manager::SegmentManager;
use crate::collection::{OperationResult, CollectionInfo};
use crate::operations::index_def::Indexes;
use crate::operations::CollectionUpdateOperations;

type SegmentId = u64;
type SegmentAliasId = u64;

type LockedSegment = RwLock<Box<dyn SegmentEntry>>;


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
}

impl SimpleSegmentManager {

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
    ) -> Vec<(PointIdType, ScoreType)> {
        unimplemented!()
    }
}
