use std::sync::RwLock;
use std::collections::HashMap;
use segment::entry::entry_point::SegmentEntry;
use segment::types::{SeqNumberType, PointIdType, Filter, ScoreType, SearchParams, VectorElementType, Distance};
use crate::storage::collection::segment_manager::segment_manager::SegmentManager;
use crate::operations::CollectionUpdateOperations;
use crate::storage::collection::collection::{OperationResult, CollectionInfo};
use crate::common::index_def::Indexes;

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
    point_alias_mapping: RwLock<HashMap<PointIdType, SegmentAliasId>>,
    alias_segment_mapping: RwLock<HashMap<SegmentAliasId, SegmentId>>,
    index_params: Indexes,
    distance: Distance,
}

impl SimpleSegmentManager {
    fn segment_id(&self, point_id: PointIdType) -> Option<SegmentId> {
        self.point_alias_mapping.read().unwrap()
            .get(&point_id)
            .and_then(|alias| self.alias_segment_mapping
                .read()
                .unwrap()
                .get(alias)
                .cloned()
            )
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
    ) -> Vec<(PointIdType, ScoreType)> {
        unimplemented!()
    }
}