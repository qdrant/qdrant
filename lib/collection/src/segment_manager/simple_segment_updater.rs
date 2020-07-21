use std::sync::{Arc, RwLock};
use crate::segment_manager::segment_holder::{SegmentHolder, LockedSegment};
use crate::segment_manager::segment_managers::SegmentUpdater;
use crate::operations::CollectionUpdateOperations;
use crate::collection::{OperationResult, UpdateError};
use segment::types::{SeqNumberType, PointIdType};
use segment::entry::entry_point::OperationError;
use std::collections::{HashSet, HashMap};
use crate::operations::types::VectorType;
use rand::Rng;
use crate::operations::point_ops::PointOps;
use crate::operations::payload_ops::PayloadOps;

struct SimpleSegmentUpdater {
    segments: Arc<RwLock<SegmentHolder>>,
}


impl SimpleSegmentUpdater {
    /// Selects point ids, which is stored in this segment
    fn segment_points(&self, ids: &Vec<PointIdType>, segment: &LockedSegment) -> Vec<PointIdType> {
        let read_segment = segment.read().unwrap();
        ids
            .iter()
            .cloned()
            .filter(|id| read_segment.has_point(*id))
            .collect()
    }


    /// Tries to delete points from all segments, returns number of actually deleted points
    fn delete_points(&self, op_num: SeqNumberType, ids: &Vec<PointIdType>) -> OperationResult<usize> {
        let mut res: usize = 0;
        for (idx, segment) in self.segments.read().unwrap().iter() {
            /// Skip this segment if it already have bigger version (WAL recovery related)
            if segment.read().unwrap().version() > op_num { continue; }

            /// Collect deletable points first, we want to lock segment for writing as rare as possible
            let segment_points = self.segment_points(ids, segment);

            let mut write_segment = segment.write().unwrap();
            for point_id in segment_points {
                match write_segment.delete_point(op_num, point_id) {
                    Ok(is_deleted) => res += (is_deleted as usize),
                    Err(err) => match err {
                        /// It is ok, if we are recovering from WAL and some changes happened somehow
                        /// Which might be possible with parallel updates
                        OperationError::SeqError { .. } => {}
                        /// No other errors could be handled here
                        _ => panic!(format!("Unexpected error {}", err)),  //noinspection all
                    },
                }
            }
        }
        Ok(res)
    }

    /// Checks point id in each segment, update point if found.
    /// All not found points are inserted into random segment.
    /// Returns: number of updated points.
    fn upsert_points(&self, op_num: SeqNumberType, ids: &Vec<PointIdType>, vectors: &Vec<VectorType>) -> OperationResult<usize> {
        let mut res: usize = 0;
        let mut updated_points: HashSet<PointIdType> = Default::default();
        let points_map: HashMap<PointIdType, &VectorType> = ids.iter().cloned().zip(vectors).collect();

        let segments = self.segments.read().unwrap();

        for (_segment_id, segment) in segments.iter() {
            let segment_points = self.segment_points(ids, segment);
            let mut write_segment = segment.write().unwrap();
            for id in segment_points {
                updated_points.insert(id);
                match write_segment.upsert_point(op_num, id, points_map[&id]) {
                    Ok(point_res) => res += (point_res as usize),
                    Err(err) => match err {
                        OperationError::WrongVector { expected_dim, received_dim } =>
                            return Err(UpdateError::BadInput { description: format!("{}", err) }), //noinspection all
                        OperationError::SeqError { .. } => {} /// Ok if recovering from WAL
                        OperationError::PointIdError { .. } => panic!(format!("Unexpected error {}", err)), //noinspection all
                    },
                }
            }
        }

        let new_point_ids = ids
            .iter()
            .cloned()
            .filter(|x| !updated_points.contains(x));

        let write_segment = segments.random_segment();
        return match write_segment {
            None => Err(UpdateError::ServiceError { error: "No segments exists, expected at least one".to_string() }),
            Some(segment) => {
                let mut write_segment = segment.write().unwrap();
                for point_id in new_point_ids {
                    write_segment.upsert_point(op_num, point_id, points_map[&point_id]);
                }
                Ok(res)
            }
        };
    }


    pub fn process_point_operation(&self, op_num: SeqNumberType, point_operation: &PointOps) -> OperationResult<usize> {
        match point_operation {
            PointOps::UpsertPoints {
                ids,
                vectors,
                ..
            } => self.upsert_points(op_num, ids, vectors),
            PointOps::DeletePoints { ids, .. } => self.delete_points(op_num, ids),
        }
    }

    pub fn process_payload_operation(&self, op_num: SeqNumberType, payload_operation: &PayloadOps) -> OperationResult<usize> {
        unimplemented!()
    }
}


impl SegmentUpdater for SimpleSegmentUpdater {
    fn update(&self, op_num: SeqNumberType, operation: &CollectionUpdateOperations) -> OperationResult<usize> {
        match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => self.process_point_operation(op_num, point_operation),
            CollectionUpdateOperations::PayloadOperation(payload_operation) => self.process_payload_operation(op_num, payload_operation),
        }
    }
}