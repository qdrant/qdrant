use crate::id_mapper::id_mapper::IdMapper;
use crate::vector_storage::vector_storage::VectorStorage;
use std::cell::RefCell;
use std::rc::Rc;
use crate::payload_storage::payload_storage::{PayloadStorage, DeletedFlagStorage};
use crate::entry::entry_point::{SegmentEntry, Result};
use crate::types::{Filter, PayloadKeyType, PayloadType, SeqNumberType, VectorElementType, PointIdType, ScoreType};
use std::ops::Deref;
use crate::query_planner::query_planner::QueryPlanner;

/// Simple segment implementation
pub struct SimpleSegment {
    version: SeqNumberType,
    pub id_mapper: Rc<RefCell<dyn IdMapper>>,
    pub vector_storage: Rc<RefCell<dyn VectorStorage>>,
    pub payload_storage: Rc<RefCell<dyn PayloadStorage>>,
    /// User for writing only here.
    pub delete_flag_storage: Rc<RefCell<dyn DeletedFlagStorage>>,
    pub query_planner: Rc<RefCell<dyn QueryPlanner>>,
}


impl SegmentEntry for SimpleSegment {
    fn version(&self) -> SeqNumberType { self.version }

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize) -> Vec<(PointIdType, ScoreType)> {
        let internal_result = {
            let index = self.query_planner.borrow();
            index.search(vector, filter, top)
        };

        let id_mapper = self.id_mapper.borrow();
        internal_result.iter()
            .map(|&(internal_id, score)|
                (
                    id_mapper
                        .external_id(internal_id)
                        .unwrap_or_else(format!("Corrupter id_mapper, no external value for {}", internal_id)),
                    score
                )
            ).collect()
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>) -> Result<bool> {}

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        unimplemented!()
    }

    fn set_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: PayloadKeyType, payload: PayloadType) -> Result<bool> {
        unimplemented!()
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: PayloadKeyType) -> Result<bool> {
        unimplemented!()
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        unimplemented!()
    }

    fn wipe_payload(&mut self, op_num: SeqNumberType) -> Result<bool> {
        unimplemented!()
    }
}