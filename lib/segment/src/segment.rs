use crate::id_mapper::id_mapper::IdMapper;
use crate::vector_storage::vector_storage::VectorStorage;
use std::cell::RefCell;
use std::rc::Rc;
use crate::payload_storage::payload_storage::{PayloadStorage, DeletedFlagStorage, TheMap};
use crate::entry::entry_point::{SegmentEntry, Result, OperationError};
use crate::types::{Filter, PayloadKeyType, PayloadType, SeqNumberType, VectorElementType, PointIdType, ScoreType, PointOffsetType};
use crate::query_planner::query_planner::QueryPlanner;

/// Simple segment implementation
pub struct Segment {
    pub version: SeqNumberType,
    pub id_mapper: Rc<RefCell<dyn IdMapper>>,
    pub vector_storage: Rc<RefCell<dyn VectorStorage>>,
    pub payload_storage: Rc<RefCell<dyn PayloadStorage>>,
    /// User for writing only here.
    pub delete_flag_storage: Rc<RefCell<dyn DeletedFlagStorage>>,
    pub query_planner: Rc<RefCell<dyn QueryPlanner>>,
}


impl Segment {
    fn update_vector(&mut self,
                     old_iternal_id: PointOffsetType,
                     vector: &Vec<VectorElementType>,
    ) -> PointOffsetType {
        let payload = self.payload_storage.borrow_mut().drop(old_iternal_id);
        self.delete_flag_storage.borrow_mut().mark_deleted(old_iternal_id);
        let new_internal_index = self.vector_storage.borrow_mut().put_vector(vector);
        match payload {
            Some(payload) => self.payload_storage
                .borrow_mut()
                .assign_all(new_internal_index, payload),
            None => ()
        }
        new_internal_index
    }

    fn check_version(&self, op_num: SeqNumberType) -> Result<bool> {
        return if self.version > op_num {
            Err(OperationError::SeqError { current_state: self.version, operation_num: op_num })
        } else {
            Ok(true)
        };
    }

    fn lookup_internal_id(&self, point_id: PointIdType) -> Result<PointOffsetType> {
        let internal_id_opt = self.id_mapper.borrow().internal_id(point_id);
        match internal_id_opt {
            Some(internal_id) => Ok(internal_id),
            None => Err(OperationError::PointIdError { missed_point_id: point_id })
        }
    }
}


impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType { self.version }

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize) -> Vec<(PointIdType, ScoreType)> {
        let internal_result = self.query_planner.borrow().search(vector, filter, top);


        let id_mapper = self.id_mapper.borrow();
        internal_result.iter()
            .map(|&(internal_id, score)|
                (
                    id_mapper
                        .external_id(internal_id)
                        .unwrap_or_else(|| panic!("Corrupter id_mapper, no external value for {}", internal_id)),
                    score
                )
            ).collect()
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>) -> Result<bool> {
        self.check_version(op_num)?;

        let vector_dim = self.vector_storage.borrow().vector_dim();
        if vector_dim != vector.len() {
            return Err(OperationError::WrongVector { expected_dim: vector_dim, received_dim: vector.len() });
        }

        let stored_internal_point = {
            let id_mapped = self.id_mapper.borrow();
            id_mapped.internal_id(point_id)
        };

        let (was_replaced, new_index) = match stored_internal_point {
            Some(existing_internal_id) =>
                (true, self.update_vector(existing_internal_id, vector)),
            None =>
                (false, self.vector_storage.borrow_mut().put_vector(vector))
        };

        self.id_mapper.borrow_mut().set_link(point_id, new_index);
        self.version = op_num;
        Ok(was_replaced)
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        self.check_version(op_num)?;
        match self.id_mapper.borrow().internal_id(point_id) {
            Some(internal_id) => {
                self.delete_flag_storage.borrow_mut().mark_deleted(internal_id);
                self.id_mapper.borrow_mut().drop(point_id);

                Ok(true)
            }
            None => Ok(false)
        }
    }

    fn set_payload(&mut self,
                   op_num: SeqNumberType,
                   point_id: PointIdType,
                   key: &PayloadKeyType,
                   payload: PayloadType,
    ) -> Result<bool> {
        self.check_version(op_num)?;
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().assign(internal_id, key, payload);
        Ok(true)
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType) -> Result<bool> {
        self.check_version(op_num)?;
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().delete(internal_id, key);
        Ok(true)
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        self.check_version(op_num)?;
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().drop(internal_id);
        Ok(true)
    }

    fn wipe_payload(&mut self, op_num: SeqNumberType) -> Result<bool> {
        self.check_version(op_num)?;
        self.payload_storage.borrow_mut().wipe();
        Ok(true)
    }

    fn vector(&self, point_id: PointIdType) -> Result<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.vector_storage.borrow().get_vector(internal_id).unwrap())
    }

    fn payload(&self, point_id: PointIdType) -> Result<TheMap<PayloadKeyType, PayloadType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.payload_storage.borrow().payload(internal_id))
    }
}