use crate::id_mapper::id_mapper::IdMapper;
use crate::vector_storage::vector_storage::VectorStorage;
use crate::payload_storage::payload_storage::{PayloadStorage};
use crate::entry::entry_point::{SegmentEntry, OperationResult, OperationError};
use crate::types::{Filter, PayloadKeyType, PayloadType, SeqNumberType, VectorElementType, PointIdType, PointOffsetType, SearchParams, ScoredPoint, TheMap, SegmentInfo, SegmentType, SegmentConfig, SegmentState};
use crate::query_planner::query_planner::QueryPlanner;
use std::sync::{Arc, Mutex};
use atomic_refcell::{AtomicRefCell};
use std::cmp;
use std::path::PathBuf;
use std::fs::{remove_dir_all};
use std::io::Write;
use atomicwrites::{AtomicFile, AllowOverwrite};
use crate::index::index::PayloadIndex;


pub const SEGMENT_STATE_FILE: &str = "segment.json";

/// Simple segment implementation
pub struct Segment {
    pub version: SeqNumberType,
    pub persisted_version: Arc<Mutex<SeqNumberType>>,
    pub current_path: PathBuf,
    pub id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
    pub vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    pub payload_storage: Arc<AtomicRefCell<dyn PayloadStorage>>,
    pub payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    /// User for writing only here.
    pub query_planner: Arc<AtomicRefCell<dyn QueryPlanner>>,
    pub appendable_flag: bool,
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
}


impl Segment {
    /// Update current segment with all (not deleted) vectors and payload form `other` segment
    /// Perform index building at the end of update
    pub fn update_from(&mut self, other: &Segment) -> OperationResult<()> {
        self.version = cmp::max(self.version, other.version());

        let other_id_mapper = other.id_mapper.borrow();
        let other_vector_storage = other.vector_storage.borrow();
        let other_payload_storage = other.payload_storage.borrow();

        let new_internal_range = self.vector_storage.borrow_mut().update_from(&*other_vector_storage)?;

        let mut id_mapper = self.id_mapper.borrow_mut();
        let mut payload_storage = self.payload_storage.borrow_mut();

        for (new_internal_id, old_internal_id) in new_internal_range.zip(other.vector_storage.borrow().iter_ids()) {
            let other_external_id = other_id_mapper.external_id(old_internal_id).unwrap();
            id_mapper.set_link(other_external_id, new_internal_id)?;
            payload_storage.assign_all(new_internal_id, other_payload_storage.payload(old_internal_id))?;
        }
        Ok(())
    }


    /// Launch index rebuilding on a whole segment data
    pub fn finish_building(&mut self) -> OperationResult<()> {
        self.query_planner.borrow_mut().build_index()?;
        Ok(())
    }


    fn update_vector(&mut self,
                     old_internal_id: PointOffsetType,
                     vector: &Vec<VectorElementType>,
    ) -> OperationResult<PointOffsetType> {
        let new_internal_index = {
            let mut vector_storage = self.vector_storage.borrow_mut();
            vector_storage.update_vector(old_internal_id, vector)
        }?;
        if new_internal_index != old_internal_id {
            let payload = self.payload_storage.borrow_mut().drop(old_internal_id)?;
            match payload {
                Some(payload) => self.payload_storage
                    .borrow_mut()
                    .assign_all(new_internal_index, payload)?,
                None => ()
            }
        }

        Ok(new_internal_index)
    }

    fn skip_by_version(&mut self, op_num: SeqNumberType) -> bool {
        return if self.version > op_num {
            true
        } else {
            self.version = op_num;
            false
        };
    }

    fn lookup_internal_id(&self, point_id: PointIdType) -> OperationResult<PointOffsetType> {
        let internal_id_opt = self.id_mapper.borrow().internal_id(point_id);
        match internal_id_opt {
            Some(internal_id) => Ok(internal_id),
            None => Err(OperationError::PointIdError { missed_point_id: point_id })
        }
    }

    fn get_state(&self) -> SegmentState {
        SegmentState {
            version: self.version,
            config: self.segment_config.clone(),
        }
    }

    fn save_state(&self, state: &SegmentState) -> OperationResult<()> {
        let state_path = self.current_path.join(SEGMENT_STATE_FILE);
        let af = AtomicFile::new(state_path, AllowOverwrite);
        let state_bytes = serde_json::to_vec(state).unwrap();
        af.write(|f| {
            f.write_all(&state_bytes)
        })?;
        Ok(())
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        self.save_state(&self.get_state())
    }
}


impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType { self.version }

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let expected_vector_dim = self.vector_storage.borrow().vector_dim();
        if expected_vector_dim != vector.len() {
            return Err(OperationError::WrongVector {
                expected_dim: expected_vector_dim,
                received_dim: vector.len(),
            });
        }

        let internal_result = self.query_planner.borrow().search(vector, filter, top, params);


        let id_mapper = self.id_mapper.borrow();
        let res = internal_result.iter()
            .map(|&scored_point_offset|
                (
                    ScoredPoint {
                        id: id_mapper
                            .external_id(scored_point_offset.idx)
                            .unwrap_or_else(|| panic!("Corrupter id_mapper, no external value for {}", scored_point_offset.idx)),
                        score: scored_point_offset.score,
                    }
                )
            ).collect();
        return Ok(res);
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); }

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
                (true, self.update_vector(existing_internal_id, vector)?),
            None =>
                (false, self.vector_storage.borrow_mut().put_vector(vector)?)
        };

        self.id_mapper.borrow_mut().set_link(point_id, new_index)?;
        Ok(was_replaced)
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let mut mapper = self.id_mapper.borrow_mut();
        let internal_id = mapper.internal_id(point_id);
        match internal_id {
            Some(internal_id) => {
                self.vector_storage.borrow_mut().delete(internal_id)?;
                mapper.drop(point_id)?;
                Ok(true)
            }
            None => Ok(false)
        }
    }

    fn set_full_payload(&mut self,
                        op_num: SeqNumberType,
                        point_id: PointIdType,
                        full_payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().assign_all(internal_id, full_payload)?;
        Ok(true)
    }

    fn set_payload(&mut self,
                   op_num: SeqNumberType,
                   point_id: PointIdType,
                   key: &PayloadKeyType,
                   payload: PayloadType,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().assign(internal_id, key, payload)?;
        Ok(true)
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().delete(internal_id, key)?;
        Ok(true)
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().drop(internal_id)?;
        Ok(true)
    }

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.vector_storage.borrow().get_vector(internal_id).unwrap())
    }

    fn payload(&self, point_id: PointIdType) -> OperationResult<TheMap<PayloadKeyType, PayloadType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.payload_storage.borrow().payload(internal_id))
    }

    fn iter_points(&self) -> Box<dyn Iterator<Item=PointIdType> + '_> {
        // Sorry for that, but I didn't find any way easier.
        // If you try simply return iterator - it won't work because AtomicRef should exist
        // If you try to make callback instead - you won't be able to create <dyn SegmentEntry>
        // Attempt to create return borrowed value along with iterator failed because of insane lifetimes
        unsafe { self.id_mapper.as_ptr().as_ref().unwrap().iter_external() }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_mapper.borrow().internal_id(point_id).is_some()
    }

    fn vectors_count(&self) -> usize {
        self.vector_storage.borrow().vector_count()
    }

    fn info(&self) -> SegmentInfo {
        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.vectors_count(),
            num_deleted_vectors: self.vector_storage.borrow().deleted_count(),
            ram_usage_bytes: 0, // ToDo: Implement
            disk_usage_bytes: 0,  // ToDo: Implement
            is_appendable: self.appendable_flag,
        }
    }

    fn config(&self) -> SegmentConfig {
        self.segment_config.clone()
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }

    fn flush(&self) -> OperationResult<SeqNumberType> {
        let persisted_version = self.persisted_version.lock().unwrap();
        if *persisted_version == self.version {
            return Ok(*persisted_version);
        }

        let state = self.get_state();

        self.id_mapper.borrow().flush()?;
        self.payload_storage.borrow().flush()?;
        self.vector_storage.borrow().flush()?;

        self.save_state(&state)?;

        Ok(state.version)
    }

    fn drop_data(&mut self) -> OperationResult<()> {
        Ok(remove_dir_all(&self.current_path)?)
    }

    fn delete_field_index(&mut self, op_num: u64, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        self.payload_index.borrow_mut().drop_index(key)?;
        Ok(true)
    }

    fn create_field_index(&mut self, op_num: u64, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        self.payload_index.borrow_mut().set_indexed(key)?;
        Ok(true)
    }
}