use crate::entry::entry_point::{OperationError, OperationResult, SegmentEntry, is_service_error};
use crate::id_mapper::IdMapper;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::{ConditionChecker, PayloadStorage};
use crate::spaces::tools::mertic_object;
use crate::types::{
    Filter, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaInfo, PayloadType, PointIdType,
    PointOffsetType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo, SegmentState,
    SegmentType, SeqNumberType, TheMap, VectorElementType, WithPayload,
};
use crate::vector_storage::VectorStorage;
use atomic_refcell::AtomicRefCell;
use atomicwrites::{AllowOverwrite, AtomicFile};
use std::fs::{remove_dir_all, rename};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    pub condition_checker: Arc<dyn ConditionChecker>,
    pub vector_index: Arc<AtomicRefCell<dyn VectorIndex>>,
    pub appendable_flag: bool,
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
    pub error_status: Option<(SeqNumberType, OperationError)>
}

impl Segment {
    fn update_vector(
        &mut self,
        old_internal_id: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<PointOffsetType> {
        let new_internal_index = {
            let mut vector_storage = self.vector_storage.borrow_mut();
            vector_storage.update_vector(old_internal_id, vector)
        }?;
        if new_internal_index != old_internal_id {
            // If vector was moved to a new internal id, move payload to this internal id as well
            let mut payload_storage = self.payload_storage.borrow_mut();
            let payload = payload_storage.drop(old_internal_id)?;
            if let Some(payload) = payload {
                payload_storage.assign_all(new_internal_index, payload)?
            }
        }

        Ok(new_internal_index)
    }

    /// Manage segment version checking
    /// If current version if higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    fn handle_version<F>(
        &mut self,
        op_num: SeqNumberType,
        operation: F
    ) -> OperationResult<bool>
    where F: FnOnce(&mut Segment) -> OperationResult<bool>
    {
        if let Some((failed_operation, error)) = &self.error_status {
            // Failed operations should not be skipped,
            // fail if newer operation is attempted before proper recovery
            if *failed_operation != op_num {
                return Err(OperationError::ServiceError {
                    description: format!("Not recovered from previous error {}", error)
                })
            } // else: Re-try operation
        }
        if self.version > op_num {
            // Skip without execution, current version is higher => operation already applied
            return Ok(false)
        }
        let res = operation(self);

        if is_service_error(&res) {
            // ToDo: Recover previous segment state
            self.error_status = Some((op_num, res.clone().err().unwrap()))
        }

        if res.is_ok() {
            self.version = op_num
        }
        res
    }


    fn lookup_internal_id(&self, point_id: PointIdType) -> OperationResult<PointOffsetType> {
        let internal_id_opt = self.id_mapper.borrow().internal_id(point_id);
        match internal_id_opt {
            Some(internal_id) => Ok(internal_id),
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        }
    }

    fn get_state(&self) -> SegmentState {
        SegmentState {
            version: self.version(),
            config: self.segment_config.clone(),
        }
    }

    fn save_state(&self, state: &SegmentState) -> OperationResult<()> {
        let state_path = self.current_path.join(SEGMENT_STATE_FILE);
        let af = AtomicFile::new(state_path, AllowOverwrite);
        let state_bytes = serde_json::to_vec(state).unwrap();
        af.write(|f| f.write_all(&state_bytes))?;
        Ok(())
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        self.save_state(&self.get_state())
    }
}

impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType {
        self.version
    }

    fn search(
        &self,
        vector: &[VectorElementType],
        with_payload: &WithPayload,
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

        let internal_result = self
            .vector_index
            .borrow()
            .search(vector, filter, top, params);

        let id_mapper = self.id_mapper.borrow();

        let res: OperationResult<Vec<ScoredPoint>> = internal_result
            .iter()
            .map(|&scored_point_offset| {
                let point_id = id_mapper
                    .external_id(scored_point_offset.idx)
                    .ok_or_else(|| OperationError::ServiceError {
                        description: format!(
                            "Corrupter id_mapper, no external value for {}",
                            scored_point_offset.idx
                        ),
                    })?;
                let payload = if with_payload.enable {
                    let initial_payload = self.payload(point_id)?;
                    if let Some(i) = &with_payload.payload_selector {
                        i.process(initial_payload)
                    } else {
                        initial_payload
                    }
                } else {
                    TheMap::new()
                };
                Ok(ScoredPoint {
                    id: point_id,
                    score: scored_point_offset.score,
                    payload,
                })
            })
            .collect();
        res
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector: &[VectorElementType],
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let vector_dim = segment.vector_storage.borrow().vector_dim();
            if vector_dim != vector.len() {
                return Err(OperationError::WrongVector {
                    expected_dim: vector_dim,
                    received_dim: vector.len(),
                });
            }

            let metric = mertic_object(&segment.segment_config.distance);
            let processed_vector = metric
                .preprocess(vector)
                .unwrap_or_else(|| vector.to_owned());

            let stored_internal_point = {
                let id_mapped = segment.id_mapper.borrow();
                id_mapped.internal_id(point_id)
            };

            let (was_replaced, new_index) = match stored_internal_point {
                Some(existing_internal_id) => (
                    true,
                    segment.update_vector(existing_internal_id, processed_vector)?,
                ),
                None => (
                    false,
                    segment.vector_storage
                        .borrow_mut()
                        .put_vector(processed_vector)?,
                ),
            };

            segment.id_mapper.borrow_mut().set_link(point_id, new_index)?;
            Ok(was_replaced)
        })
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let mut mapper = segment.id_mapper.borrow_mut();
            let internal_id = mapper.internal_id(point_id);
            match internal_id {
                Some(internal_id) => {
                    segment.vector_storage.borrow_mut().delete(internal_id)?;
                    mapper.drop(point_id)?;
                    Ok(true)
                }
                None => Ok(false),
            }
        })
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment.payload_storage
                .borrow_mut()
                .assign_all(internal_id, full_payload)?;
            Ok(true)
        })
    }

    fn set_full_payload_with_json(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &str,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            let payload: TheMap<PayloadKeyType, serde_json::value::Value> =
                serde_json::from_str(full_payload)?;
            segment.payload_storage
                .borrow_mut()
                .assign_all_with_value(internal_id, payload)?;
            Ok(true)
        })
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        payload: PayloadType,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment.payload_storage
                .borrow_mut()
                .assign(internal_id, key, payload)?;
            Ok(true)
        })
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment.payload_storage.borrow_mut().delete(internal_id, key)?;
            Ok(true)
        })
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment.payload_storage.borrow_mut().drop(internal_id)?;
            Ok(true)
        })
    }

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self
            .vector_storage
            .borrow()
            .get_vector(internal_id)
            .unwrap())
    }

    fn payload(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<TheMap<PayloadKeyType, PayloadType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.payload_storage.borrow().payload(internal_id))
    }

    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        // Sorry for that, but I didn't find any way easier.
        // If you try simply return iterator - it won't work because AtomicRef should exist
        // If you try to make callback instead - you won't be able to create <dyn SegmentEntry>
        // Attempt to create return borrowed value along with iterator failed because of insane lifetimes
        unsafe { self.id_mapper.as_ptr().as_ref().unwrap().iter_external() }
    }

    fn read_filtered<'a>(
        &'a self,
        offset: PointIdType,
        limit: usize,
        filter: Option<&'a Filter>,
    ) -> Vec<PointIdType> {
        let storage = self.vector_storage.borrow();
        match filter {
            None => self
                .id_mapper
                .borrow()
                .iter_from(offset)
                .map(|x| x.0)
                .take(limit)
                .collect(),
            Some(condition) => self
                .id_mapper
                .borrow()
                .iter_from(offset)
                .filter(move |(_, internal_id)| !storage.is_deleted(*internal_id))
                .filter(move |(_, internal_id)| {
                    self.condition_checker.check(*internal_id, condition)
                })
                .map(|x| x.0)
                .take(limit)
                .collect(),
        }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_mapper.borrow().internal_id(point_id).is_some()
    }

    fn vectors_count(&self) -> usize {
        self.vector_storage.borrow().vector_count()
    }

    fn deleted_count(&self) -> usize {
        self.vector_storage.borrow().deleted_count()
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn info(&self) -> SegmentInfo {
        let indexed_fields = self.payload_index.borrow().indexed_fields();
        let schema = self
            .payload_storage
            .borrow()
            .schema()
            .into_iter()
            .map(|(key, data_type)| {
                let is_indexed = indexed_fields.contains(&key);
                (
                    key,
                    PayloadSchemaInfo {
                        data_type,
                        indexed: is_indexed,
                    },
                )
            })
            .collect();

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.vectors_count(),
            num_deleted_vectors: self.vector_storage.borrow().deleted_count(),
            ram_usage_bytes: 0,  // ToDo: Implement
            disk_usage_bytes: 0, // ToDo: Implement
            is_appendable: self.appendable_flag,
            schema,
        }
    }

    fn config(&self) -> SegmentConfig {
        self.segment_config.clone()
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }

    fn flush(&self) -> OperationResult<SeqNumberType> {
        let mut persisted_version = self.persisted_version.lock().unwrap();
        if *persisted_version == self.version() {
            return Ok(*persisted_version);
        }

        let state = self.get_state();

        self.id_mapper.borrow().flush()?;
        self.payload_storage.borrow().flush()?;
        self.vector_storage.borrow().flush()?;
        self.save_state(&state)?;

        *persisted_version = state.version;

        Ok(state.version)
    }

    fn drop_data(&mut self) -> OperationResult<()> {
        let mut deleted_path = self.current_path.clone();
        deleted_path.set_extension("deleted");
        rename(self.current_path.as_path(), deleted_path.as_path())?;
        Ok(remove_dir_all(&deleted_path)?)
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            Ok(true)
        })
    }

    fn create_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_version(op_num, |segment| {
            segment.payload_index.borrow_mut().set_indexed(key)?;
            Ok(true)
        })
    }

    fn get_indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.payload_index.borrow().indexed_fields()
    }

    fn check_error(&self) -> Option<(SeqNumberType, OperationError)> {
        self.error_status.clone()
    }

    fn reset_error_state(&mut self, op_num: SeqNumberType) -> bool {
        match &self.error_status {
            None => false,
            Some((failed_op, _err)) => if *failed_op == op_num {
                self.error_status = None;
                true
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::entry_point::SegmentEntry;
    use crate::segment_constructor::build_segment;
    use crate::types::{Distance, Indexes, PayloadIndexType, SegmentConfig, StorageType};
    use tempdir::TempDir;


    #[test]
    fn test_set_invalid_payload_from_json() {
        let data1 = r#"
        {
            "invalid_data"
        }"#;
        let data2 = r#"
        {
            "array": [1, "hello"],
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment
            .upsert_point(0, 0, &vec![1.0 as f32, 1.0 as f32])
            .unwrap();
        let result1 = segment.set_full_payload_with_json(0, 0, &data1.to_string());
        match result1 {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }
        let result2 = segment.set_full_payload_with_json(0, 0, &data2.to_string());
        match result2 {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }
    }

    #[test]
    fn test_from_filter_attributes() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment
            .upsert_point(0, 0, &vec![1.0 as f32, 1.0 as f32])
            .unwrap();
        segment
            .set_full_payload_with_json(0, 0, &data.to_string())
            .unwrap();

        let filter_valid_str = r#"
        {
            "must": [
                {
                    "key": "metadata__height",
                    "match": {
                        "integer": 50
                    }
                }
            ]
        }"#;

        let filter_valid: Filter = serde_json::from_str(filter_valid_str).unwrap();
        let filter_invalid_str = r#"
        {
            "must": [
                {
                    "key": "metadata__height",
                    "match": {
                        "integer": 60
                    }
                }
            ]
        }"#;

        let filter_invalid: Filter = serde_json::from_str(filter_invalid_str).unwrap();
        let results_with_valid_filter = segment
            .search(
                &vec![1.0 as f32, 1.0 as f32],
                &WithPayload::default(),
                Some(&filter_valid),
                1,
                None,
            )
            .unwrap();
        assert_eq!(results_with_valid_filter.len(), 1);
        assert_eq!(results_with_valid_filter.first().unwrap().id, 0);
        let results_with_invalid_filter = segment
            .search(
                &vec![1.0 as f32, 1.0 as f32],
                &WithPayload::default(),
                Some(&filter_invalid),
                1,
                None,
            )
            .unwrap();
        assert!(results_with_invalid_filter.is_empty());
    }
}
