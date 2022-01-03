use crate::entry::entry_point::{
    get_service_error, OperationError, OperationResult, SegmentEntry, SegmentFailedState,
};
use crate::id_tracker::IdTracker;
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

/// Segment - an object which manages an independent group of points.
///
/// - Provides storage, indexing and managing operations for points (vectors + payload)
/// - Keeps track of point versions
/// - Persists data
/// - Keeps track of occurred errors
pub struct Segment {
    /// Latest update operation number, applied to this segment
    pub version: SeqNumberType,
    /// Latest persisted version
    pub persisted_version: Arc<Mutex<SeqNumberType>>,
    /// Path of the storage root
    pub current_path: PathBuf,
    /// Component for mapping external ids to internal and also keeping track of point versions
    pub id_tracker: Arc<AtomicRefCell<dyn IdTracker>>,
    pub vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    pub payload_storage: Arc<AtomicRefCell<dyn PayloadStorage>>,
    pub payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    pub condition_checker: Arc<dyn ConditionChecker>,
    pub vector_index: Arc<AtomicRefCell<dyn VectorIndex>>,
    /// Shows if it is possible to insert more points into this segment
    pub appendable_flag: bool,
    /// Shows what kind of indexes and storages are used in this segment
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
    /// Last unhandled error
    /// If not None, all update operations will be aborted until original operation is performed properly
    pub error_status: Option<SegmentFailedState>,
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
                payload_storage.assign_all(new_internal_index, payload)?;
            }
        }

        Ok(new_internal_index)
    }

    fn handle_version_and_failure<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_id: Option<PointIdType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<bool>,
    {
        if let Some(SegmentFailedState {
            version: failed_version,
            point_id: _failed_point_id,
            error,
        }) = &self.error_status
        {
            // Failed operations should not be skipped,
            // fail if newer operation is attempted before proper recovery
            if *failed_version < op_num {
                return Err(OperationError::ServiceError {
                    description: format!("Not recovered from previous error: {}", error),
                });
            } // else: Re-try operation
        }

        let res = self.handle_version(op_num, op_point_id, operation);

        match get_service_error(&res) {
            None => {
                // Recover error state
                match &self.error_status {
                    None => {} // all good
                    Some(error) => {
                        if error.point_id == op_point_id {
                            // Fixed
                            self.error_status = None;
                        }
                    }
                }
            }
            Some(error) => {
                // ToDo: Recover previous segment state
                self.error_status = Some(SegmentFailedState {
                    version: op_num,
                    point_id: op_point_id,
                    error,
                });
            }
        }
        res
    }

    /// Manage segment version checking
    /// If current version if higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    fn handle_version<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_id: Option<PointIdType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<bool>,
    {
        match op_point_id {
            None => {
                // Not a point operation, use global version to check if already applied
                if self.version > op_num {
                    return Ok(false); // Skip without execution
                }
            }
            Some(point_id) => {
                // Check if point not exists or have lower version
                if self
                    .id_tracker
                    .borrow()
                    .version(point_id)
                    .map(|current_version| current_version > op_num)
                    .unwrap_or(false)
                {
                    return Ok(false);
                }
            }
        }

        let res = operation(self);

        if res.is_ok() {
            self.version = op_num;
            if let Some(point_id) = op_point_id {
                self.id_tracker.borrow_mut().set_version(point_id, op_num)?;
            }
        }
        res
    }

    fn lookup_internal_id(&self, point_id: PointIdType) -> OperationResult<PointOffsetType> {
        let internal_id_opt = self.id_tracker.borrow().internal_id(point_id);
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

/// This is a basic implementation of `SegmentEntry`,
/// meaning that it implements the _actual_ operations with data and not any kind of proxy or wrapping
impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType {
        self.version
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        self.id_tracker.borrow().version(point_id)
    }

    fn search(
        &self,
        vector: &[VectorElementType],
        with_payload: &WithPayload,
        with_vector: bool,
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

        let id_tracker = self.id_tracker.borrow();

        let res: OperationResult<Vec<ScoredPoint>> = internal_result
            .iter()
            .map(|&scored_point_offset| {
                let point_id = id_tracker.external_id(scored_point_offset.idx).ok_or(
                    OperationError::ServiceError {
                        description: format!(
                            "Corrupter id_tracker, no external value for {}",
                            scored_point_offset.idx
                        ),
                    },
                )?;
                let point_version =
                    id_tracker
                        .version(point_id)
                        .ok_or(OperationError::ServiceError {
                            description: format!(
                                "Corrupter id_tracker, no version for point {}",
                                point_id
                            ),
                        })?;
                let payload = if with_payload.enable {
                    let initial_payload = self.payload(point_id)?;
                    let processed_payload = if let Some(i) = &with_payload.payload_selector {
                        i.process(initial_payload)
                    } else {
                        initial_payload
                    };
                    Some(processed_payload)
                } else {
                    None
                };

                let vector = if with_vector {
                    Some(self.vector(point_id)?)
                } else {
                    None
                };

                Ok(ScoredPoint {
                    id: point_id,
                    version: point_version,
                    score: scored_point_offset.score,
                    payload,
                    vector,
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
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
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

            let stored_internal_point = segment.id_tracker.borrow().internal_id(point_id);

            let was_replaced = if let Some(existing_internal_id) = stored_internal_point {
                let new_index = segment.update_vector(existing_internal_id, processed_vector)?;
                if new_index != existing_internal_id {
                    let mut id_tracker = segment.id_tracker.borrow_mut();
                    id_tracker.drop(point_id)?;
                    id_tracker.set_link(point_id, new_index)?;
                }
                true
            } else {
                let new_index = segment
                    .vector_storage
                    .borrow_mut()
                    .put_vector(processed_vector)?;
                segment
                    .id_tracker
                    .borrow_mut()
                    .set_link(point_id, new_index)?;
                false
            };

            Ok(was_replaced)
        })
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let mut id_tracker = segment.id_tracker.borrow_mut();
            let internal_id = id_tracker.internal_id(point_id);
            match internal_id {
                Some(internal_id) => {
                    segment.vector_storage.borrow_mut().delete(internal_id)?;
                    id_tracker.drop(point_id)?;
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
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment
                .payload_storage
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
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            let payload: TheMap<PayloadKeyType, serde_json::value::Value> =
                serde_json::from_str(full_payload)?;
            segment
                .payload_storage
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
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment
                .payload_storage
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
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment
                .payload_storage
                .borrow_mut()
                .delete(internal_id, key)?;
            Ok(true)
        })
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
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
        unsafe { self.id_tracker.as_ptr().as_ref().unwrap().iter_external() }
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
                .id_tracker
                .borrow()
                .iter_from(offset)
                .map(|x| x.0)
                .take(limit)
                .collect(),
            Some(condition) => self
                .id_tracker
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
        self.id_tracker.borrow().internal_id(point_id).is_some()
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

        self.id_tracker.borrow().flush()?;
        self.payload_storage.borrow().flush()?;
        self.vector_storage.borrow().flush()?;
        self.save_state(&state)?;

        *persisted_version = state.version;

        Ok(state.version)
    }

    fn drop_data(&mut self) -> OperationResult<()> {
        let mut deleted_path = self.current_path.clone();
        deleted_path.set_extension("deleted");
        rename(&self.current_path, &deleted_path)?;
        Ok(remove_dir_all(&deleted_path)?)
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, None, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            Ok(true)
        })
    }

    fn create_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, None, |segment| {
            segment.payload_index.borrow_mut().set_indexed(key)?;
            Ok(true)
        })
    }

    fn get_indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.payload_index.borrow().indexed_fields()
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.error_status.clone()
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
        segment.upsert_point(0, 0, &[1.0, 1.0]).unwrap();
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
        segment.upsert_point(0, 0, &[1.0, 1.0]).unwrap();
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
                &[1.0, 1.0],
                &WithPayload::default(),
                false,
                Some(&filter_valid),
                1,
                None,
            )
            .unwrap();
        assert_eq!(results_with_valid_filter.len(), 1);
        assert_eq!(results_with_valid_filter.first().unwrap().id, 0);
        let results_with_invalid_filter = segment
            .search(
                &[1.0, 1.0],
                &WithPayload::default(),
                false,
                Some(&filter_invalid),
                1,
                None,
            )
            .unwrap();
        assert!(results_with_invalid_filter.is_empty());
    }
}
