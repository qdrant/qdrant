use crate::common::version::StorageVersion;
use crate::entry::entry_point::OperationError::ServiceError;
use crate::entry::entry_point::{
    get_service_error, OperationError, OperationResult, SegmentEntry, SegmentFailedState,
};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndexSS};
use crate::types::{
    Filter, Payload, PayloadIndexInfo, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
    PointIdType, PointOffsetType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo,
    SegmentState, SegmentType, SeqNumberType, VectorElementType, WithPayload,
};
use crate::vector_storage::VectorStorageSS;
use atomic_refcell::AtomicRefCell;
use atomicwrites::{AllowOverwrite, AtomicFile};
use fs_extra::dir::{copy_with_progress, CopyOptions, TransitProcess};
use rocksdb::DB;
use std::collections::HashMap;
use std::fs::{remove_dir_all, rename, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tar::Builder;

pub const SEGMENT_STATE_FILE: &str = "segment.json";

pub struct SegmentVersion;

impl StorageVersion for SegmentVersion {
    fn current() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

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
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub vector_index: Arc<AtomicRefCell<VectorIndexSS>>,
    /// Shows if it is possible to insert more points into this segment
    pub appendable_flag: bool,
    /// Shows what kind of indexes and storages are used in this segment
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
    /// Last unhandled error
    /// If not None, all update operations will be aborted until original operation is performed properly
    pub error_status: Option<SegmentFailedState>,
    pub database: Arc<AtomicRefCell<DB>>,
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
            let mut payload_index = self.payload_index.borrow_mut();
            let payload = payload_index.drop(old_internal_id)?;
            if let Some(payload) = payload {
                payload_index.assign(new_internal_index, &payload)?;
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
                return Err(OperationError::service_error(&format!(
                    "Not recovered from previous error: {}",
                    error
                )));
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
                    .map_or(false, |current_version| current_version > op_num)
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

    /// Retrieve vector by internal ID
    ///
    /// Panics if vector does not exists or deleted
    #[inline]
    fn vector_by_offset(
        &self,
        point_offset: PointOffsetType,
    ) -> OperationResult<Vec<VectorElementType>> {
        Ok(self
            .vector_storage
            .borrow()
            .get_vector(point_offset)
            .unwrap())
    }

    /// Retrieve payload by internal ID
    #[inline]
    fn payload_by_offset(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        self.payload_index.borrow().payload(point_offset)
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        self.save_state(&self.get_state())
    }

    fn infer_from_payload_data(
        &self,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>> {
        let payload_index = self.payload_index.borrow();
        payload_index.infer_payload_type(key)
    }

    pub fn restore_snapshot(snapshot_path: &Path, segment_id: &str) -> OperationResult<()> {
        let segment_path = snapshot_path.parent().unwrap().join(segment_id);
        let archive_file = File::open(snapshot_path)?;
        let mut ar = tar::Archive::new(archive_file);
        ar.unpack(&segment_path)?;
        Ok(())
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
                let point_offset = scored_point_offset.idx;
                let point_id = id_tracker.external_id(point_offset).ok_or_else(|| {
                    OperationError::service_error(&format!(
                        "Corrupter id_tracker, no external value for {}",
                        scored_point_offset.idx
                    ))
                })?;
                let point_version = id_tracker.version(point_id).ok_or_else(|| {
                    OperationError::service_error(&format!(
                        "Corrupter id_tracker, no version for point {}",
                        point_id
                    ))
                })?;
                let payload = if with_payload.enable {
                    let initial_payload = self.payload_by_offset(point_offset)?;
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
                    Some(self.vector_by_offset(point_offset)?)
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

            let processed_vector = segment
                .segment_config
                .distance
                .preprocess_vector(vector)
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
                    segment.payload_index.borrow_mut().drop(internal_id)?;
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
        full_payload: &Payload,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment
                .payload_index
                .borrow_mut()
                .assign_all(internal_id, full_payload)?;
            Ok(true)
        })
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, Some(point_id), |segment| {
            let internal_id = segment.lookup_internal_id(point_id)?;
            segment
                .payload_index
                .borrow_mut()
                .assign(internal_id, payload)?;
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
                .payload_index
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
            segment.payload_index.borrow_mut().drop(internal_id)?;
            Ok(true)
        })
    }

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.vector_by_offset(internal_id)
    }

    fn payload(&self, point_id: PointIdType) -> OperationResult<Payload> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_by_offset(internal_id)
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
        offset: Option<PointIdType>,
        limit: usize,
        filter: Option<&'a Filter>,
    ) -> Vec<PointIdType> {
        match filter {
            None => self
                .id_tracker
                .borrow()
                .iter_from(offset)
                .map(|x| x.0)
                .take(limit)
                .collect(),
            Some(condition) => {
                let payload_index = self.payload_index.borrow();
                let filter_context = payload_index.filter_context(condition);
                self.id_tracker
                    .borrow()
                    .iter_from(offset)
                    .filter(move |(_, internal_id)| filter_context.check(*internal_id))
                    .map(|x| x.0)
                    .take(limit)
                    .collect()
            }
        }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_tracker.borrow().internal_id(point_id).is_some()
    }

    fn points_count(&self) -> usize {
        self.id_tracker.borrow().points_count()
    }

    fn estimate_points_count<'a>(&'a self, filter: Option<&'a Filter>) -> CardinalityEstimation {
        match filter {
            None => {
                let total_count = self.points_count();
                CardinalityEstimation {
                    primary_clauses: vec![],
                    min: total_count,
                    exp: total_count,
                    max: total_count,
                }
            }
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                payload_index.estimate_cardinality(filter)
            }
        }
    }

    fn deleted_count(&self) -> usize {
        self.vector_storage.borrow().deleted_count()
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn info(&self) -> SegmentInfo {
        let schema = self
            .payload_index
            .borrow()
            .indexed_fields()
            .into_iter()
            .map(|(key, index_schema)| {
                (
                    key,
                    PayloadIndexInfo {
                        data_type: index_schema,
                    },
                )
            })
            .collect();

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.points_count(),
            num_points: self.points_count(),
            num_deleted_vectors: self.vector_storage.borrow().deleted_count(),
            ram_usage_bytes: 0,  // ToDo: Implement
            disk_usage_bytes: 0, // ToDo: Implement
            is_appendable: self.appendable_flag,
            index_schema: schema,
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

        // Flush mapping first to prevent having orphan internal ids.
        self.id_tracker.borrow().flush_mapping().map_err(|err| {
            OperationError::service_error(&format!("Failed to flush id_tracker mapping: {}", err))
        })?;
        self.vector_storage.borrow().flush().map_err(|err| {
            OperationError::service_error(&format!("Failed to flush vector_storage: {}", err))
        })?;

        self.payload_index.borrow().flush().map_err(|err| {
            OperationError::service_error(&format!("Failed to flush payload_index: {}", err))
        })?;
        // Id Tracker contains versions of points. We need to flush it after vector_storage and payload_index flush.
        // This is because vector_storage and payload_index flush are not atomic.
        // If payload or vector flush fails, we will be able to recover data from WAL.
        // If Id Tracker flush fails, we are also able to recover data from WAL
        //  by simply overriding data in vector and payload storages.
        // Once versions are saved - points are considered persisted.
        self.id_tracker.borrow().flush_versions().map_err(|err| {
            OperationError::service_error(&format!("Failed to flush id_tracker versions: {}", err))
        })?;
        self.save_state(&state).map_err(|err| {
            OperationError::service_error(&format!("Failed to flush segment state: {}", err))
        })?;

        self.database.borrow().flush().map_err(|err| {
            OperationError::service_error(&format!("Failed to flush database: {}", err))
        })?;

        *persisted_version = state.version;

        Ok(state.version)
    }

    fn drop_data(self) -> OperationResult<()> {
        let current_path = self.current_path.clone();
        drop(self);
        let mut deleted_path = current_path.clone();
        deleted_path.set_extension("deleted");
        rename(&current_path, &deleted_path)?;
        remove_dir_all(&deleted_path).map_err(|err| {
            OperationError::service_error(&format!(
                "Can't remove segment data at {}, error: {}",
                deleted_path.to_str().unwrap_or_default(),
                err
            ))
        })
    }

    fn data_path(&self) -> PathBuf {
        self.current_path.clone()
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, None, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            Ok(true)
        })
    }

    fn create_field_index(
        &mut self,
        op_num: u64,
        key: PayloadKeyTypeRef,
        field_type: &Option<PayloadSchemaType>,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, None, |segment| match field_type {
            Some(schema_type) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .set_indexed(key, *schema_type)?;
                Ok(true)
            }
            None => match segment.infer_from_payload_data(key)? {
                None => Err(ServiceError {
                    description: "cannot infer field data type".to_string(),
                }),
                Some(schema_type) => {
                    segment
                        .payload_index
                        .borrow_mut()
                        .set_indexed(key, schema_type)?;
                    Ok(true)
                }
            },
        })
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadSchemaType> {
        self.payload_index.borrow().indexed_fields()
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.error_status.clone()
    }

    fn delete_filtered<'a>(
        &'a mut self,
        op_num: SeqNumberType,
        filter: &'a Filter,
    ) -> OperationResult<usize> {
        let mut deleted_points = 0;
        for point_id in self.read_filtered(None, usize::MAX, Some(filter)) {
            deleted_points += self.delete_point(op_num, point_id)? as usize;
        }

        Ok(deleted_points)
    }

    fn vector_dim(&self) -> usize {
        self.segment_config.vector_size
    }

    fn take_snapshot(&self, snapshot_dir_path: &Path) -> OperationResult<()> {
        log::debug!(
            "Taking snapshot of segment {:?} into {:?}",
            self.current_path,
            snapshot_dir_path
        );
        if !snapshot_dir_path.exists() {
            return Err(OperationError::service_error(&format!(
                "the snapshot path provided {:?} does not exist",
                snapshot_dir_path
            )));
        }
        if !snapshot_dir_path.is_dir() {
            return Err(OperationError::service_error(&format!(
                "the snapshot path provided {:?} is not a directory",
                snapshot_dir_path
            )));
        }
        // flush segment to capture latest state
        self.flush()?;
        // extract segment id from current path
        let segment_id = self
            .current_path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap();
        let file_name = format!("{}.tar", segment_id);
        let archive_path = snapshot_dir_path.join(file_name);

        // If `archive_path` exists, we still want to overwrite it
        let file = File::create(archive_path)?;
        let mut builder = Builder::new(file);
        // archive recursively segment directory `current_path` into `archive_path`.
        builder.append_dir_all(".", &self.current_path)?;
        builder.finish()?;
        Ok(())
    }

    fn copy_segment_directory(&self, target_dir_path: &Path) -> OperationResult<PathBuf> {
        log::info!(
            "Copying segment {:?} into {:?}",
            self.current_path,
            target_dir_path
        );
        if !target_dir_path.exists() {
            return Err(OperationError::service_error(&format!(
                "the copy path provided {:?} does not exist",
                target_dir_path
            )));
        }
        if !target_dir_path.is_dir() {
            return Err(OperationError::service_error(&format!(
                "the copy path provided {:?} is not a directory",
                target_dir_path
            )));
        }

        // flush segment to capture latest state
        self.flush()?;

        let segment_id = self
            .current_path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap();

        let handle = |process_info: TransitProcess| {
            log::debug!(
                "Copying segment {} {}/{}",
                segment_id,
                process_info.copied_bytes,
                process_info.total_bytes
            );
            fs_extra::dir::TransitProcessResult::ContinueOrAbort
        };

        let options = CopyOptions::new();
        copy_with_progress(&self.current_path, target_dir_path, &options, handle)?;

        Ok(target_dir_path.join(segment_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::entry_point::SegmentEntry;
    use crate::segment_constructor::build_segment;
    use crate::types::{Distance, Indexes, SegmentConfig, StorageType};
    use std::fs;
    use tar::Archive;
    use tempdir::TempDir;
    use walkdir::WalkDir;

    // no longer valid since users are now allowed to store arbitrary json objects.
    // TODO(gvelo): add tests for invalid payload types on indexed fields.
    // #[test]
    // fn test_set_invalid_payload_from_json() {
    //     let data1 = r#"
    //     {
    //         "invalid_data"
    //     }"#;
    //     let data2 = r#"
    //     {
    //         "array": [1, "hello"],
    //     }"#;
    //
    //     let dir = TempDir::new("payload_dir").unwrap();
    //     let dim = 2;
    //     let config = SegmentConfig {
    //         vector_size: dim,
    //         index: Indexes::Plain {},
    //         payload_index: Some(PayloadIndexType::Plain),
    //         storage_type: StorageType::InMemory,
    //         distance: Distance::Dot,
    //     };
    //
    //     let mut segment =
    //         build_segment(dir.path(), &config, Arc::new(SchemaStorage::new())).unwrap();
    //     segment.upsert_point(0, 0.into(), &[1.0, 1.0]).unwrap();
    //
    //     let result1 = segment.set_full_payload_with_json(0, 0.into(), &data1.to_string());
    //     assert!(result1.is_err());
    //
    //     let result2 = segment.set_full_payload_with_json(0, 0.into(), &data2.to_string());
    //     assert!(result2.is_err());
    // }

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
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment.upsert_point(0, 0.into(), &[1.0, 1.0]).unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();

        segment.set_full_payload(0, 0.into(), &payload).unwrap();

        let filter_valid_str = r#"
        {
            "must": [
                {
                    "key": "metadata.height",
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
                    "key": "metadata.height",
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
        assert_eq!(results_with_valid_filter.first().unwrap().id, 0.into());
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

    #[test]
    fn test_snapshot() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

        let segment_base_dir = TempDir::new("segment_dir").unwrap();
        let config = SegmentConfig {
            vector_size: 2,
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config).unwrap();
        segment.upsert_point(0, 0.into(), &[1.0, 1.0]).unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush().unwrap();

        // Recursively count all files and folders in directory and subdirectories:
        let segment_file_count = WalkDir::new(segment.current_path.clone())
            .into_iter()
            .count();
        assert_eq!(segment_file_count, 20);

        let snapshot_dir = TempDir::new("snapshot_dir").unwrap();

        // snapshotting!
        segment.take_snapshot(snapshot_dir.path()).unwrap();

        // validate that single file has been created
        let archive = fs::read_dir(snapshot_dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let archive_extension = archive.extension().unwrap();
        let archive_name = archive.file_name().unwrap().to_str().unwrap().to_string();

        // correct file extension
        assert_eq!(archive_extension, "tar");

        // archive name contains segment id
        let segment_id = segment
            .current_path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap();
        assert!(archive_name.starts_with(segment_id));

        // decompress archive
        let snapshot_decompress_dir = TempDir::new("snapshot_decompress_dir").unwrap();
        let archive_file = File::open(archive).unwrap();
        let mut ar = Archive::new(archive_file);
        ar.unpack(snapshot_decompress_dir.path()).unwrap();

        // validate the decompressed archive the same number of files as in the segment
        let decompressed_file_count = WalkDir::new(snapshot_decompress_dir.path())
            .into_iter()
            .count();
        assert_eq!(decompressed_file_count, segment_file_count);
    }

    #[test]
    fn test_copy_segment_directory() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

        let segment_base_dir = TempDir::new("segment_dir").unwrap();
        let config = SegmentConfig {
            vector_size: 2,
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config).unwrap();
        segment.upsert_point(0, 0.into(), &[1.0, 1.0]).unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush().unwrap();

        // Recursively count all files and folders in directory and subdirectories.
        let segment_file_count = WalkDir::new(segment.current_path.clone())
            .into_iter()
            .count();
        assert_eq!(segment_file_count, 20);

        let segment_copy_dir = TempDir::new("segment_copy_dir").unwrap();
        let full_copy_path = segment
            .copy_segment_directory(segment_copy_dir.path())
            .unwrap();

        // Recursively count all files and folders in directory and subdirectories.
        let copy_segment_file_count = WalkDir::new(full_copy_path).into_iter().count();

        assert_eq!(copy_segment_file_count, segment_file_count);
    }
}
