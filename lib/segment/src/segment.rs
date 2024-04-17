use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use atomic_refcell::AtomicRefCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use io::file_operations::{atomic_save_json, read_json};
use itertools::Either;
use memory::mmap_ops;
use parking_lot::{Mutex, RwLock};
use rocksdb::DB;
use sparse::common::sparse_vector::SparseVector;
use tar::Builder;
use uuid::Uuid;

use crate::common::operation_error::OperationError::TypeInferenceError;
use crate::common::operation_error::{
    get_service_error, OperationError, OperationResult, SegmentFailedState,
};
use crate::common::validate_snapshot_archive::open_snapshot_archive_with_validation;
use crate::common::version::{StorageVersion, VERSION_FILE};
use crate::common::{check_named_vectors, check_query_vectors, check_stopped, check_vector_name};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::{Direction, OrderBy, OrderingValue};
use crate::data_types::vectors::{MultiDenseVector, QueryVector, Vector, VectorRef};
use crate::entry::entry_point::SegmentEntry;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::numeric_index::StreamRange;
use crate::index::field_index::CardinalityEstimation;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex, VectorIndexEnum};
use crate::json_path::JsonPath;
use crate::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadIndexInfo, PayloadKeyType, PayloadKeyTypeRef,
    PayloadSchemaType, PointIdType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo,
    SegmentState, SegmentType, SeqNumberType, VectorDataInfo, WithPayload, WithVector,
};
use crate::utils;
use crate::utils::fs::find_symlink;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub const SEGMENT_STATE_FILE: &str = "segment.json";

const SNAPSHOT_PATH: &str = "snapshot";

// Sub-directories of `SNAPSHOT_PATH`:
const DB_BACKUP_PATH: &str = "db_backup";
const PAYLOAD_DB_BACKUP_PATH: &str = "payload_index_db_backup";
const SNAPSHOT_FILES_PATH: &str = "files";

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
    /// If None, there were no updates and segment is empty
    pub version: Option<SeqNumberType>,
    /// Latest persisted version
    pub persisted_version: Arc<Mutex<Option<SeqNumberType>>>,
    /// Path of the storage root
    pub current_path: PathBuf,
    /// Component for mapping external ids to internal and also keeping track of point versions
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_data: HashMap<String, VectorData>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    /// Shows if it is possible to insert more points into this segment
    pub appendable_flag: bool,
    /// Shows what kind of indexes and storages are used in this segment
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
    /// Last unhandled error
    /// If not None, all update operations will be aborted until original operation is performed properly
    pub error_status: Option<SegmentFailedState>,
    pub database: Arc<RwLock<DB>>,
    pub flush_thread: Mutex<Option<JoinHandle<OperationResult<SeqNumberType>>>>,
}

pub struct VectorData {
    pub vector_index: Arc<AtomicRefCell<VectorIndexEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
}

impl VectorData {
    pub fn prefault_mmap_pages(&self) -> impl Iterator<Item = mmap_ops::PrefaultMmapPages> {
        let index_task = match &*self.vector_index.borrow() {
            VectorIndexEnum::HnswMmap(index) => index.prefault_mmap_pages(),
            _ => None,
        };

        let storage_task = match &*self.vector_storage.borrow() {
            VectorStorageEnum::DenseMemmap(storage) => storage.prefault_mmap_pages(),
            _ => None,
        };

        index_task.into_iter().chain(storage_task)
    }
}

impl Segment {
    /// Replace vectors in-place
    ///
    /// This replaces all named vectors for this point with the given set of named vectors.
    ///
    /// - new named vectors are inserted
    /// - existing named vectors are replaced
    /// - existing named vectors not specified are deleted
    ///
    /// This differs with [`Segment::update_vectors`], because this deletes unspecified vectors.
    ///
    /// # Warning
    ///
    /// Available for appendable segments only.
    fn replace_all_vectors(
        &mut self,
        internal_id: PointOffsetType,
        vectors: NamedVectors,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        for (vector_name, vector_data) in self.vector_data.iter_mut() {
            let vector = vectors.get(vector_name);
            match vector {
                Some(vector) => {
                    let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    vector_storage.insert_vector(internal_id, vector)?;
                    let mut vector_index = vector_data.vector_index.borrow_mut();
                    vector_index.update_vector(internal_id, vector)?;
                }
                None => {
                    // No vector provided, so we remove it
                    let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    vector_storage.delete_vector(internal_id)?;
                }
            }
        }
        Ok(())
    }

    /// Update vectors in-place
    ///
    /// This updates all specified named vectors for this point with the given set of named vectors, leaving unspecified vectors untouched.
    ///
    /// - new named vectors are inserted
    /// - existing named vectors are replaced
    /// - existing named vectors not specified are untouched and kept as-is
    ///
    /// This differs with [`Segment::replace_all_vectors`], because this keeps unspecified vectors as-is.
    ///
    /// # Warning
    ///
    /// Available for appendable segments only.
    fn update_vectors(
        &mut self,
        internal_id: PointOffsetType,
        vectors: NamedVectors,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        for (vector_name, new_vector) in vectors {
            let vector_data = &self.vector_data[vector_name.as_ref()];
            let new_vector = new_vector.as_vec_ref();
            vector_data
                .vector_storage
                .borrow_mut()
                .insert_vector(internal_id, new_vector)?;
            vector_data
                .vector_index
                .borrow_mut()
                .update_vector(internal_id, new_vector)?;
        }
        Ok(())
    }

    /// Insert new vectors into the segment
    ///
    /// # Warning
    ///
    /// Available for appendable segments only.
    fn insert_new_vectors(
        &mut self,
        point_id: PointIdType,
        vectors: NamedVectors,
    ) -> OperationResult<PointOffsetType> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        let new_index = self.id_tracker.borrow().total_point_count() as PointOffsetType;
        for (vector_name, vector_data) in self.vector_data.iter_mut() {
            let vector_opt = vectors.get(vector_name);
            let mut vector_storage = vector_data.vector_storage.borrow_mut();
            let mut vector_index = vector_data.vector_index.borrow_mut();
            match vector_opt {
                None => {
                    let dim = vector_storage.vector_dim();
                    // placeholder vector for marking deletion
                    let vector = match *vector_storage {
                        VectorStorageEnum::DenseSimple(_)
                        | VectorStorageEnum::DenseSimpleByte(_)
                        | VectorStorageEnum::DenseMemmap(_)
                        | VectorStorageEnum::DenseMemmapByte(_)
                        | VectorStorageEnum::DenseAppendableMemmap(_)
                        | VectorStorageEnum::DenseAppendableMemmapByte(_) => {
                            Vector::from(vec![1.0; dim])
                        }
                        VectorStorageEnum::SparseSimple(_) => Vector::from(SparseVector::default()),
                        VectorStorageEnum::MultiDenseSimple(_) => {
                            Vector::from(MultiDenseVector::placeholder(dim))
                        }
                    };
                    vector_storage.insert_vector(new_index, VectorRef::from(&vector))?;
                    vector_storage.delete_vector(new_index)?;
                    vector_index.update_vector(new_index, VectorRef::from(&vector))?;
                }
                Some(vec) => {
                    vector_storage.insert_vector(new_index, vec)?;
                    vector_index.update_vector(new_index, vec)?;
                }
            }
        }
        self.id_tracker.borrow_mut().set_link(point_id, new_index)?;
        Ok(new_index)
    }

    /// Operation wrapped, which handles previous and new errors in the segment, automatically
    /// updates versions and skips operations if the segment version is too old
    ///
    /// # Arguments
    ///
    /// * `op_num` - sequential operation of the current operation
    /// * `op` - operation to be wrapped. Should return `OperationResult` of bool (which is returned outside)
    ///     and optionally new offset of the changed point.
    ///
    /// # Result
    ///
    /// Propagates `OperationResult` of bool (which is returned in the `op` closure)
    fn handle_segment_version_and_failure<F>(
        &mut self,
        op_num: SeqNumberType,
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
                return Err(OperationError::service_error(format!(
                    "Not recovered from previous error: {error}"
                )));
            } // else: Re-try operation
        }

        let res = self.handle_segment_version(op_num, operation);

        if let Some(error) = get_service_error(&res) {
            // ToDo: Recover previous segment state
            log::error!(
                "Segment {:?} operation error: {error}",
                self.current_path.as_path(),
            );
            self.error_status = Some(SegmentFailedState {
                version: op_num,
                point_id: None,
                error,
            });
        }
        res
    }

    /// Operation wrapped, which handles previous and new errors in the segment, automatically
    /// updates versions and skips operations if the point version is too old
    ///
    /// # Arguments
    ///
    /// * `op_num` - sequential operation of the current operation
    /// * `op_point_offset` - If point offset is specified, handler will use point version for comparison.
    ///     Otherwise, it will be applied without version checks.
    /// * `op` - operation to be wrapped. Should return `OperationResult` of bool (which is returned outside)
    ///     and optionally new offset of the changed point.
    ///
    /// # Result
    ///
    /// Propagates `OperationResult` of bool (which is returned in the `op` closure)
    fn handle_point_version_and_failure<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_offset: Option<PointOffsetType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<(bool, Option<PointOffsetType>)>,
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
                return Err(OperationError::service_error(format!(
                    "Not recovered from previous error: {error}"
                )));
            } // else: Re-try operation
        }

        let res = self.handle_point_version(op_num, op_point_offset, operation);

        match get_service_error(&res) {
            None => {
                // Recover error state
                match &self.error_status {
                    None => {} // all good
                    Some(error) => {
                        let point_id = op_point_offset.and_then(|point_offset| {
                            self.id_tracker.borrow().external_id(point_offset)
                        });
                        if error.point_id == point_id {
                            // Fixed
                            log::info!("Recovered from error: {}", error.error);
                            self.error_status = None;
                        }
                    }
                }
            }
            Some(error) => {
                // ToDo: Recover previous segment state
                log::error!(
                    "Segment {:?} operation error: {error}",
                    self.current_path.as_path(),
                );
                let point_id = op_point_offset
                    .and_then(|point_offset| self.id_tracker.borrow().external_id(point_offset));
                self.error_status = Some(SegmentFailedState {
                    version: op_num,
                    point_id,
                    error,
                });
            }
        }
        res
    }

    /// Manage segment version checking, for segment level operations
    ///
    /// If current version if higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    fn handle_segment_version<F>(
        &mut self,
        op_num: SeqNumberType,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<bool>,
    {
        // Global version to check if operation has already been applied, then skip without execution
        if self.version.unwrap_or(0) > op_num {
            return Ok(false);
        }

        let applied = operation(self)?;
        self.bump_segment_version(op_num);
        Ok(applied)
    }

    /// Manage point version checking inside this segment, for point level operations
    ///
    /// If current version if higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    fn handle_point_version<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_offset: Option<PointOffsetType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<(bool, Option<PointOffsetType>)>,
    {
        // Check if point not exists or have lower version
        if let Some(point_offset) = op_point_offset {
            if self
                .id_tracker
                .borrow()
                .internal_version(point_offset)
                .map_or(false, |current_version| current_version > op_num)
            {
                return Ok(false);
            }
        }

        let (applied, point_id) = operation(self)?;

        self.bump_segment_version(op_num);
        if let Some(point_id) = point_id {
            self.id_tracker
                .borrow_mut()
                .set_internal_version(point_id, op_num)?;
        }

        Ok(applied)
    }

    fn bump_segment_version(&mut self, op_num: SeqNumberType) {
        self.version = Some(max(op_num, self.version.unwrap_or(0)));
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
            version: self.version,
            config: self.segment_config.clone(),
        }
    }

    pub fn save_state(state: &SegmentState, current_path: &Path) -> OperationResult<()> {
        let state_path = current_path.join(SEGMENT_STATE_FILE);
        Ok(atomic_save_json(&state_path, state)?)
    }

    pub fn load_state(current_path: &Path) -> OperationResult<SegmentState> {
        let state_path = current_path.join(SEGMENT_STATE_FILE);
        read_json(&state_path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read segment state {} error: {}",
                current_path.display(),
                err
            ))
        })
    }

    /// Retrieve vector by internal ID
    ///
    /// Returns None if the vector does not exists or deleted
    #[inline]
    fn vector_by_offset(
        &self,
        vector_name: &str,
        point_offset: PointOffsetType,
    ) -> OperationResult<Option<Vector>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let is_vector_deleted = vector_data
            .vector_storage
            .borrow()
            .is_deleted_vector(point_offset);
        if !is_vector_deleted && !self.id_tracker.borrow().is_deleted_point(point_offset) {
            let vector_storage = vector_data.vector_storage.borrow();

            if vector_storage.total_vector_count() <= point_offset as usize {
                // Storage does not have vector with such offset.
                // This is possible if the storage is inconsistent due to interrupted flush.
                // Assume consistency will be restored with WAL replay.

                // Without this check, the service will panic on the `get_vector` call.
                Err(OperationError::InconsistentStorage {
                    description: format!(
                        "Vector storage is inconsistent, total_vector_count: {}, point_offset: {}",
                        vector_storage.total_vector_count(),
                        point_offset
                    ),
                })
            } else {
                Ok(Some(vector_storage.get_vector(point_offset).to_owned()))
            }
        } else {
            Ok(None)
        }
    }

    fn all_vectors_by_offset(
        &self,
        point_offset: PointOffsetType,
    ) -> OperationResult<NamedVectors> {
        let mut vectors = NamedVectors::default();
        for (vector_name, vector_data) in &self.vector_data {
            let is_vector_deleted = vector_data
                .vector_storage
                .borrow()
                .is_deleted_vector(point_offset);
            if !is_vector_deleted {
                let vector_storage = vector_data.vector_storage.borrow();
                let vector = vector_storage
                    .get_vector(point_offset)
                    .as_vec_ref()
                    .to_owned();
                vectors.insert(vector_name.clone(), vector);
            }
        }
        Ok(vectors)
    }

    /// Retrieve payload by internal ID
    #[inline]
    fn payload_by_offset(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        self.payload_index.borrow().payload(point_offset)
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        Self::save_state(&self.get_state(), &self.current_path)
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

        let mut archive = open_snapshot_archive_with_validation(snapshot_path)?;

        archive.unpack(&segment_path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to unpack segment snapshot archive {snapshot_path:?}: {err}"
            ))
        })?;

        let snapshot_path = segment_path.join(SNAPSHOT_PATH);

        if snapshot_path.exists() {
            let db_backup_path = snapshot_path.join(DB_BACKUP_PATH);
            let payload_index_db_backup = snapshot_path.join(PAYLOAD_DB_BACKUP_PATH);

            crate::rocksdb_backup::restore(&db_backup_path, &segment_path)?;

            if payload_index_db_backup.is_dir() {
                StructPayloadIndex::restore_database_snapshot(
                    &payload_index_db_backup,
                    &segment_path,
                )?;
            }

            let files_path = snapshot_path.join(SNAPSHOT_FILES_PATH);

            if let Some(symlink) = find_symlink(&files_path) {
                return Err(OperationError::service_error(format!(
                    "Snapshot is corrupted, can't read file: {:?}",
                    symlink
                )));
            }

            utils::fs::move_all(&files_path, &segment_path)?;

            fs::remove_dir_all(&snapshot_path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to remove {snapshot_path:?} directory: {err}"
                ))
            })?;
        } else {
            log::info!("Attempt to restore legacy snapshot format");
            // Do nothing, legacy format is just plain archive
        }

        Ok(())
    }

    // Joins flush thread if exists
    // Returns lock to guarantee that there will be no other flush in a different thread
    fn lock_flushing(
        &self,
    ) -> OperationResult<parking_lot::MutexGuard<Option<JoinHandle<OperationResult<SeqNumberType>>>>>
    {
        let mut lock = self.flush_thread.lock();
        let mut join_handle: Option<JoinHandle<OperationResult<SeqNumberType>>> = None;
        std::mem::swap(&mut join_handle, &mut lock);
        if let Some(join_handle) = join_handle {
            // Flush result was reported to segment, so we don't need this value anymore
            let _background_flush_result = join_handle
                .join()
                .map_err(|_err| OperationError::service_error("failed to join flush thread"))??;
        }
        Ok(lock)
    }

    fn is_background_flushing(&self) -> bool {
        let lock = self.flush_thread.lock();
        if let Some(join_handle) = lock.as_ref() {
            !join_handle.is_finished()
        } else {
            false
        }
    }

    /// Converts raw ScoredPointOffset search result into ScoredPoint result
    fn process_search_result(
        &self,
        internal_result: &[ScoredPointOffset],
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let id_tracker = self.id_tracker.borrow();
        internal_result
            .iter()
            .filter_map(|&scored_point_offset| {
                let point_offset = scored_point_offset.idx;
                let external_id = id_tracker.external_id(point_offset);
                match external_id {
                    Some(point_id) => Some((point_id, scored_point_offset)),
                    None => {
                        log::warn!(
                            "Point with internal ID {} not found in id tracker, skipping",
                            point_offset
                        );
                        None
                    }
                }
            })
            .map(|(point_id, scored_point_offset)| {
                let point_offset = scored_point_offset.idx;
                let point_version = id_tracker.internal_version(point_offset).ok_or_else(|| {
                    OperationError::service_error(format!(
                        "Corrupter id_tracker, no version for point {point_id}"
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
                let vector = match with_vector {
                    WithVector::Bool(false) => None,
                    WithVector::Bool(true) => {
                        Some(self.all_vectors_by_offset(point_offset)?.into())
                    }
                    WithVector::Selector(vectors) => {
                        let mut result = NamedVectors::default();
                        for vector_name in vectors {
                            if let Some(vector) =
                                self.vector_by_offset(vector_name, point_offset)?
                            {
                                result.insert(vector_name.clone(), vector);
                            }
                        }
                        Some(result.into())
                    }
                };

                Ok(ScoredPoint {
                    id: point_id,
                    version: point_version,
                    score: scored_point_offset.score,
                    payload,
                    vector,
                    shard_key: None,
                })
            })
            .collect()
    }

    /// Estimates how many checks it would need for getting `limit` amount of points by streaming and then
    /// filtering, versus getting all filtered points from the index and then sorting them afterwards.
    ///
    /// If the filter is restrictive enough to yield fewer points than the amount of points a streaming
    /// approach would need to advance, it returns true.
    fn should_pre_filter(&self, filter: &Filter, limit: Option<usize>) -> bool {
        let query_cardinality = {
            let payload_index = self.payload_index.borrow();
            payload_index.estimate_cardinality(filter)
        };

        // ToDo: Add telemetry for this heuristics

        // Calculate expected number of condition checks required for
        // this scroll request with is stream strategy.
        // Example:
        //  - cardinality = 1000
        //  - limit = 10
        //  - total = 10000
        //  - point filter prob = 1000 / 10000 = 0.1
        //  - expected_checks = 10 / 0.1  = 100
        //  -------------------------------
        //  - cardinality = 10
        //  - limit = 10
        //  - total = 10000
        //  - point filter prob = 10 / 10000 = 0.001
        //  - expected_checks = 10 / 0.001  = 10000

        let available_points = self.available_point_count() + 1 /* + 1 for division-by-zero */;
        // Expected number of successful checks per point
        let check_probability =
            (query_cardinality.exp as f64 + 1.0/* protect from zero */) / available_points as f64;
        let exp_stream_checks =
            (limit.unwrap_or(available_points) as f64 / check_probability) as usize;

        // Assume it would require about `query cardinality` checks.
        // We are interested in approximate number of checks, so we can
        // use `query cardinality` as a starting point.
        let exp_index_checks = query_cardinality.max;

        exp_stream_checks > exp_index_checks
    }

    fn read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
    ) -> Vec<PointIdType> {
        self.id_tracker
            .borrow()
            .iter_from(offset)
            .map(|x| x.0)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn filtered_read_by_index(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();

        let ids_iterator = payload_index
            .query_points(condition)
            .into_iter()
            .filter_map(|internal_id| {
                let external_id = id_tracker.external_id(internal_id);
                match external_id {
                    Some(external_id) => match offset {
                        Some(offset) if external_id < offset => None,
                        _ => Some(external_id),
                    },
                    None => None,
                }
            });

        let mut page = match limit {
            Some(limit) => peek_top_smallest_iterable(ids_iterator, limit),
            None => ids_iterator.collect(),
        };

        page.sort_unstable();

        page
    }

    pub fn filtered_read_by_index_ordered(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        condition: &Filter,
    ) -> OperationResult<Vec<(OrderingValue, PointIdType)>> {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();

        let numeric_index = payload_index
            .field_indexes
            .get(&order_by.key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
            .ok_or_else(|| OperationError::ValidationError { description: "There is no range index for the `order_by` key, please create one to use `order_by`".to_string() })?;

        let start_from = order_by.start_from();

        let values_ids_iterator = payload_index
            .query_points(condition)
            .into_iter()
            .flat_map(|internal_id| {
                // Repeat a point for as many values as it has
                numeric_index
                    .get_ordering_values(internal_id)
                    // But only those which start from `start_from` 😛
                    .filter(|value| match order_by.direction() {
                        Direction::Asc => value >= &start_from,
                        Direction::Desc => value <= &start_from,
                    })
                    .map(move |ordering_value| (ordering_value, internal_id))
            })
            .filter_map(|(value, internal_id)| {
                id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            });

        let page = match order_by.direction() {
            Direction::Asc => {
                let mut page = match limit {
                    Some(limit) => peek_top_smallest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by(|(value_a, _), (value_b, _)| value_a.cmp(value_b));
                page
            }
            Direction::Desc => {
                let mut page = match limit {
                    Some(limit) => peek_top_largest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by(|(value_a, _), (value_b, _)| value_b.cmp(value_a));
                page
            }
        };

        Ok(page)
    }

    pub fn filtered_read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let filter_context = payload_index.filter_context(condition);
        self.id_tracker
            .borrow()
            .iter_from(offset)
            .filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map(|(external_id, _)| external_id)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn filtered_read_by_value_stream(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        filter: Option<&Filter>,
    ) -> OperationResult<Vec<(OrderingValue, PointIdType)>> {
        let payload_index = self.payload_index.borrow();

        let numeric_index = payload_index
            .field_indexes
            .get(&order_by.key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
            .ok_or_else(|| OperationError::ValidationError { description: "There is no range index for the `order_by` key, please create one to use `order_by`".to_string() })?;

        let range_iter = numeric_index.stream_range(&order_by.as_range());

        let directed_range_iter = match order_by.direction() {
            Direction::Asc => Either::Left(range_iter),
            Direction::Desc => Either::Right(range_iter.rev()),
        };

        let id_tracker = self.id_tracker.borrow();

        let filtered_iter = match filter {
            None => Either::Left(directed_range_iter),
            Some(filter) => {
                let filter_context = payload_index.filter_context(filter);

                Either::Right(
                    directed_range_iter
                        .filter(move |(_, internal_id)| filter_context.check(*internal_id)),
                )
            }
        };

        let reads = filtered_iter
            .filter_map(|(value, internal_id)| {
                id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            })
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        Ok(reads)
    }

    /// Check consistency of the segment's data and repair it if possible.
    pub fn check_consistency_and_repair(&mut self) -> OperationResult<()> {
        let mut internal_ids_to_delete = HashSet::new();
        let id_tracker = self.id_tracker.borrow();
        for internal_id in id_tracker.iter_ids() {
            if id_tracker.external_id(internal_id).is_none() {
                internal_ids_to_delete.insert(internal_id);
            }
        }

        if !internal_ids_to_delete.is_empty() {
            log::info!(
                "Found {} points in vector storage without external id - those will be deleted",
                internal_ids_to_delete.len(),
            );

            for internal_id in &internal_ids_to_delete {
                // Drop removed points from payload index
                self.payload_index.borrow_mut().drop(*internal_id)?;

                // Drop removed points from vector storage
                for vector_data in self.vector_data.values() {
                    let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    vector_storage.delete_vector(*internal_id)?;
                }
            }

            // We do not drop version here, because it is already not loaded into memory.
            // There are no explicit mapping between internal ID and version, so all dangling
            // versions will be ignored automatically.
            // Those versions could be overwritten by new points, but it is not a problem.
            // They will also be deleted by the next optimization.
        }

        // Flush entire segment if needed
        if !internal_ids_to_delete.is_empty() {
            self.flush(true)?;
        }
        Ok(())
    }

    pub fn available_vector_count(&self, vector_name: &str) -> OperationResult<usize> {
        check_vector_name(vector_name, &self.segment_config)?;
        Ok(self.vector_data[vector_name]
            .vector_storage
            .borrow()
            .available_vector_count())
    }

    pub fn total_point_count(&self) -> usize {
        self.id_tracker.borrow().total_point_count()
    }

    pub fn prefault_mmap_pages(&self) {
        let tasks: Vec<_> = self
            .vector_data
            .values()
            .flat_map(|data| data.prefault_mmap_pages())
            .collect();

        let _ = thread::Builder::new()
            .name(format!(
                "segment-{:?}-prefault-mmap-pages",
                self.current_path,
            ))
            .spawn(move || tasks.iter().for_each(mmap_ops::PrefaultMmapPages::exec));
    }

    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        vector_name: &str,
        vector: &QueryVector,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let result = self.search_batch(
            vector_name,
            &[vector],
            with_payload,
            with_vector,
            filter,
            top,
            params,
            &false.into(),
            usize::MAX,
        )?;

        Ok(result.into_iter().next().unwrap())
    }
}

/// This is a basic implementation of `SegmentEntry`,
/// meaning that it implements the _actual_ operations with data and not any kind of proxy or wrapping
impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType {
        self.version.unwrap_or(0)
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        let id_tracker = self.id_tracker.borrow();
        id_tracker
            .internal_id(point_id)
            .and_then(|internal_id| id_tracker.internal_version(internal_id))
    }

    fn search_batch(
        &self,
        vector_name: &str,
        query_vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        is_stopped: &AtomicBool,
        search_optimized_threshold_kb: usize,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        check_query_vectors(vector_name, query_vectors, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let internal_results = vector_data.vector_index.borrow().search(
            query_vectors,
            filter,
            top,
            params,
            is_stopped,
            search_optimized_threshold_kb,
        )?;

        check_stopped(is_stopped)?;

        let res = internal_results
            .iter()
            .map(|internal_result| {
                self.process_search_result(internal_result, with_payload, with_vector)
            })
            .collect();

        res
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
    ) -> OperationResult<bool> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(&self.segment_config);
        let stored_internal_point = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, stored_internal_point, |segment| {
            if let Some(existing_internal_id) = stored_internal_point {
                segment.replace_all_vectors(existing_internal_id, vectors)?;
                Ok((true, Some(existing_internal_id)))
            } else {
                let new_index = segment.insert_new_vectors(point_id, vectors)?;
                Ok((false, Some(new_index)))
            }
        })
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            // Point does already not exist anymore
            None => Ok(false),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    // Mark point as deleted, drop mapping
                    segment.payload_index.borrow_mut().drop(internal_id)?;
                    segment.id_tracker.borrow_mut().drop(point_id)?;

                    // Before, we propagated point deletions to also delete its vectors. This turns
                    // out to be problematic because this sometimes makes us loose vector data
                    // because we cannot control the order of segment flushes.
                    // Disabled until we properly fix it or find a better way to clean up old
                    // vectors.
                    //
                    // // Propagate point deletion to all its vectors
                    // for vector_data in segment.vector_data.values() {
                    //     let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    //     vector_storage.delete_vector(internal_id)?;
                    // }

                    Ok((true, Some(internal_id)))
                })
            }
        }
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
    ) -> OperationResult<bool> {
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(&self.segment_config);
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    segment.update_vectors(internal_id, vectors)?;
                    Ok((true, Some(internal_id)))
                })
            }
        }
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &str,
    ) -> OperationResult<bool> {
        check_vector_name(vector_name, &self.segment_config)?;
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    let vector_data = segment.vector_data.get(vector_name).ok_or(
                        OperationError::VectorNameNotExists {
                            received_name: vector_name.to_string(),
                        },
                    )?;
                    let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    let is_deleted = vector_storage.delete_vector(internal_id)?;
                    Ok((is_deleted, Some(internal_id)))
                })
            }
        }
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .assign_all(internal_id, full_payload)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .assign(internal_id, payload, key)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .delete(internal_id, key)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment.payload_index.borrow_mut().drop(internal_id)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn vector(&self, vector_name: &str, point_id: PointIdType) -> OperationResult<Option<Vector>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let internal_id = self.lookup_internal_id(point_id)?;
        let vector_opt = self.vector_by_offset(vector_name, internal_id)?;
        Ok(vector_opt)
    }

    fn all_vectors(&self, point_id: PointIdType) -> OperationResult<NamedVectors> {
        let mut result = NamedVectors::default();
        for vector_name in self.vector_data.keys() {
            if let Some(vec) = self.vector(vector_name, point_id)? {
                result.insert(vector_name.clone(), vec);
            }
        }
        Ok(result)
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
        limit: Option<usize>,
        filter: Option<&'a Filter>,
    ) -> Vec<PointIdType> {
        match filter {
            None => self.read_by_id_stream(offset, limit),
            Some(condition) => {
                if self.should_pre_filter(condition, limit) {
                    self.filtered_read_by_index(offset, limit, condition)
                } else {
                    self.filtered_read_by_id_stream(offset, limit, condition)
                }
            }
        }
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a OrderBy,
    ) -> OperationResult<Vec<(OrderingValue, PointIdType)>> {
        match filter {
            None => self.filtered_read_by_value_stream(order_by, limit, None),
            Some(filter) => {
                if self.should_pre_filter(filter, limit) {
                    self.filtered_read_by_index_ordered(order_by, limit, filter)
                } else {
                    self.filtered_read_by_value_stream(order_by, limit, Some(filter))
                }
            }
        }
    }

    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        let id_tracker = self.id_tracker.borrow();
        let iterator = id_tracker.iter_from(from).map(|x| x.0);
        match to {
            None => iterator.collect(),
            Some(to_id) => iterator.take_while(|x| *x < to_id).collect(),
        }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_tracker.borrow().internal_id(point_id).is_some()
    }

    fn available_point_count(&self) -> usize {
        self.id_tracker.borrow().available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.id_tracker.borrow().deleted_point_count()
    }

    fn estimate_point_count<'a>(&'a self, filter: Option<&'a Filter>) -> CardinalityEstimation {
        match filter {
            None => {
                let available = self.available_point_count();
                CardinalityEstimation {
                    primary_clauses: vec![],
                    min: available,
                    exp: available,
                    max: available,
                }
            }
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                payload_index.estimate_cardinality(filter)
            }
        }
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn info(&self) -> SegmentInfo {
        let payload_index = self.payload_index.borrow();
        let schema = payload_index
            .indexed_fields()
            .into_iter()
            .map(|(key, index_schema)| {
                let points_count = payload_index.indexed_points(&key);
                (key, PayloadIndexInfo::new(index_schema, points_count))
            })
            .collect();

        let num_vectors = self
            .vector_data
            .values()
            .map(|data| data.vector_storage.borrow().available_vector_count())
            .sum();

        let vector_data_info = self
            .vector_data
            .iter()
            .map(|(key, vector_data)| {
                let vector_storage = vector_data.vector_storage.borrow();
                let num_vectors = vector_storage.available_vector_count();
                let vector_index = vector_data.vector_index.borrow();
                let is_indexed = vector_index.is_index();
                let vector_data_info = VectorDataInfo {
                    num_vectors,
                    num_indexed_vectors: if is_indexed {
                        vector_index.indexed_vector_count()
                    } else {
                        0
                    },
                    num_deleted_vectors: vector_storage.deleted_vector_count(),
                };
                (key.to_string(), vector_data_info)
            })
            .collect();

        let num_indexed_vectors = if self.segment_type == SegmentType::Indexed {
            self.vector_data
                .values()
                .map(|data| data.vector_index.borrow().indexed_vector_count())
                .sum()
        } else {
            0
        };

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors,
            num_indexed_vectors,
            num_points: self.available_point_count(),
            num_deleted_vectors: self.deleted_point_count(),
            ram_usage_bytes: 0,  // ToDo: Implement
            disk_usage_bytes: 0, // ToDo: Implement
            is_appendable: self.appendable_flag,
            index_schema: schema,
            vector_data: vector_data_info,
        }
    }

    fn config(&self) -> &SegmentConfig {
        &self.segment_config
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }

    fn flush(&self, sync: bool) -> OperationResult<SeqNumberType> {
        let current_persisted_version: Option<SeqNumberType> = *self.persisted_version.lock();
        if !sync && self.is_background_flushing() {
            return Ok(current_persisted_version.unwrap_or(0));
        }

        let mut background_flush_lock = self.lock_flushing()?;
        match (self.version, current_persisted_version) {
            (None, _) => {
                // Segment is empty, nothing to flush
                return Ok(current_persisted_version.unwrap_or(0));
            }
            (Some(version), Some(persisted_version)) => {
                if version == persisted_version {
                    // Segment is already flushed
                    return Ok(persisted_version);
                }
            }
            (_, _) => {}
        }

        let vector_storage_flushers: Vec<_> = self
            .vector_data
            .values()
            .map(|v| v.vector_storage.borrow().flusher())
            .collect();
        let state = self.get_state();
        let current_path = self.current_path.clone();
        let id_tracker_mapping_flusher = self.id_tracker.borrow().mapping_flusher();
        let payload_index_flusher = self.payload_index.borrow().flusher();
        let id_tracker_versions_flusher = self.id_tracker.borrow().versions_flusher();
        let persisted_version = self.persisted_version.clone();

        // Flush order is important:
        //
        // 1. Flush id mapping. So during recovery the point will be recovered er in proper segment.
        // 2. Flush vectors and payloads.
        // 3. Flush id versions last. So presence of version indicates that all other data is up-to-date.
        //
        // Example of recovery from WAL in case of partial flush:
        //
        // In-memory state:
        //
        //     Segment 1                  Segment 2
        //
        //    ID-mapping     vst.1       ID-mapping     vst.2
        //   ext     int
        //  ┌───┐   ┌───┐   ┌───┐       ┌───┐   ┌───┐   ┌───┐
        //  │100├───┤1  │   │1  │       │300├───┤1  │   │1  │
        //  └───┘   └───┘   │2  │       └───┘   └───┘   │2  │
        //                  │   │                       │   │
        //  ┌───┐   ┌───┐   │   │       ┌───┐   ┌───┐   │   │
        //  │200├───┤2  │   │   │       │400├───┤2  │   │   │
        //  └───┘   └───┘   └───┘       └───┘   └───┘   └───┘
        //
        //
        //  ext - external id
        //  int - internal id
        //  vst - vector storage
        //
        //  ─────────────────────────────────────────────────
        //   After flush, segments could be partially preserved:
        //
        //  ┌───┐   ┌───┐   ┌───┐       ┌───┐   ┌───┐   ┌───┐
        //  │100├───┤1  │   │ 1 │       │300├───┤1  │   │ * │
        //  └───┘   └───┘   │   │       └───┘   └───┘   │ * │
        //                  │   │                       │ 3 │
        //                  │   │       ┌───┐   ┌───┐   │   │
        //                  │   │       │400├───┤2  │   │   │
        //                  └───┘       └───┘   └───┘   └───┘
        //  WAL:      ▲
        //            │                 ┌───┐   ┌───┐
        //  100───────┘      ┌────────► │200├───┤3  │
        //                   |          └───┘   └───┘
        //  200──────────────┘
        //
        //  300
        //
        //  400

        let flush_op = move || {
            // Flush mapping first to prevent having orphan internal ids.
            id_tracker_mapping_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush id_tracker mapping: {err}"))
            })?;
            for vector_storage_flusher in vector_storage_flushers {
                vector_storage_flusher().map_err(|err| {
                    OperationError::service_error(format!("Failed to flush vector_storage: {err}"))
                })?;
            }
            payload_index_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush payload_index: {err}"))
            })?;
            // Id Tracker contains versions of points. We need to flush it after vector_storage and payload_index flush.
            // This is because vector_storage and payload_index flush are not atomic.
            // If payload or vector flush fails, we will be able to recover data from WAL.
            // If Id Tracker flush fails, we are also able to recover data from WAL
            //  by simply overriding data in vector and payload storages.
            // Once versions are saved - points are considered persisted.
            id_tracker_versions_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush id_tracker versions: {err}"))
            })?;
            Self::save_state(&state, &current_path).map_err(|err| {
                OperationError::service_error(format!("Failed to flush segment state: {err}"))
            })?;
            *persisted_version.lock() = state.version;

            debug_assert!(state.version.is_some());
            Ok(state.version.unwrap_or(0))
        };

        if sync {
            flush_op()
        } else {
            *background_flush_lock = Some(
                thread::Builder::new()
                    .name("background_flush".to_string())
                    .spawn(flush_op)
                    .unwrap(),
            );
            Ok(current_persisted_version.unwrap_or(0))
        }
    }

    fn drop_data(self) -> OperationResult<()> {
        let current_path = self.current_path.clone();
        drop(self);
        let mut deleted_path = current_path.clone();
        deleted_path.set_extension("deleted");
        fs::rename(&current_path, &deleted_path)?;
        fs::remove_dir_all(&deleted_path).map_err(|err| {
            OperationError::service_error(format!(
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
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            Ok(true)
        })
    }

    fn create_field_index(
        &mut self,
        op_num: u64,
        key: PayloadKeyTypeRef,
        field_type: Option<&PayloadFieldSchema>,
    ) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| match field_type {
            Some(schema) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .set_indexed(key, schema.clone())?;
                Ok(true)
            }
            None => match segment.infer_from_payload_data(key)? {
                None => Err(TypeInferenceError {
                    field_name: key.clone(),
                }),
                Some(schema_type) => {
                    segment
                        .payload_index
                        .borrow_mut()
                        .set_indexed(key, schema_type.into())?;
                    Ok(true)
                }
            },
        })
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
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
        for point_id in self.read_filtered(None, None, Some(filter)) {
            deleted_points += self.delete_point(op_num, point_id)? as usize;
        }

        Ok(deleted_points)
    }

    fn vector_dim(&self, vector_name: &str) -> OperationResult<usize> {
        check_vector_name(vector_name, &self.segment_config)?;
        Ok(self.vector_data[vector_name]
            .vector_storage
            .borrow()
            .vector_dim())
    }

    fn vector_dims(&self) -> HashMap<String, usize> {
        self.vector_data
            .iter()
            .map(|(vector_name, vector_data)| {
                (
                    vector_name.clone(),
                    vector_data.vector_storage.borrow().vector_dim(),
                )
            })
            .collect()
    }

    fn take_snapshot(
        &self,
        temp_path: &Path,
        snapshot_dir_path: &Path,
    ) -> OperationResult<PathBuf> {
        log::debug!(
            "Taking snapshot of segment {:?} into {:?}",
            self.current_path,
            snapshot_dir_path,
        );

        if !snapshot_dir_path.exists() {
            return Err(OperationError::service_error(format!(
                "the snapshot path {snapshot_dir_path:?} does not exist"
            )));
        }

        if !snapshot_dir_path.is_dir() {
            return Err(OperationError::service_error(format!(
                "the snapshot path {snapshot_dir_path:?} is not a directory",
            )));
        }

        // flush segment to capture latest state
        self.flush(true)?;

        // use temp_path for intermediary files
        let temp_path = temp_path.join(format!("segment-{}", Uuid::new_v4()));
        let db_backup_path = temp_path.join(DB_BACKUP_PATH);
        let payload_index_db_backup_path = temp_path.join(PAYLOAD_DB_BACKUP_PATH);

        {
            let db = self.database.read();
            crate::rocksdb_backup::create(&db, &db_backup_path)?;
        }

        self.payload_index
            .borrow()
            .take_database_snapshot(&payload_index_db_backup_path)?;

        let segment_id = self
            .current_path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap();

        let archive_path = snapshot_dir_path.join(format!("{segment_id}.tar"));

        // If `archive_path` exists, we still want to overwrite it
        let file = File::create(&archive_path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to create segment snapshot archive {archive_path:?}: {err}"
            ))
        })?;

        let mut builder = Builder::new(file);

        builder
            .append_dir_all(SNAPSHOT_PATH, &temp_path)
            .map_err(|err| utils::tar::failed_to_append_error(&temp_path, err))?;

        let files = Path::new(SNAPSHOT_PATH).join(SNAPSHOT_FILES_PATH);

        for vector_data in self.vector_data.values() {
            for file in vector_data.vector_index.borrow().files() {
                utils::tar::append_file_relative_to_base(
                    &mut builder,
                    &self.current_path,
                    &file,
                    &files,
                )?;
            }

            for file in vector_data.vector_storage.borrow().files() {
                utils::tar::append_file_relative_to_base(
                    &mut builder,
                    &self.current_path,
                    &file,
                    &files,
                )?;
            }

            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().as_ref() {
                for file in quantized_vectors.files() {
                    utils::tar::append_file_relative_to_base(
                        &mut builder,
                        &self.current_path,
                        &file,
                        &files,
                    )?;
                }
            }
        }

        for file in self.payload_index.borrow().files() {
            utils::tar::append_file_relative_to_base(
                &mut builder,
                &self.current_path,
                &file,
                &files,
            )?;
        }

        utils::tar::append_file(
            &mut builder,
            &self.current_path.join(SEGMENT_STATE_FILE),
            &files.join(SEGMENT_STATE_FILE),
        )?;

        utils::tar::append_file(
            &mut builder,
            &self.current_path.join(VERSION_FILE),
            &files.join(VERSION_FILE),
        )?;

        builder.finish()?;

        // remove tmp directory in background
        let _ = thread::spawn(move || {
            let res = fs::remove_dir_all(&temp_path);
            if let Err(err) = res {
                log::error!(
                    "Failed to remove tmp directory at {}: {:?}",
                    temp_path.display(),
                    err
                );
            }
        });

        Ok(archive_path)
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        let vector_index_searches: Vec<_> = self
            .vector_data
            .iter()
            .map(|(k, v)| {
                let mut telemetry = v.vector_index.borrow().get_telemetry_data(detail);
                telemetry.index_name = Some(k.clone());
                telemetry
            })
            .collect();

        SegmentTelemetry {
            info: self.info(),
            config: self.config().clone(),
            vector_index_searches,
            payload_field_indices: self.payload_index.borrow().get_telemetry_data(),
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let _lock = self.lock_flushing();
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::check_vector;
    use crate::common::operation_error::OperationError::PointIdError;
    use crate::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
    use crate::segment_constructor::{build_segment, load_segment};
    use crate::types::{Distance, Indexes, SegmentConfig, VectorDataConfig, VectorStorageType};

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
    //     let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
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
    fn test_search_batch_equivalence_single() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let dim = 4;
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config, true).unwrap();

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        segment
            .upsert_point(100, 4.into(), only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        segment
            .upsert_point(101, 6.into(), only_default_vector(&vec6))
            .unwrap();
        segment.delete_point(102, 1.into()).unwrap();

        let query_vector = [1.0, 1.0, 1.0, 1.0].into();
        let search_result = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();
        eprintln!("search_result = {search_result:#?}");

        let search_batch_result = segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query_vector],
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
                &false.into(),
                10_000,
            )
            .unwrap();
        eprintln!("search_batch_result = {search_batch_result:#?}");

        assert!(!search_result.is_empty());
        assert_eq!(search_result, search_batch_result[0].clone())
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

        let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(dir.path(), &config, true).unwrap();
        segment
            .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();

        segment.set_full_payload(0, 0.into(), &payload).unwrap();

        let filter_valid_str = r#"
        {
            "must": [
                {
                    "key": "metadata.height",
                    "match": {
                        "value": 50
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
                        "value": 60
                    }
                }
            ]
        }"#;

        let filter_invalid: Filter = serde_json::from_str(filter_invalid_str).unwrap();
        let results_with_valid_filter = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &[1.0, 1.0].into(),
                &WithPayload::default(),
                &false.into(),
                Some(&filter_valid),
                1,
                None,
            )
            .unwrap();
        assert_eq!(results_with_valid_filter.len(), 1);
        assert_eq!(results_with_valid_filter.first().unwrap().id, 0.into());
        let results_with_invalid_filter = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &[1.0, 1.0].into(),
                &WithPayload::default(),
                &false.into(),
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

        let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: 2,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config, true).unwrap();

        segment
            .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();

        segment
            .set_full_payload(1, 0.into(), &serde_json::from_str(data).unwrap())
            .unwrap();

        let snapshot_dir = Builder::new().prefix("snapshot_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();

        // snapshotting!
        let archive = segment
            .take_snapshot(temp_dir.path(), snapshot_dir.path())
            .unwrap();
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

        // restore snapshot
        Segment::restore_snapshot(&archive, segment_id).unwrap();

        let restored_segment = load_segment(
            &snapshot_dir.path().join(segment_id),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();

        // validate restored snapshot is the same as original segment
        assert_eq!(segment.vector_dims(), restored_segment.vector_dims());
        assert_eq!(
            segment.total_point_count(),
            restored_segment.total_point_count(),
        );
        assert_eq!(
            segment.available_point_count(),
            restored_segment.available_point_count(),
        );
        assert_eq!(
            segment.deleted_point_count(),
            restored_segment.deleted_point_count(),
        );

        for id in segment.iter_points() {
            let vectors = segment.all_vectors(id).unwrap();
            let restored_vectors = restored_segment.all_vectors(id).unwrap();
            assert_eq!(vectors, restored_vectors);

            let payload = segment.payload(id).unwrap();
            let restored_payload = restored_segment.payload(id).unwrap();
            assert_eq!(payload, restored_payload);
        }
    }

    #[test]
    fn test_background_flush() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

        let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: 2,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config, true).unwrap();
        segment
            .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush(false).unwrap();

        // call flush second time to check that background flush finished successful
        segment.flush(true).unwrap();
    }

    #[test]
    fn test_check_consistency() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let dim = 4;
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config, true).unwrap();

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        segment
            .upsert_point(100, 4.into(), only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        segment
            .upsert_point(101, 6.into(), only_default_vector(&vec6))
            .unwrap();

        // first pass on consistent data
        segment.check_consistency_and_repair().unwrap();

        let query_vector = [1.0, 1.0, 1.0, 1.0].into();
        let search_result = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();

        assert_eq!(search_result.len(), 2);
        assert_eq!(search_result[0].id, 6.into());
        assert_eq!(search_result[1].id, 4.into());

        assert!(segment.vector(DEFAULT_VECTOR_NAME, 6.into()).is_ok());

        let internal_id = segment.lookup_internal_id(6.into()).unwrap();

        // make id_tracker inconsistent
        segment.id_tracker.borrow_mut().drop(6.into()).unwrap();

        let search_result = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();

        // only one result because of inconsistent id_tracker
        assert_eq!(search_result.len(), 1);
        assert_eq!(search_result[0].id, 4.into());

        // querying by external id is broken
        assert!(
            matches!(segment.vector(DEFAULT_VECTOR_NAME, 6.into()), Err(PointIdError {missed_point_id }) if missed_point_id == 6.into())
        );

        // but querying by internal id still works
        matches!(
            segment.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id),
            Ok(Some(_))
        );

        // fix segment's data
        segment.check_consistency_and_repair().unwrap();

        // querying by internal id now consistent
        matches!(
            segment.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id),
            Ok(None)
        );
    }

    #[test]
    fn test_point_vector_count() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let dim = 1;
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multi_vec_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config, true).unwrap();

        // Insert point ID 4 and 6, assert counts
        segment
            .upsert_point(100, 4.into(), only_default_vector(&[0.4]))
            .unwrap();
        segment
            .upsert_point(101, 6.into(), only_default_vector(&[0.6]))
            .unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 2);
        assert_eq!(segment_info.num_vectors, 2);

        // Delete nonexistent point, counts should remain the same
        segment.delete_point(102, 1.into()).unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 2);
        assert_eq!(segment_info.num_vectors, 2);

        // Delete point 4, counts should decrease by 1
        segment.delete_point(103, 4.into()).unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 1);
        assert_eq!(segment_info.num_vectors, 2); // We don't propagate deletes to vectors at this time

        // // Delete vector of point 6, vector count should now be zero
        // segment
        //     .delete_vector(104, 6.into(), DEFAULT_VECTOR_NAME)
        //     .unwrap();
        // let segment_info = segment.info();
        // assert_eq!(segment_info.num_points, 1);
        // assert_eq!(segment_info.num_vectors, 1);
    }

    #[test]
    fn test_point_vector_count_multivec() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let dim = 1;
        let config = SegmentConfig {
            vector_data: HashMap::from([
                (
                    "a".into(),
                    VectorDataConfig {
                        size: dim,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                        datatype: None,
                    },
                ),
                (
                    "b".into(),
                    VectorDataConfig {
                        size: dim,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                        datatype: None,
                    },
                ),
            ]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config, true).unwrap();

        // Insert point ID 4 and 6 fully, 8 and 10 partially, assert counts
        segment
            .upsert_point(
                100,
                4.into(),
                NamedVectors::from([("a".into(), vec![0.4]), ("b".into(), vec![0.5])]),
            )
            .unwrap();
        segment
            .upsert_point(
                101,
                6.into(),
                NamedVectors::from([("a".into(), vec![0.6]), ("b".into(), vec![0.7])]),
            )
            .unwrap();
        segment
            .upsert_point(102, 8.into(), NamedVectors::from([("a".into(), vec![0.0])]))
            .unwrap();
        segment
            .upsert_point(
                103,
                10.into(),
                NamedVectors::from([("b".into(), vec![1.0])]),
            )
            .unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 6);

        // Delete nonexistent point, counts should remain the same
        segment.delete_point(104, 1.into()).unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 6);

        // Delete point 4, counts should decrease by 1
        segment.delete_point(105, 4.into()).unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 6); // We don't propagate deletes to vectors at this time

        // Delete vector 'a' of point 6, vector count should decrease by 1
        segment.delete_vector(106, 6.into(), "a").unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 5);

        // Deleting it again shouldn't chain anything
        segment.delete_vector(107, 6.into(), "a").unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 5);

        // Replace vector 'a' for point 8, counts should remain the same
        let internal_8 = segment.lookup_internal_id(8.into()).unwrap();
        segment
            .replace_all_vectors(internal_8, NamedVectors::from([("a".into(), vec![0.1])]))
            .unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 5);

        // Replace both vectors for point 8, adding a new vector
        segment
            .replace_all_vectors(
                internal_8,
                NamedVectors::from([("a".into(), vec![0.1]), ("b".into(), vec![0.1])]),
            )
            .unwrap();
        let segment_info = segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 6);
    }

    /// Tests segment functions to ensure invalid requests do error
    #[test]
    fn test_vector_compatibility_checks() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let config = SegmentConfig {
            vector_data: HashMap::from([
                (
                    "a".into(),
                    VectorDataConfig {
                        size: 4,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                        datatype: None,
                    },
                ),
                (
                    "b".into(),
                    VectorDataConfig {
                        size: 2,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                        datatype: None,
                    },
                ),
            ]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config, true).unwrap();

        // Insert one point for a reference internal ID
        let point_id = 4.into();
        segment
            .upsert_point(
                100,
                point_id,
                NamedVectors::from([
                    ("a".into(), vec![0.1, 0.2, 0.3, 0.4]),
                    ("b".into(), vec![1.0, 0.9]),
                ]),
            )
            .unwrap();
        let internal_id = segment.lookup_internal_id(point_id).unwrap();

        // A set of broken vectors
        let wrong_vectors_single = vec![
            // Incorrect dimensionality
            ("a", vec![]),
            ("a", vec![0.0, 1.0, 0.0]),
            ("a", vec![0.0, 1.0, 0.0, 1.0, 0.0]),
            ("b", vec![]),
            ("b", vec![0.5]),
            ("b", vec![0.0, 0.1, 0.2, 0.3]),
            // Incorrect names
            ("aa", vec![0.0, 0.1, 0.2, 0.3]),
            ("bb", vec![0.0, 0.1]),
        ];
        let wrong_vectors_multi = vec![
            // Incorrect dimensionality
            NamedVectors::from_ref("a", [].as_slice().into()),
            NamedVectors::from_ref("a", [0.0, 1.0, 0.0].as_slice().into()),
            NamedVectors::from_ref("a", [0.0, 1.0, 0.0, 1.0, 0.0].as_slice().into()),
            NamedVectors::from_ref("b", [].as_slice().into()),
            NamedVectors::from_ref("b", [0.5].as_slice().into()),
            NamedVectors::from_ref("b", [0.0, 0.1, 0.2, 0.3].as_slice().into()),
            NamedVectors::from([
                ("a".into(), vec![0.1, 0.2, 0.3]),
                ("b".into(), vec![1.0, 0.9]),
            ]),
            NamedVectors::from([
                ("a".into(), vec![0.1, 0.2, 0.3, 0.4]),
                ("b".into(), vec![1.0, 0.9, 0.0]),
            ]),
            // Incorrect names
            NamedVectors::from_ref("aa", [0.0, 0.1, 0.2, 0.3].as_slice().into()),
            NamedVectors::from_ref("bb", [0.0, 0.1].as_slice().into()),
            NamedVectors::from([
                ("aa".into(), vec![0.1, 0.2, 0.3, 0.4]),
                ("b".into(), vec![1.0, 0.9]),
            ]),
            NamedVectors::from([
                ("a".into(), vec![0.1, 0.2, 0.3, 0.4]),
                ("bb".into(), vec![1.0, 0.9]),
            ]),
        ];
        let wrong_names = vec!["aa", "bb", ""];

        for (vector_name, vector) in wrong_vectors_single.iter() {
            let query_vector = vector.to_owned().into();
            check_vector(vector_name, &query_vector, &config)
                .err()
                .unwrap();
            segment
                .search(
                    vector_name,
                    &query_vector,
                    &WithPayload {
                        enable: false,
                        payload_selector: None,
                    },
                    &WithVector::Bool(true),
                    None,
                    1,
                    None,
                )
                .err()
                .unwrap();
            segment
                .search_batch(
                    vector_name,
                    &[&query_vector, &query_vector],
                    &WithPayload {
                        enable: false,
                        payload_selector: None,
                    },
                    &WithVector::Bool(true),
                    None,
                    1,
                    None,
                    &false.into(),
                    10_000,
                )
                .err()
                .unwrap();
        }

        for vectors in wrong_vectors_multi {
            check_named_vectors(&vectors, &config).err().unwrap();
            segment
                .upsert_point(101, point_id, vectors.clone())
                .err()
                .unwrap();
            segment
                .update_vectors(internal_id, vectors.clone())
                .err()
                .unwrap();
            segment
                .insert_new_vectors(point_id, vectors.clone())
                .err()
                .unwrap();
            segment
                .replace_all_vectors(internal_id, vectors.clone())
                .err()
                .unwrap();
        }

        for wrong_name in wrong_names {
            check_vector_name(wrong_name, &config).err().unwrap();
            segment.vector(wrong_name, point_id).err().unwrap();
            segment.vector_dim(wrong_name).err().unwrap();
            segment
                .delete_vector(101, point_id, wrong_name)
                .err()
                .unwrap();
            segment.available_vector_count(wrong_name).err().unwrap();
            segment
                .vector_by_offset(wrong_name, internal_id)
                .err()
                .unwrap();
        }
    }
}
