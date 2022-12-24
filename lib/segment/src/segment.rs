use std::collections::HashMap;
use std::fs::{remove_dir_all, rename, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use atomic_refcell::AtomicRefCell;
use fs_extra::dir::{copy_with_progress, CopyOptions, TransitProcess};
use parking_lot::{Mutex, RwLock};
use rocksdb::DB;
use tar::Builder;

use crate::common::file_operations::{atomic_save_json, read_json};
use crate::common::version::StorageVersion;
use crate::common::{check_vector_name, check_vectors_set};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationError::TypeInferenceError;
use crate::entry::entry_point::{
    get_service_error, OperationError, OperationResult, SegmentEntry, SegmentFailedState,
};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndexSS};
use crate::spaces::tools::peek_top_smallest_iterable;
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadIndexInfo, PayloadKeyType, PayloadKeyTypeRef,
    PayloadSchemaType, PointIdType, PointOffsetType, ScoredPoint, SearchParams, SegmentConfig,
    SegmentInfo, SegmentState, SegmentType, SeqNumberType, WithPayload, WithVector,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};

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
    pub vector_index: Arc<AtomicRefCell<VectorIndexSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
}

impl Segment {
    /// Change vector in-place.
    /// WARN: Available for appendable segments only
    fn update_vector(
        &mut self,
        internal_id: PointOffsetType,
        vectors: NamedVectors,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        check_vectors_set(&vectors, &self.segment_config)?;
        for (vector_name, vector) in vectors {
            let vector_name: &str = &vector_name;
            let vector = vector.into_owned();
            let vector_data = &self.vector_data[vector_name];
            let mut vector_storage = vector_data.vector_storage.borrow_mut();
            vector_storage.insert_vector(internal_id, vector)?;
        }
        Ok(())
    }

    /// Operation wrapped, which handles previous and new errors in the segment,
    /// automatically updates versions and skips operations if version is too old
    ///
    /// # Arguments
    ///
    /// * `op_num` - sequential operation of the current operation
    /// * `op_point_offset` - if operation is point-related, specify this point offset.
    ///     If point offset is specified, handler will use point version for comparision.
    ///     Otherwise, it will use global storage version
    /// * `op` - operation to be wrapped. Should return `OperationResult` of bool (which is returned outside)
    ///     and optionally new offset of the changed point.
    ///
    /// # Result
    ///
    /// Propagates `OperationResult` of bool (which is returned in the `op` closure)
    ///
    fn handle_version_and_failure<F>(
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
                return Err(OperationError::service_error(&format!(
                    "Not recovered from previous error: {}",
                    error
                )));
            } // else: Re-try operation
        }

        let res = self.handle_version(op_num, op_point_offset, operation);

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
                    "Segment {:?} operation error: {}",
                    self.current_path.as_path(),
                    error
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

    /// Manage segment version checking
    /// If current version if higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    fn handle_version<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_offset: Option<PointOffsetType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<(bool, Option<PointOffsetType>)>,
    {
        match op_point_offset {
            None => {
                // Not a point operation, use global version to check if already applied
                if self.version > op_num {
                    return Ok(false); // Skip without execution
                }
            }
            Some(point_offset) => {
                // Check if point not exists or have lower version
                if self
                    .id_tracker
                    .borrow()
                    .internal_version(point_offset)
                    .map_or(false, |current_version| current_version > op_num)
                {
                    return Ok(false);
                }
            }
        }

        let res = operation(self);

        if res.is_ok() {
            self.version = op_num;
            if let Ok((_, Some(point_id))) = res {
                self.id_tracker
                    .borrow_mut()
                    .set_internal_version(point_id, op_num)?;
            }
        }
        res.map(|(res, _)| res)
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

    pub fn save_state(state: &SegmentState, current_path: &Path) -> OperationResult<()> {
        let state_path = current_path.join(SEGMENT_STATE_FILE);
        Ok(atomic_save_json(&state_path, state)?)
    }

    pub fn load_state(current_path: &Path) -> OperationResult<SegmentState> {
        let state_path = current_path.join(SEGMENT_STATE_FILE);
        Ok(read_json(&state_path)?)
    }

    /// Retrieve vector by internal ID
    ///
    /// Panics if vector does not exists or deleted
    #[inline]
    fn vector_by_offset(
        &self,
        vector_name: &str,
        point_offset: PointOffsetType,
    ) -> OperationResult<Vec<VectorElementType>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        Ok(vector_data
            .vector_storage
            .borrow()
            .get_vector(point_offset)
            .unwrap())
    }

    fn all_vectors_by_offset(
        &self,
        point_offset: PointOffsetType,
    ) -> OperationResult<NamedVectors> {
        let mut vectors = NamedVectors::default();
        for (vector_name, vector_data) in &self.vector_data {
            vectors.insert(
                vector_name.clone(),
                vector_data
                    .vector_storage
                    .borrow()
                    .get_vector(point_offset)
                    .unwrap(),
            );
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
        let archive_file = File::open(snapshot_path)?;
        let mut ar = tar::Archive::new(archive_file);
        ar.unpack(&segment_path)?;
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
                let vector = match with_vector {
                    WithVector::Bool(false) => None,
                    WithVector::Bool(true) => {
                        Some(self.all_vectors_by_offset(point_offset)?.into())
                    }
                    WithVector::Selector(vectors) => {
                        let mut result = NamedVectors::default();
                        for vector_name in vectors {
                            result.insert(
                                vector_name.clone(),
                                self.vector_by_offset(vector_name, point_offset)?,
                            );
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
                })
            })
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

    pub fn check_consistency(&self) -> OperationResult<()> {
        let id_tracker = self.id_tracker.borrow();
        for (_vector_name, vector_storage) in self.vector_data.iter() {
            let vector_storage = vector_storage.vector_storage.borrow();
            for internal_id in vector_storage.iter_ids() {
                id_tracker.external_id(internal_id).ok_or_else(|| {
                    let payload = self.payload_by_offset(internal_id).unwrap();
                    OperationError::service_error(&format!(
                        "Corrupter id_tracker, no external value for {}, payload: {:?}",
                        internal_id, payload
                    ))
                })?;
            }
        }
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
        let id_tracker = self.id_tracker.borrow();
        id_tracker
            .internal_id(point_id)
            .and_then(|internal_id| id_tracker.internal_version(internal_id))
    }

    fn search(
        &self,
        vector_name: &str,
        vector: &[VectorElementType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let expected_vector_dim = vector_data.vector_storage.borrow().vector_dim();
        if vector.len() != expected_vector_dim {
            return Err(OperationError::WrongVector {
                expected_dim: expected_vector_dim,
                received_dim: vector.len(),
            });
        }

        let internal_result =
            &vector_data
                .vector_index
                .borrow()
                .search(&[vector], filter, top, params)[0];

        self.process_search_result(internal_result, with_payload, with_vector)
    }

    fn search_batch(
        &self,
        vector_name: &str,
        vectors: &[&[VectorElementType]],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let expected_vector_dim = vector_data.vector_storage.borrow().vector_dim();
        for vector in vectors {
            if vector.len() != expected_vector_dim {
                return Err(OperationError::WrongVector {
                    expected_dim: expected_vector_dim,
                    received_dim: vector.len(),
                });
            }
        }

        let internal_results = vector_data
            .vector_index
            .borrow()
            .search(vectors, filter, top, params);

        let res = internal_results
            .iter()
            .map(|internal_result| {
                self.process_search_result(internal_result, with_payload, with_vector)
            })
            .collect();

        res
    }

    fn upsert_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: &NamedVectors,
    ) -> OperationResult<bool> {
        debug_assert!(self.is_appendable());
        check_vectors_set(vectors, &self.segment_config)?;
        let stored_internal_point = self.id_tracker.borrow().internal_id(point_id);
        self.handle_version_and_failure(op_num, stored_internal_point, |segment| {
            let mut processed_vectors = NamedVectors::default();
            for (vector_name, vector) in vectors.iter() {
                let vector_name: &str = vector_name;
                let vector: &[VectorElementType] = vector;
                let vector_data = &segment.vector_data[vector_name];
                let vector_dim = vector_data.vector_storage.borrow().vector_dim();
                if vector_dim != vector.len() {
                    return Err(OperationError::WrongVector {
                        expected_dim: vector_dim,
                        received_dim: vector.len(),
                    });
                }

                let processed_vector_opt = segment.segment_config.vector_data[vector_name]
                    .distance
                    .preprocess_vector(vector);
                match processed_vector_opt {
                    None => processed_vectors.insert_ref(vector_name, vector),
                    Some(preprocess_vector) => {
                        processed_vectors.insert(vector_name.to_string(), preprocess_vector)
                    }
                }
            }

            if let Some(existing_internal_id) = stored_internal_point {
                segment.update_vector(existing_internal_id, processed_vectors)?;
                Ok((true, Some(existing_internal_id)))
            } else {
                let new_index = segment.id_tracker.borrow().internal_size() as PointOffsetType;

                for (vector_name, processed_vector) in processed_vectors {
                    let vector_name: &str = &vector_name;
                    let processed_vector = processed_vector.into_owned();
                    segment.vector_data[vector_name]
                        .vector_storage
                        .borrow_mut()
                        .insert_vector(new_index, processed_vector)?;
                }
                segment
                    .id_tracker
                    .borrow_mut()
                    .set_link(point_id, new_index)?;
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
            None => Ok(false), // Point already not exists
            Some(internal_id) => {
                self.handle_version_and_failure(op_num, Some(internal_id), |segment| {
                    for vector_data in segment.vector_data.values() {
                        vector_data
                            .vector_storage
                            .borrow_mut()
                            .delete(internal_id)?;
                    }
                    segment.payload_index.borrow_mut().drop(internal_id)?;
                    segment.id_tracker.borrow_mut().drop(point_id)?;
                    Ok((true, Some(internal_id)))
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
        let internal_id = self.lookup_internal_id(point_id)?;
        self.handle_version_and_failure(op_num, Some(internal_id), |segment| {
            segment
                .payload_index
                .borrow_mut()
                .assign_all(internal_id, full_payload)?;
            Ok((true, Some(internal_id)))
        })
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
    ) -> OperationResult<bool> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.handle_version_and_failure(op_num, Some(internal_id), |segment| {
            segment
                .payload_index
                .borrow_mut()
                .assign(internal_id, payload)?;
            Ok((true, Some(internal_id)))
        })
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.handle_version_and_failure(op_num, Some(internal_id), |segment| {
            segment
                .payload_index
                .borrow_mut()
                .delete(internal_id, key)?;
            Ok((true, Some(internal_id)))
        })
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.handle_version_and_failure(op_num, Some(internal_id), |segment| {
            segment.payload_index.borrow_mut().drop(internal_id)?;
            Ok((true, Some(internal_id)))
        })
    }

    fn vector(
        &self,
        vector_name: &str,
        point_id: PointIdType,
    ) -> OperationResult<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.vector_by_offset(vector_name, internal_id)
    }

    fn all_vectors(&self, point_id: PointIdType) -> OperationResult<NamedVectors> {
        let mut result = NamedVectors::default();
        for vector_name in self.vector_data.keys() {
            result.insert(vector_name.clone(), self.vector(vector_name, point_id)?);
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
            None => self
                .id_tracker
                .borrow()
                .iter_from(offset)
                .map(|x| x.0)
                .take(limit.unwrap_or(usize::MAX))
                .collect(),
            Some(condition) => {
                let query_cardinality = {
                    let payload_index = self.payload_index.borrow();
                    payload_index.estimate_cardinality(condition)
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

                let total_points = self.points_count() + 1 /* + 1 for division-by-zero */;
                // Expected number of successful checks per point
                let check_probability = (query_cardinality.exp as f64 + 1.0/* protect from zero */)
                    / total_points as f64;
                let exp_stream_checks =
                    (limit.unwrap_or(total_points) as f64 / check_probability) as usize;

                // Assume it would require about `query cardinality` checks.
                // We are interested in approximate number of checks, so we can
                // use `query cardinality` as a starting point.
                let exp_index_checks = query_cardinality.max;

                if exp_stream_checks > exp_index_checks {
                    self.filtered_read_by_index(offset, limit, condition)
                } else {
                    self.filtered_read_by_id_stream(offset, limit, condition)
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
        let vector_data = self.vector_data.values().next();
        if let Some(vector_data) = vector_data {
            vector_data.vector_storage.borrow().deleted_count()
        } else {
            0
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

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.points_count() * self.vector_data.len(),
            num_points: self.points_count(),
            num_deleted_vectors: self.deleted_count(),
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

    fn flush(&self, sync: bool) -> OperationResult<SeqNumberType> {
        let current_persisted_version: SeqNumberType = *self.persisted_version.lock();
        if !sync && self.is_background_flushing() {
            return Ok(current_persisted_version);
        }

        let mut background_flush_lock = self.lock_flushing()?;
        if current_persisted_version == self.version() {
            return Ok(current_persisted_version);
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
        let flush_op = move || {
            log::debug!("id_tracker_mapping_flusher {}", self.current_path.display());

            // Flush mapping first to prevent having orphan internal ids.
            id_tracker_mapping_flusher().map_err(|err| {
                OperationError::service_error(&format!(
                    "Failed to flush id_tracker mapping: {}",
                    err
                ))
            })?;

            log::debug!("vector_storage_flushers {}", self.current_path.display());

            for vector_storage_flusher in vector_storage_flushers {
                vector_storage_flusher().map_err(|err| {
                    OperationError::service_error(&format!(
                        "Failed to flush vector_storage: {}",
                        err
                    ))
                })?;
            }

            log::debug!("payload_index_flusher {}", self.current_path.display());
            payload_index_flusher().map_err(|err| {
                OperationError::service_error(&format!("Failed to flush payload_index: {}", err))
            })?;
            // Id Tracker contains versions of points. We need to flush it after vector_storage and payload_index flush.
            // This is because vector_storage and payload_index flush are not atomic.
            // If payload or vector flush fails, we will be able to recover data from WAL.
            // If Id Tracker flush fails, we are also able to recover data from WAL
            //  by simply overriding data in vector and payload storages.
            // Once versions are saved - points are considered persisted.
            log::debug!(
                "id_tracker_versions_flusher {}",
                self.current_path.display()
            );
            id_tracker_versions_flusher().map_err(|err| {
                OperationError::service_error(&format!(
                    "Failed to flush id_tracker versions: {}",
                    err
                ))
            })?;

            log::debug!("save_state {}", self.current_path.display());
            Self::save_state(&state, &current_path).map_err(|err| {
                OperationError::service_error(&format!("Failed to flush segment state: {}", err))
            })?;
            *persisted_version.lock() = state.version;

            Ok(state.version)
        };

        if sync {
            flush_op()
        } else {
            *background_flush_lock = Some(
                std::thread::Builder::new()
                    .name("background_flush".to_string())
                    .spawn(flush_op)
                    .unwrap(),
            );
            Ok(current_persisted_version)
        }
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
            Ok((true, None))
        })
    }

    fn create_field_index(
        &mut self,
        op_num: u64,
        key: PayloadKeyTypeRef,
        field_type: Option<&PayloadFieldSchema>,
    ) -> OperationResult<bool> {
        self.handle_version_and_failure(op_num, None, |segment| match field_type {
            Some(schema) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .set_indexed(key, schema.clone())?;
                Ok((true, None))
            }
            None => match segment.infer_from_payload_data(key)? {
                None => Err(TypeInferenceError {
                    field_name: key.to_string(),
                }),
                Some(schema_type) => {
                    segment
                        .payload_index
                        .borrow_mut()
                        .set_indexed(key, schema_type.into())?;
                    Ok((true, None))
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
        let vector_data_config = &self.segment_config.vector_data[vector_name];
        Ok(vector_data_config.size)
    }

    fn vector_dims(&self) -> HashMap<String, usize> {
        self.segment_config
            .vector_data
            .iter()
            .map(|(vector_name, vector_config)| (vector_name.clone(), vector_config.size))
            .collect()
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
        self.flush(true)?;
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
        self.flush(true)?;

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

    fn get_telemetry_data(&self) -> SegmentTelemetry {
        let vector_index_searches: Vec<_> = self
            .vector_data
            .iter()
            .map(|(k, v)| {
                let mut telemetry = v.vector_index.borrow().get_telemetry_data();
                telemetry.index_name = Some(k.clone());
                telemetry
            })
            .collect();

        SegmentTelemetry {
            info: self.info(),
            config: self.config(),
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
    use std::fs;

    use tar::Archive;
    use tempfile::Builder;
    use walkdir::WalkDir;

    use super::*;
    use crate::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
    use crate::segment_constructor::build_segment;
    use crate::types::{Distance, Indexes, SegmentConfig, StorageType, VectorDataConfig};

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
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };
        let mut segment = build_segment(dir.path(), &config).unwrap();

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        segment
            .upsert_vector(100, 4.into(), &only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        segment
            .upsert_vector(101, 6.into(), &only_default_vector(&vec6))
            .unwrap();
        segment.delete_point(102, 1.into()).unwrap();

        let query_vector = vec![1.0, 1.0, 1.0, 1.0];
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
        eprintln!("search_result = {:#?}", search_result);

        let search_batch_result = segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query_vector],
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();
        eprintln!("search_batch_result = {:#?}", search_batch_result);

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
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment
            .upsert_vector(0, 0.into(), &only_default_vector(&[1.0, 1.0]))
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
                &[1.0, 1.0],
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
                &[1.0, 1.0],
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
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config).unwrap();
        segment
            .upsert_vector(0, 0.into(), &only_default_vector(&[1.0, 1.0]))
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush(true).unwrap();

        // Recursively count all files and folders in directory and subdirectories:
        let segment_file_count = WalkDir::new(segment.current_path.clone())
            .into_iter()
            .count();
        assert_eq!(segment_file_count, 20);

        let snapshot_dir = Builder::new().prefix("snapshot_dir").tempdir().unwrap();

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
        let snapshot_decompress_dir = Builder::new()
            .prefix("snapshot_decompress_dir")
            .tempdir()
            .unwrap();
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

        let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: 2,
                    distance: Distance::Dot,
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config).unwrap();
        segment
            .upsert_vector(0, 0.into(), &only_default_vector(&[1.0, 1.0]))
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush(true).unwrap();

        // Recursively count all files and folders in directory and subdirectories.
        let segment_file_count = WalkDir::new(segment.current_path.clone())
            .into_iter()
            .count();
        assert_eq!(segment_file_count, 20);

        let segment_copy_dir = Builder::new().prefix("segment_copy_dir").tempdir().unwrap();
        let full_copy_path = segment
            .copy_segment_directory(segment_copy_dir.path())
            .unwrap();

        // Recursively count all files and folders in directory and subdirectories.
        let copy_segment_file_count = WalkDir::new(full_copy_path).into_iter().count();

        assert_eq!(copy_segment_file_count, segment_file_count);
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
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(segment_base_dir.path(), &config).unwrap();
        segment
            .upsert_vector(0, 0.into(), &only_default_vector(&[1.0, 1.0]))
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();
        segment.set_full_payload(0, 0.into(), &payload).unwrap();
        segment.flush(false).unwrap();

        // call flush second time to check that background flush finished successful
        segment.flush(true).unwrap();
    }
}
