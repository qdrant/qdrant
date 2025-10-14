use std::cmp::max;
use std::collections::HashMap;
use std::path::Path;

use bitvec::prelude::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fs_err as fs;
use io::file_operations::{atomic_save_json, read_json};

use super::{SEGMENT_STATE_FILE, SNAPSHOT_FILES_PATH, SNAPSHOT_PATH, Segment};
use crate::common::operation_error::{
    OperationError, OperationResult, SegmentFailedState, get_service_error,
};
use crate::common::validate_snapshot_archive::open_snapshot_archive_with_validation;
use crate::common::{check_named_vectors, check_vector_name};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorInternal;
use crate::entry::entry_point::SegmentEntry;
use crate::index::{PayloadIndex, VectorIndex};
use crate::types::{
    Payload, PayloadFieldSchema, PayloadKeyType, PointIdType, SegmentState, SeqNumberType,
    SnapshotFormat, VectorName,
};
use crate::utils;
use crate::vector_storage::{Random, VectorStorage};

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
    pub(super) fn replace_all_vectors(
        &mut self,
        internal_id: PointOffsetType,
        op_num: SeqNumberType,
        vectors: &NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        check_named_vectors(vectors, &self.segment_config)?;
        for (vector_name, vector_data) in self.vector_data.iter_mut() {
            let vector = vectors.get(vector_name);
            let mut vector_index = vector_data.vector_index.borrow_mut();
            vector_index.update_vector(internal_id, vector, hw_counter)?;
            self.version_tracker.set_vector(vector_name, Some(op_num));
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
    #[allow(clippy::needless_pass_by_ref_mut)] // ensure single access to AtomicRefCell vector_index
    pub(super) fn update_vectors(
        &mut self,
        internal_id: PointOffsetType,
        op_num: SeqNumberType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        for (vector_name, new_vector) in vectors {
            let vector_data = &self.vector_data[vector_name.as_ref()];
            let mut vector_index = vector_data.vector_index.borrow_mut();
            vector_index.update_vector(internal_id, Some(new_vector.as_vec_ref()), hw_counter)?;
            self.version_tracker.set_vector(&vector_name, Some(op_num));
        }
        Ok(())
    }

    /// Insert new vectors into the segment
    ///
    /// # Warning
    ///
    /// Available for appendable segments only.
    pub(super) fn insert_new_vectors(
        &mut self,
        point_id: PointIdType,
        op_num: SeqNumberType,
        vectors: &NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<PointOffsetType> {
        debug_assert!(self.is_appendable());
        check_named_vectors(vectors, &self.segment_config)?;
        let new_index = self.id_tracker.borrow().total_point_count() as PointOffsetType;
        for (vector_name, vector_data) in self.vector_data.iter_mut() {
            let vector_opt = vectors.get(vector_name);
            let mut vector_index = vector_data.vector_index.borrow_mut();
            vector_index.update_vector(new_index, vector_opt, hw_counter)?;
            self.version_tracker.set_vector(vector_name, Some(op_num));
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
    ///   and optionally new offset of the changed point.
    ///
    /// # Result
    ///
    /// Propagates `OperationResult` of bool (which is returned in the `op` closure)
    pub(super) fn handle_segment_version_and_failure<F>(
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
    ///   Otherwise, it will be applied without version checks.
    /// * `op` - operation to be wrapped. Should return `OperationResult` of bool (which is returned outside) and optionally new offset of the changed point.
    ///
    /// # Result
    ///
    /// Propagates `OperationResult` of bool (which is returned in the `op` closure)
    pub(super) fn handle_point_version_and_failure<F>(
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
    /// If current version is higher than operation version - do not perform the operation
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
    /// If current version is higher than operation version - do not perform the operation
    /// Update current version if operation successfully executed
    pub(super) fn handle_point_version<F>(
        &mut self,
        op_num: SeqNumberType,
        op_point_offset: Option<PointOffsetType>,
        operation: F,
    ) -> OperationResult<bool>
    where
        F: FnOnce(&mut Segment) -> OperationResult<(bool, Option<PointOffsetType>)>,
    {
        // If point does not exist or has lower version, ignore operation
        if let Some(point_offset) = op_point_offset
            && self
                .id_tracker
                .borrow()
                .internal_version(point_offset)
                .is_some_and(|current_version| current_version > op_num)
        {
            return Ok(false);
        }

        let (applied, internal_id) = operation(self)?;

        self.bump_segment_version(op_num);
        if let Some(internal_id) = internal_id {
            self.id_tracker
                .borrow_mut()
                .set_internal_version(internal_id, op_num)?;
        }

        Ok(applied)
    }

    pub fn delete_point_internal(
        &mut self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Mark point as deleted, drop mapping
        self.payload_index
            .borrow_mut()
            .clear_payload(internal_id, hw_counter)?;

        self.id_tracker.borrow_mut().drop_internal(internal_id)?;

        // Before, we propagated point deletions to also delete its vectors. This turns
        // out to be problematic because this sometimes makes us lose vector data
        // because we cannot control the order of segment flushes.
        // Disabled until we properly fix it or find a better way to clean up old
        // vectors.
        //
        // // Propagate point deletion to all its vectors
        // for vector_data in segment.vector_data.values() {
        //     let mut vector_storage = vector_data.vector_storage.borrow_mut();
        //     vector_storage.delete_vector(internal_id)?;
        // }

        Ok(())
    }

    fn bump_segment_version(&mut self, op_num: SeqNumberType) {
        self.version.replace(max(op_num, self.version.unwrap_or(0)));
    }

    pub fn get_internal_id(&self, point_id: PointIdType) -> Option<PointOffsetType> {
        self.id_tracker.borrow().internal_id(point_id)
    }

    pub fn get_deleted_points_bitvec(&self) -> BitVec {
        BitVec::from(self.id_tracker.borrow().deleted_point_bitslice())
    }

    pub(super) fn lookup_internal_id(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<PointOffsetType> {
        let internal_id_opt = self.id_tracker.borrow().internal_id(point_id);
        match internal_id_opt {
            Some(internal_id) => Ok(internal_id),
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        }
    }

    pub(super) fn get_state(&self) -> SegmentState {
        SegmentState {
            initial_version: self.initial_version,
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
    pub(super) fn vector_by_offset(
        &self,
        vector_name: &VectorName,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let vector_storage = vector_data.vector_storage.borrow();
        let is_vector_deleted = vector_storage.is_deleted_vector(point_offset);
        if !is_vector_deleted && !self.id_tracker.borrow().is_deleted_point(point_offset) {
            if vector_storage.total_vector_count() <= point_offset as usize {
                // Storage does not have vector with such offset.
                // This is possible if the storage is inconsistent due to interrupted flush.
                // Assume consistency will be restored with WAL replay.

                // Without this check, the service will panic on the `get_vector` call.
                Err(OperationError::InconsistentStorage {
                    description: format!(
                        "Vector storage '{}' is inconsistent, total_vector_count: {}, point_offset: {}",
                        vector_name,
                        vector_storage.total_vector_count(),
                        point_offset
                    ),
                })
            } else {
                let vector = vector_storage.get_vector::<Random>(point_offset);
                if vector_storage.is_on_disk() {
                    hw_counter
                        .vector_io_read()
                        .incr_delta(vector.estimate_size_in_bytes());
                }
                Ok(Some(vector.to_owned()))
            }
        } else {
            Ok(None)
        }
    }

    pub(super) fn all_vectors_by_offset(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        let mut vectors = NamedVectors::default();
        for vector_name in self.vector_data.keys() {
            if let Some(vector) = self.vector_by_offset(vector_name, point_offset, hw_counter)? {
                vectors.insert(vector_name.clone(), vector);
            }
        }
        Ok(vectors)
    }

    /// Retrieve payload by internal ID
    #[inline]
    pub(super) fn payload_by_offset(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload_index
            .borrow()
            .get_payload(point_offset, hw_counter)
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        Self::save_state(&self.get_state(), &self.current_path)
    }

    /// Unpacks and restores the segment snapshot in-place. The original
    /// snapshot is destroyed in the process.
    ///
    /// Both of the following calls would result in a directory
    /// `foo/bar/segment-id/` with the segment data:
    ///
    /// - `segment.restore_snapshot("foo/bar/segment-id.tar")`  (tar archive)
    /// - `segment.restore_snapshot("foo/bar/segment-id")`      (directory)
    pub fn restore_snapshot_in_place(snapshot_path: &Path) -> OperationResult<()> {
        restore_snapshot_in_place(snapshot_path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to restore snapshot from {snapshot_path:?}: {err}",
            ))
        })
    }

    /// Check consistency of the segment's data and repair it if possible.
    /// Removes partially persisted points.
    pub fn check_consistency_and_repair(&mut self) -> OperationResult<()> {
        // Get rid of versionless points.
        let ids_to_clean = self.fix_id_tracker_inconsistencies()?;

        // There are some leftovers to clean from segment.
        // After that we need to set internal version to 0, so that
        // we won't need to clean them again.

        // This is internal operation, no hw measurement needed
        let disposable_hw_counter = HardwareCounterCell::disposable();
        if !ids_to_clean.is_empty() {
            log::debug!("cleaning up {} points without version", ids_to_clean.len());

            for internal_id in ids_to_clean {
                self.delete_point_internal(internal_id, &disposable_hw_counter)?;
            }

            self.flush(true)?;

            // We do not drop version here, because it is already not loaded into memory.
            // There are no explicit mapping between internal ID and version, so all dangling
            // versions will be ignored automatically.
            // Those versions could be overwritten by new points, but it is not a problem.
            // They will also be deleted by the next optimization.
        }

        Ok(())
    }

    /// Update all payload/field indices to match `desired_schemas`
    ///
    /// Missing payload indices are created. Incorrectly configured payload indices are recreated.
    /// Extra payload indices are NOT deleted.
    ///
    /// This does nothing if the current payload indices state matches `desired_schemas` exactly.
    pub fn update_all_field_indices(
        &mut self,
        desired_schemas: &HashMap<PayloadKeyType, PayloadFieldSchema>,
    ) -> OperationResult<()> {
        let schema_applied = self.payload_index.borrow().indexed_fields();
        let schema_config = desired_schemas;

        // Create or update payload indices if they don't match configuration
        for (key, schema) in schema_config {
            match schema_applied.get(key) {
                Some(existing_schema) if existing_schema == schema => continue,
                Some(existing_schema) => log::warn!(
                    "Segment has incorrect payload index for {key}, recreating it now (current: {:?}, configured: {:?})",
                    existing_schema.name(),
                    schema.name(),
                ),
                None => log::warn!(
                    "Segment is missing a {} payload index for {key}, creating it now",
                    schema.name(),
                ),
            }

            let created = self.create_field_index(
                self.version(),
                key,
                Some(schema),
                &HardwareCounterCell::disposable(), // This function is only used in Segment::load which is unmeasured.
            )?;
            if !created {
                log::warn!("Failed to create payload index for {key} in segment");
            }
        }

        // Do not delete extra payload indices, because collection-level information about
        // the payload indices might be incomplete due to migrations from older versions.

        Ok(())
    }

    /// Check data consistency of the segment on its own
    /// - internal id without external id
    /// - external id without internal
    /// - internal id without version
    /// - internal id without vector
    ///
    /// A shard can still be consistent with an inconsistent segment as points are merged based on their version.
    ///
    /// Returns an error if any inconsistency is found
    pub fn check_data_consistency(&self) -> OperationResult<()> {
        let id_tracker = self.id_tracker.borrow();

        // dangling internal ids
        let mut has_dangling_internal_ids = false;
        for internal_id in id_tracker.iter_ids() {
            if id_tracker.external_id(internal_id).is_none() {
                log::error!("Internal id {internal_id} without external id");
                has_dangling_internal_ids = true
            }
        }

        // dangling external ids
        let mut has_dangling_external_ids = false;
        for external_id in id_tracker.iter_external() {
            if id_tracker.internal_id(external_id).is_none() {
                log::error!("External id {external_id} without internal id");
                has_dangling_external_ids = true;
            }
        }

        // checking internal id without version
        let mut has_internal_ids_without_version = false;
        for internal_id in id_tracker.iter_ids() {
            if id_tracker.internal_version(internal_id).is_none() {
                log::error!("Internal id {internal_id} without version");
                has_internal_ids_without_version = true;
            }
        }

        // check that non deleted points exist in vector storage
        let mut has_internal_ids_without_vector = false;
        for internal_id in id_tracker.iter_ids() {
            for (vector_name, vector_data) in &self.vector_data {
                let vector_storage = vector_data.vector_storage.borrow();
                let is_vector_deleted_storage = vector_storage.is_deleted_vector(internal_id);
                let is_vector_deleted_tracker = id_tracker.is_deleted_point(internal_id);
                let vector_stored = vector_storage.get_vector_opt::<Random>(internal_id);
                if !is_vector_deleted_storage
                    && !is_vector_deleted_tracker
                    && vector_stored.is_none()
                {
                    let point_id = id_tracker.external_id(internal_id);
                    let point_version = id_tracker.internal_version(internal_id);
                    // ignoring initial version because the WAL replay can resurrect un-flushed points by assigning them a new initial version
                    // those points will be deleted by the next deduplication process
                    if point_version != Some(0) {
                        log::error!(
                            "Vector storage '{vector_name}' is missing point {point_id:?} point_offset: {internal_id} version: {point_version:?}",
                        );
                        has_internal_ids_without_vector = true;
                    }
                }
            }
        }

        let is_inconsistent = has_dangling_internal_ids
            || has_dangling_external_ids
            || has_internal_ids_without_version
            || has_internal_ids_without_vector;

        if is_inconsistent {
            Err(OperationError::service_error(
                "Inconsistent segment data detected",
            ))
        } else {
            Ok(())
        }
    }

    pub fn available_vector_count(&self, vector_name: &VectorName) -> OperationResult<usize> {
        check_vector_name(vector_name, &self.segment_config)?;
        Ok(self.vector_data[vector_name]
            .vector_storage
            .borrow()
            .available_vector_count())
    }

    pub fn total_point_count(&self) -> usize {
        self.id_tracker.borrow().total_point_count()
    }

    /// Fixes inconsistencies in the ID tracker, if any.
    /// Returns list of IDs, which should be removed from segment
    pub fn fix_id_tracker_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        self.id_tracker.borrow_mut().fix_inconsistencies()
    }
}

fn restore_snapshot_in_place(snapshot_path: &Path) -> OperationResult<()> {
    let segments_dir = snapshot_path
        .parent()
        .ok_or_else(|| OperationError::service_error("Cannot extract parent path"))?;

    let file_name = snapshot_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            OperationError::service_error("Cannot extract segment ID from snapshot path")
        })?;

    let meta = fs::metadata(snapshot_path)?;
    let (segment_id, is_tar) = match file_name.split_once('.') {
        Some((segment_id, "tar")) if meta.is_file() => (segment_id, true),
        None if meta.is_dir() => (file_name, false),
        _ => {
            return Err(OperationError::service_error(
                "Invalid snapshot path, expected either a directory or a .tar file",
            ));
        }
    };

    if !is_tar {
        log::debug!(
            "Extracting segment {} from {:?} snapshot",
            segment_id,
            SnapshotFormat::Streamable
        );
        unpack_snapshot(snapshot_path)?;
    } else {
        let segment_path = segments_dir.join(segment_id);
        open_snapshot_archive_with_validation(snapshot_path)?.unpack(&segment_path)?;

        let inner_path = segment_path.join(SNAPSHOT_PATH);
        if inner_path.is_dir() {
            log::debug!(
                "Extracting segment {} from {:?} snapshot",
                segment_id,
                SnapshotFormat::Regular
            );
            unpack_snapshot(&inner_path)?;
            utils::fs::move_all(&inner_path, &segment_path)?;
            fs::remove_dir(&inner_path)?;
        } else {
            log::debug!(
                "Extracting segment {} from {:?} snapshot",
                segment_id,
                SnapshotFormat::Ancient
            );
            // Do nothing, this format is just a plain archive.
        }

        fs::remove_file(snapshot_path)?;
    }

    Ok(())
}

fn unpack_snapshot(segment_path: &Path) -> OperationResult<()> {
    #[cfg(feature = "rocksdb")]
    {
        use super::{DB_BACKUP_PATH, PAYLOAD_DB_BACKUP_PATH};
        use crate::index::struct_payload_index::StructPayloadIndex;

        let db_backup_path = segment_path.join(DB_BACKUP_PATH);
        if db_backup_path.is_dir() {
            crate::rocksdb_backup::restore(&db_backup_path, segment_path)?;
            fs::remove_dir_all(&db_backup_path)?;
        }

        let payload_index_db_backup = segment_path.join(PAYLOAD_DB_BACKUP_PATH);
        if payload_index_db_backup.is_dir() {
            StructPayloadIndex::restore_database_snapshot(&payload_index_db_backup, segment_path)?;
            fs::remove_dir_all(&payload_index_db_backup)?;
        }
    }

    let files_path = segment_path.join(SNAPSHOT_FILES_PATH);
    utils::fs::move_all(&files_path, segment_path)?;
    fs::remove_dir(&files_path)?;

    Ok(())
}
