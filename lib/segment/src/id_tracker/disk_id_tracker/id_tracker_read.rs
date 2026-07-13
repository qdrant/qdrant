//! The read surface: [`DiskMappingsSource`] and [`IdTrackerRead`] impls.

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::UniversalWrite;

use super::DiskIdTracker;
use super::mappings::{
    DiskMappingsSource, log_lookup_err, log_lookup_err_batch, resolve_external_ids_batch,
};
use super::reader::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalWrite + Send + Sync + 'static> DiskMappingsSource for DiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    fn point_deleted(&self, offset: PointOffsetType) -> OperationResult<bool> {
        // Resident bitvec, so this never fails; out-of-range offsets are treated
        // as deleted (mirrors the in-RAM tracker).
        //
        // `points_deleted_batch` is deliberately NOT overridden: its default loop
        // hits this resident bitvec, so there is no IO to pipeline. Only the
        // read-only tracker, whose deleted set stays on disk, batches it.
        Ok(self.deleted.get_bit(offset as usize).unwrap_or(true))
    }

    fn deleted_bitslice(&self) -> OperationResult<&BitSlice> {
        // Resident bitvec, so this never fails.
        Ok(&self.deleted)
    }
}

impl<S: UniversalWrite + Send + Sync + 'static> IdTrackerRead for DiskIdTracker<S> {
    type Backend = S;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Disk(self.mappings_ref_lossy())
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        // Resident versions, mutated in place. `internal_versions_batch` is
        // deliberately NOT overridden: its default loop hits this resident
        // store, so there is no IO to pipeline. Only the read-only tracker,
        // whose versions file stays on disk, batches it.
        self.internal_to_version.get(internal_id)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: DeferredBehavior,
    ) -> Option<PointOffsetType> {
        log_lookup_err(self.resolve_internal(external_id))
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        log_lookup_err(self.resolve_external(internal_id))
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
    ) -> Vec<Option<PointIdType>> {
        let internal_ids: Vec<PointOffsetType> = internal_ids.into_iter().collect();
        log_lookup_err_batch(
            self.resolve_external_batch(&internal_ids),
            internal_ids.len(),
        )
    }

    /// Batched external→internal resolution; the behavior argument is ignored
    /// (as in [`internal_id_with_behavior`](IdTrackerRead::internal_id_with_behavior)).
    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        _deferred_behavior: DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) {
        resolve_external_ids_batch(self, point_ids, callback)
    }

    fn total_point_count(&self) -> usize {
        self.reader.total_point_count() as usize
    }

    fn deleted_point_count(&self) -> usize {
        self.deleted.count_ones()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        &self.deleted
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        // Resident bitvec, so this never actually errors.
        self.point_deleted(key).unwrap_or(true)
    }

    fn name(&self) -> &'static str {
        "disk id tracker"
    }

    fn iter_internal_versions(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        Ok(Box::new(self.internal_to_version.iter()))
    }
}
