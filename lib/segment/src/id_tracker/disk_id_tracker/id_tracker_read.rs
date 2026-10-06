//! The read surface: [`DiskMappingsSource`] and [`IdTrackerRead`] impls.

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::UniversalWrite;

use super::DiskIdTracker;
use super::mappings::{DiskMappingsSource, log_lookup_err};
use super::reader::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum, default_internal_versions_batch};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalWrite> DiskIdTracker<S> {
    /// Infallible counterpart of [`DiskMappingsSource::point_deleted`]: the
    /// deleted bitvec is resident. Out-of-range offsets are treated as deleted
    /// (mirrors the in-RAM tracker).
    fn point_deleted(&self, offset: PointOffsetType) -> bool {
        self.deleted.get_bit(offset as usize).unwrap_or(true)
    }
}

impl<S: UniversalWrite + Send + Sync + 'static> DiskMappingsSource for DiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    fn point_deleted(
        &self,
        offset: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Ok(self.point_deleted(offset))
    }

    fn deleted_bitslice(&self, _hw_counter: &HardwareCounterCell) -> OperationResult<&BitSlice> {
        // Resident bitvec, so this never fails.
        Ok(&self.deleted)
    }

    fn resolve_internal_batch(
        &self,
        external_ids: impl IntoIterator<Item = PointIdType>,
        mut on_live: impl FnMut(PointIdType, PointOffsetType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // The deleted bitvec is resident, so the per-point check is infallible.
        self.reader.lookup_batch(
            external_ids,
            |id, offset| {
                if !self.point_deleted(offset) {
                    on_live(id, offset);
                }
                Ok(())
            },
            hw_counter,
        )
    }
}

impl<S: UniversalWrite + Send + Sync + 'static> IdTrackerRead for DiskIdTracker<S> {
    type Backend = S;

    fn point_mappings(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Disk(self.mappings_ref_lossy(hw_counter))
    }

    fn internal_version(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<SeqNumberType> {
        // Resident versions, mutated in place.
        self.internal_to_version.get(internal_id)
    }

    /// The default single-lookup loop hits the resident version store, so
    /// there is no IO to pipeline. Only the read-only tracker, whose versions
    /// file stays on disk, batches the reads.
    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        default_internal_versions_batch(self, internal_ids, callback, hw_counter)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> Option<PointOffsetType> {
        log_lookup_err(self.resolve_internal(external_id, hw_counter))
    }

    fn external_id(
        &self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<PointIdType> {
        log_lookup_err(self.resolve_external(internal_id, hw_counter))
    }

    /// Deleted offsets are dropped against the resident bitvec, then one
    /// pipelined mapping-read pass delivers each surviving `(offset, id)` in
    /// read-completion order; nothing is buffered.
    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.reader.external_ids_batch(
            internal_ids
                .into_iter()
                .filter(|&offset| !self.point_deleted(offset)),
            callback,
            hw_counter,
        )
    }

    /// Batched external→internal resolution; the behavior argument is ignored
    /// (as in [`internal_id_with_behavior`](IdTrackerRead::internal_id_with_behavior)).
    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        _deferred_behavior: DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.resolve_internal_batch(point_ids, callback, hw_counter)
    }

    fn total_point_count(&self) -> usize {
        self.reader.total_point_count() as usize
    }

    fn deleted_point_count(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.deleted.count_ones()
    }

    fn deleted_point_bitslice(&self, _hw_counter: &HardwareCounterCell) -> &BitSlice {
        &self.deleted
    }

    fn is_deleted_point(&self, key: PointOffsetType, _hw_counter: &HardwareCounterCell) -> bool {
        self.point_deleted(key)
    }

    fn name(&self) -> &'static str {
        "disk id tracker"
    }

    fn iter_internal_versions(
        &self,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        Ok(Box::new(self.internal_to_version.iter()))
    }
}
