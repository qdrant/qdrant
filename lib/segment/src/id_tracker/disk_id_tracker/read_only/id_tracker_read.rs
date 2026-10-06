//! The read surface: [`DiskMappingsSource`] and [`IdTrackerRead`] impls.

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt as _;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::UniversalRead;
use itertools::Itertools as _;

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::mappings::{DiskMappingsSource, log_lookup_err};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalRead> DiskMappingsSource for ReadOnlyDiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    /// A single lazy `get_bit` on the on-disk deleted file — never loads the
    /// full set. Out-of-range offsets count as deleted; storage errors
    /// propagate.
    fn point_deleted(
        &self,
        offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Ok(self
            .deleted_file
            .get_bit(u64::from(offset), hw_counter)?
            .unwrap_or(true))
    }

    fn deleted_bitslice(&self, hw_counter: &HardwareCounterCell) -> OperationResult<&BitSlice> {
        Ok(self.deleted_full(hw_counter)?.as_bitslice())
    }

    fn resolve_internal_batch(
        &self,
        external_ids: impl IntoIterator<Item = PointIdType>,
        mut on_live: impl FnMut(PointIdType, PointOffsetType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.reader.lookup_batch(
            external_ids,
            |id, offset| {
                if !self.point_deleted(offset, hw_counter)? {
                    on_live(id, offset);
                }
                Ok(())
            },
            hw_counter,
        )
    }
}

impl<S: UniversalRead> IdTrackerRead for ReadOnlyDiskIdTracker<S> {
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
        hw_counter: &HardwareCounterCell,
    ) -> Option<SeqNumberType> {
        self.versions.get(internal_id, hw_counter)
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

    /// Deleted offsets are dropped before any read is scheduled (the deleted
    /// file is prefetched whole, so each check is an in-memory bit test), then
    /// one pipelined mapping-read pass delivers each surviving `(offset, id)`
    /// in read-completion order; nothing is buffered.
    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        internal_ids
            .into_iter()
            .try_filter(|&offset| OperationResult::Ok(!self.point_deleted(offset, hw_counter)?))
            .process_results(|it| self.reader.external_ids_batch(it, callback, hw_counter))?
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.versions.get_batch(internal_ids, callback, hw_counter)
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

    fn deleted_point_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.mappings_ref_lossy(hw_counter).deleted().count_ones()
    }

    fn deleted_point_bitslice(&self, hw_counter: &HardwareCounterCell) -> &BitSlice {
        self.mappings_ref_lossy(hw_counter).deleted()
    }

    fn is_deleted_point(
        &self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        // Fail-safe on a storage error: treat the point as deleted (hide it).
        self.point_deleted(internal_id, hw_counter)
            .unwrap_or_else(|err| {
                log::error!("disk id tracker deleted check failed: {err}");
                true
            })
    }

    fn name(&self) -> &'static str {
        "read-only disk id tracker"
    }

    fn iter_internal_versions(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        self.versions.iter(hw_counter)
    }
}
