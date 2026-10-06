use std::path::Path;

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, Populate, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
};
use futures::future::BoxFuture;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::ReadOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::{
    LiveReloadResult, ReadOnlyAppendableIdTracker, TrackerProbe,
};
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

pub enum ReadOnlyIdTrackerEnum<S: UniversalRead> {
    Appendable(ReadOnlyAppendableIdTracker<S>),
    Immutable(ReadOnlyImmutableIdTracker<S>),
    DiskResident(ReadOnlyDiskIdTracker<S>),
}

impl<S: UniversalRead> ReadOnlyIdTrackerEnum<S> {
    /// Schedule background prefetch for whichever id-tracker format is
    /// present, probing in the same order as [`Self::detect_and_load`].
    /// `populate` applies to the disk-resident format only: the other formats
    /// hold their per-point data in RAM regardless.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if ReadOnlyDiskIdTracker::try_preopen(fs, segment_path, populate, hw_counter)? {
            return Ok(());
        }
        if ReadOnlyImmutableIdTracker::try_preopen(fs, segment_path, hw_counter)? {
            return Ok(());
        }
        ReadOnlyAppendableIdTracker::preopen(fs, segment_path, hw_counter);
        Ok(())
    }

    /// Detect the persisted id-tracker format and load it, by *attempting* each
    /// format's open.
    ///
    /// Order: disk-resident (the serverless/object-storage format) first, then
    /// the in-RAM immutable format, then the appendable/mutable format (whose
    /// open tolerates absent files, i.e. a fresh or empty segment).
    /// `populate` applies to the disk-resident format only, see [`Self::preopen`].
    pub fn detect_and_load(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        deferred_internal_id: Option<PointOffsetType>,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        if let Some(tracker) =
            ReadOnlyDiskIdTracker::try_open(fs, segment_path, populate, hw_counter)?
        {
            return Ok(Self::DiskResident(tracker));
        }
        if let Some(tracker) = ReadOnlyImmutableIdTracker::try_open(fs, segment_path, hw_counter)? {
            return Ok(Self::Immutable(tracker));
        }
        Ok(Self::Appendable(ReadOnlyAppendableIdTracker::open(
            fs,
            segment_path,
            deferred_internal_id,
            hw_counter,
        )?))
    }

    /// Measure how far the writer has committed, before the directory listing snapshot is taken.
    pub async fn probe_committed<Fs: UniversalReadFsAsync<File = S>>(
        &self,
        inner_fs: &Fs,
        hw_acc: &HwMeasurementAcc,
    ) -> OperationResult<TrackerProbe> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.probe_committed(inner_fs, hw_acc).await,
            Self::Immutable(_) | Self::DiskResident(_) => Ok(TrackerProbe::Unknown),
        }
    }

    /// Stage post-LIST preloading on `CachedFs` (e.g. `reschedule_open` for `deleted.dat`).
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.live_preload(fs),
            Self::Immutable(id_tracker) => id_tracker.live_preload(fs, hw_counter),
            Self::DiskResident(id_tracker) => id_tracker.live_preload(fs, hw_counter),
        }
    }

    /// Reload externally-applied changes, dispatching to the active variant.
    ///
    /// `fs` refreshes storages that mutate in place (the immutable and disk
    /// trackers' deleted bitmaps) by opening fresh handles, and serves the
    /// appendable tracker's lazy file opens.
    pub fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        max_committed_id: Option<PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<LiveReloadResult> {
        match self {
            Self::Appendable(id_tracker) => {
                id_tracker.live_reload(fs, max_committed_id, hw_counter)
            }
            Self::Immutable(id_tracker) => id_tracker.live_reload(fs, hw_counter),
            Self::DiskResident(id_tracker) => id_tracker.live_reload(fs, hw_counter),
        }
    }
}

impl<S: UniversalRead> IdTrackerRead for ReadOnlyIdTrackerEnum<S> {
    type Backend = S;

    fn point_mappings(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> PointMappingsRefEnum<'_, Self::Backend> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.point_mappings(hw_counter),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.point_mappings(hw_counter),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.point_mappings(hw_counter)
            }
        }
    }

    fn internal_version(
        &self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<SeqNumberType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.internal_version(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.internal_version(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.internal_version(internal_id, hw_counter)
            }
        }
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior, hw_counter)
            }
        }
    }

    fn external_id(
        &self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<PointIdType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.external_id(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.external_id(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.external_id(internal_id, hw_counter)
            }
        }
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.internal_versions_batch(internal_ids, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.internal_versions_batch(internal_ids, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.internal_versions_batch(internal_ids, callback, hw_counter)
            }
        }
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.external_ids_batch(internal_ids, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.external_ids_batch(internal_ids, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.external_ids_batch(internal_ids, callback, hw_counter)
            }
        }
    }

    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        deferred_behavior: common::types::DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback, hw_counter)
            }
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.total_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.total_point_count(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.total_point_count(),
        }
    }

    fn available_point_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.available_point_count(hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.available_point_count(hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.available_point_count(hw_counter)
            }
        }
    }

    fn deleted_point_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.deleted_point_count(hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.deleted_point_count(hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.deleted_point_count(hw_counter)
            }
        }
    }

    fn deleted_point_bitslice(&self, hw_counter: &HardwareCounterCell) -> &BitSlice {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.deleted_point_bitslice(hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.deleted_point_bitslice(hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.deleted_point_bitslice(hw_counter)
            }
        }
    }

    fn is_deleted_point(
        &self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id, hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.is_deleted_point(internal_id, hw_counter)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.name(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.name(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.name(),
        }
    }

    fn iter_internal_versions(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.iter_internal_versions(hw_counter)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.iter_internal_versions(hw_counter)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.iter_internal_versions(hw_counter)
            }
        }
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deferred_internal_id(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deferred_internal_id(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.deferred_internal_id(),
        }
    }

    fn deferred_deleted_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deferred_deleted_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deferred_deleted_count(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.deferred_deleted_count(),
        }
    }
}
