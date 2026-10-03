use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
};
use futures::future::BoxFuture;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::ReadOnlyDiskIdTracker;
use crate::id_tracker::disk_id_tracker::on_disk_format::i2e_path;
use crate::id_tracker::immutable_id_tracker::mappings_path as immutable_mappings_path;
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

/// Persisted id-tracker format of a segment, one per [`ReadOnlyIdTrackerEnum`] variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadOnlyIdTrackerFormat {
    Appendable,
    Immutable,
    DiskResident,
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
    ) -> OperationResult<()> {
        match Self::detect_format(fs, segment_path)? {
            ReadOnlyIdTrackerFormat::Appendable => {
                ReadOnlyAppendableIdTracker::preopen(fs, segment_path)
            }
            ReadOnlyIdTrackerFormat::Immutable => {
                ReadOnlyImmutableIdTracker::preopen(fs, segment_path)
            }
            ReadOnlyIdTrackerFormat::DiskResident => {
                ReadOnlyDiskIdTracker::preopen(fs, segment_path, populate)?
            }
        }
        Ok(())
    }

    /// File to open before the segment's listing snapshot, so the snapshot covers every point
    /// the tracker loads. `None` if the tracker format doesn't need one.
    pub fn commit_mark(
        listing: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<(PathBuf, OpenOptions)>> {
        Ok(match Self::detect_format(listing, segment_path)? {
            ReadOnlyIdTrackerFormat::Appendable => {
                Some(ReadOnlyAppendableIdTracker::<S>::commit_mark(segment_path))
            }
            ReadOnlyIdTrackerFormat::Immutable | ReadOnlyIdTrackerFormat::DiskResident => None,
        })
    }

    /// The format stored at `segment_path`, judged from the files in the `listing` snapshot.
    fn detect_format(
        listing: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<ReadOnlyIdTrackerFormat> {
        Ok(
            if UniversalReadFs::exists(listing, &i2e_path(segment_path))? {
                ReadOnlyIdTrackerFormat::DiskResident
            } else if UniversalReadFs::exists(listing, &immutable_mappings_path(segment_path))? {
                ReadOnlyIdTrackerFormat::Immutable
            } else {
                ReadOnlyIdTrackerFormat::Appendable
            },
        )
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
    ) -> OperationResult<Self> {
        if let Some(tracker) = ReadOnlyDiskIdTracker::try_open(fs, segment_path, populate)? {
            return Ok(Self::DiskResident(tracker));
        }
        if let Some(tracker) = ReadOnlyImmutableIdTracker::try_open(fs, segment_path)? {
            return Ok(Self::Immutable(tracker));
        }
        Ok(Self::Appendable(ReadOnlyAppendableIdTracker::open(
            fs,
            segment_path,
            deferred_internal_id,
        )?))
    }

    /// Measure how far the writer has committed, before the directory listing snapshot is taken.
    pub async fn probe_committed<Fs: UniversalReadFsAsync<File = S>>(
        &self,
        inner_fs: &Fs,
    ) -> OperationResult<TrackerProbe> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.probe_committed(inner_fs).await,
            Self::Immutable(_) | Self::DiskResident(_) => Ok(TrackerProbe::Unknown),
        }
    }

    /// Stage post-LIST preloading on `CachedFs` (e.g. `reschedule_open` for `deleted.dat`).
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.live_preload(fs),
            Self::Immutable(id_tracker) => id_tracker.live_preload(fs),
            Self::DiskResident(id_tracker) => id_tracker.live_preload(fs),
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
    ) -> OperationResult<LiveReloadResult> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.live_reload(fs, max_committed_id),
            Self::Immutable(id_tracker) => id_tracker.live_reload(fs),
            Self::DiskResident(id_tracker) => id_tracker.live_reload(fs),
        }
    }
}

impl<S: UniversalRead> IdTrackerRead for ReadOnlyIdTrackerEnum<S> {
    type Backend = S;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.point_mappings(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.point_mappings(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.point_mappings(),
        }
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
        }
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.external_id(internal_id),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.external_id(internal_id),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.external_id(internal_id),
        }
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.internal_versions_batch(internal_ids, callback)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.internal_versions_batch(internal_ids, callback)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.internal_versions_batch(internal_ids, callback)
            }
        }
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => t.external_ids_batch(internal_ids, callback),
            ReadOnlyIdTrackerEnum::Immutable(t) => t.external_ids_batch(internal_ids, callback),
            ReadOnlyIdTrackerEnum::DiskResident(t) => t.external_ids_batch(internal_ids, callback),
        }
    }

    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        deferred_behavior: common::types::DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
            ReadOnlyIdTrackerEnum::DiskResident(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
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

    fn available_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.available_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.available_point_count(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.available_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deleted_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deleted_point_count(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deleted_point_bitslice(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deleted_point_bitslice(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
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
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.iter_internal_versions(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.iter_internal_versions(),
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => id_tracker.iter_internal_versions(),
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
