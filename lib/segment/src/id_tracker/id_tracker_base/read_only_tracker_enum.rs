use std::path::Path;

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::ReadOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::{
    LiveReloadResult, ReadOnlyAppendableIdTracker,
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
    pub fn preopen(fs: &impl CachedReadFs<File = S>, segment_path: &Path) -> OperationResult<()> {
        if ReadOnlyDiskIdTracker::try_preopen(fs, segment_path)? {
            return Ok(());
        }
        if ReadOnlyImmutableIdTracker::try_preopen(fs, segment_path)? {
            return Ok(());
        }
        ReadOnlyAppendableIdTracker::preopen(fs, segment_path)
    }

    /// Detect the persisted id-tracker format and load it, by *attempting* each
    /// format's open rather than probing file names one by one.
    ///
    /// This avoids the separate `exists` round-trips that a name-based detector
    /// would issue — costly on object storage (S3/GCS/Azure), where each is a
    /// remote request. Each candidate's open reads its own defining file, so a
    /// not-found there simply means "not this format" and we fall through.
    ///
    /// Order: disk-resident (the serverless/object-storage format) first, then
    /// the in-RAM immutable format, then the appendable/mutable format (whose
    /// open tolerates absent files, i.e. a fresh or empty segment).
    ///
    /// The attempts are sequential for now; they are independent and can be
    /// issued concurrently later (the slow-path being remote opens).
    /// `raw_fs` is the canonical backend for the appendable tracker, which
    /// stores a filesystem handle to re-open the append-only files on later
    /// reloads — a caching wrapper's snapshot would go stale.
    pub fn detect_and_load(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &S::Fs,
        segment_path: &Path,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        if let Some(tracker) = ReadOnlyDiskIdTracker::try_open(fs, segment_path)? {
            return Ok(Self::DiskResident(tracker));
        }
        if let Some(tracker) = ReadOnlyImmutableIdTracker::try_open(fs, segment_path)? {
            return Ok(Self::Immutable(tracker));
        }
        Ok(Self::Appendable(ReadOnlyAppendableIdTracker::open(
            raw_fs,
            segment_path,
            deferred_internal_id,
        )?))
    }

    /// Reload externally-applied changes, dispatching to the active variant.
    ///
    /// `fs` refreshes storages that mutate in place (the immutable and disk
    /// trackers' deleted bitmaps) by opening fresh handles; the appendable
    /// tracker keeps its own raw fs handle instead (see
    /// [`Self::detect_and_load`]).
    pub fn live_reload(&mut self, fs: &S::Fs) -> OperationResult<LiveReloadResult> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.live_reload(),
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
