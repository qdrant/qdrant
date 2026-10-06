use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, Populate, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
};
use futures::future::BoxFuture;
use roaring::RoaringBitmap;
use strum::{EnumDiscriminants, EnumIter, IntoEnumIterator as _};
use uuid::Uuid;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::ReadOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::{
    LiveReloadResult, ReadOnlyAppendableIdTracker, TrackerProbe,
};
use crate::id_tracker::point_moves::{MoveResolution, PointMoves, PointMovesMode, SlotRef};
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

#[derive(EnumDiscriminants)]
#[strum_discriminants(name(ReadOnlyIdTrackerKind), derive(EnumIter))]
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
    ) -> OperationResult<()> {
        if ReadOnlyDiskIdTracker::try_preopen(fs, segment_path, populate)? {
            return Ok(());
        }
        if ReadOnlyImmutableIdTracker::try_preopen(fs, segment_path)? {
            return Ok(());
        }
        ReadOnlyAppendableIdTracker::preopen(fs, segment_path);
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
        max_committed_offset: Option<PointOffsetType>,
        populate: Populate,
    ) -> OperationResult<Self> {
        Self::detect_and_load_with_moves(
            fs,
            fs,
            segment_path,
            deferred_internal_id,
            max_committed_offset,
            populate,
            PointMovesMode::Ignore,
        )
    }

    /// [`detect_and_load`](Self::detect_and_load) with point moves handled per `moves`. With
    /// [`PointMovesMode::Resolve`] the move log is read through `raw_fs`, bypassing the listing
    /// snapshot of `fs`, after the tombstones: see [`point_moves`](crate::id_tracker::point_moves).
    pub fn detect_and_load_with_moves(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        deferred_internal_id: Option<PointOffsetType>,
        max_committed_offset: Option<PointOffsetType>,
        populate: Populate,
        moves: PointMovesMode,
    ) -> OperationResult<Self> {
        if let Some(tracker) =
            ReadOnlyDiskIdTracker::try_open_with_moves(fs, raw_fs, segment_path, populate, moves)?
        {
            return Ok(Self::DiskResident(tracker));
        }
        if let Some(tracker) =
            ReadOnlyImmutableIdTracker::try_open_with_moves(fs, raw_fs, segment_path, moves)?
        {
            return Ok(Self::Immutable(tracker));
        }
        Ok(Self::Appendable(
            ReadOnlyAppendableIdTracker::open_with_moves(
                fs,
                raw_fs,
                segment_path,
                deferred_internal_id,
                max_committed_offset,
                moves,
            )?,
        ))
    }

    /// Files whose size bounds the committed points, across every format that
    /// has one. Probe them before taking the listing snapshot.
    pub fn commit_mark_paths(segment_path: &Path) -> Vec<PathBuf> {
        ReadOnlyIdTrackerKind::iter()
            .filter_map(|kind| match kind {
                ReadOnlyIdTrackerKind::Appendable => {
                    ReadOnlyAppendableIdTracker::<S>::commit_mark_path(segment_path)
                }
                ReadOnlyIdTrackerKind::Immutable => {
                    ReadOnlyImmutableIdTracker::<S>::commit_mark_path(segment_path)
                }
                ReadOnlyIdTrackerKind::DiskResident => {
                    ReadOnlyDiskIdTracker::<S>::commit_mark_path(segment_path)
                }
            })
            .collect()
    }

    /// Exclusive offset bound of committed points, from `fs`'s snapshot of the
    /// [`commit_mark_paths`](Self::commit_mark_paths) files.
    pub fn max_committed_offset(
        fs: &impl CachedReadFs,
        segment_path: &Path,
    ) -> Option<PointOffsetType> {
        ReadOnlyIdTrackerKind::iter().find_map(|kind| match kind {
            ReadOnlyIdTrackerKind::Appendable => {
                ReadOnlyAppendableIdTracker::<S>::max_committed_offset(fs, segment_path)
            }
            ReadOnlyIdTrackerKind::Immutable => {
                ReadOnlyImmutableIdTracker::<S>::max_committed_offset(fs, segment_path)
            }
            ReadOnlyIdTrackerKind::DiskResident => {
                ReadOnlyDiskIdTracker::<S>::max_committed_offset(fs, segment_path)
            }
        })
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

    /// Make the inserts reported by the last [`Self::live_reload`] visible to readers. Call once
    /// every component has ingested them.
    pub fn publish_staged(&mut self) {
        match self {
            Self::Appendable(id_tracker) => id_tracker.publish_staged(),
            // Never report inserts
            Self::Immutable(_) | Self::DiskResident(_) => {}
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

/// Point moves, see [`point_moves`](crate::id_tracker::point_moves). Every method is a no-op, or
/// reports nothing, on a tracker opened with [`PointMovesMode::Ignore`].
impl<S: UniversalRead> ReadOnlyIdTrackerEnum<S> {
    /// Whether `internal_id` has settled in this view: its point, or a later state of it, is
    /// visible. Only appendable segments receive moves; every slot of the others has settled.
    pub fn is_settled(&self, internal_id: PointOffsetType) -> bool {
        match self {
            Self::Appendable(id_tracker) => id_tracker.is_settled(internal_id),
            Self::Immutable(id_tracker) => (internal_id as usize) < id_tracker.total_point_count(),
            Self::DiskResident(id_tracker) => {
                (internal_id as usize) < id_tracker.total_point_count()
            }
        }
    }

    fn point_moves_state(&self) -> Option<&PointMoves> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.point_moves(),
            Self::Immutable(id_tracker) => id_tracker.point_moves().map(|moves| &moves.state),
            Self::DiskResident(id_tracker) => id_tracker.point_moves().map(|moves| &moves.state),
        }
    }

    /// Whether this tracker resolves point moves.
    pub fn resolves_point_moves(&self) -> bool {
        self.point_moves_state().is_some()
    }

    /// Per source segment, the source slots of this segment's settled moved-in records: the masks
    /// the shard hands to those segments.
    pub fn settled_moved_in(&self) -> Option<&AHashMap<Uuid, RoaringBitmap>> {
        self.point_moves_state().map(PointMoves::settled_moved_in)
    }

    /// Every target this segment's moved-out records name.
    pub fn moved_out_targets(&self) -> Vec<SlotRef> {
        self.point_moves_state()
            .map(|moves| moves.moved_out_targets().collect())
            .unwrap_or_default()
    }

    /// Where a tail read of the move log has to start, if some tombstone waits for one to be
    /// classified.
    pub fn point_moves_tail_to_read(&self) -> Option<(PathBuf, u64)> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.point_moves_tail_to_read(),
            Self::Immutable(id_tracker) => id_tracker.point_moves()?.tail_to_read(),
            Self::DiskResident(id_tracker) => id_tracker.point_moves()?.tail_to_read(),
        }
    }

    /// Take in a tail read of the move log from byte offset `start`, and classify the tombstones
    /// that waited for it.
    pub fn ingest_point_moves_tail(&mut self, start: u64, bytes: &[u8]) {
        match self {
            Self::Appendable(id_tracker) => id_tracker.ingest_point_moves_tail(start, bytes),
            Self::Immutable(id_tracker) => id_tracker.ingest_point_moves_tail(start, bytes),
            Self::DiskResident(id_tracker) => id_tracker.ingest_point_moves_tail(start, bytes),
        }
    }

    /// Whether some tombstone is still held back for a move that did not settle, or for a tail read
    /// that did not happen.
    pub fn holds_point_moves(&self) -> bool {
        match self {
            Self::Appendable(id_tracker) => id_tracker.holds_point_moves(),
            Self::Immutable(id_tracker) => id_tracker
                .point_moves()
                .is_some_and(|moves| moves.holds_moves()),
            Self::DiskResident(id_tracker) => id_tracker
                .point_moves()
                .is_some_and(|moves| moves.holds_moves()),
        }
    }

    /// Whether some tombstone stays held back for a move once `resolution` is applied: then a copy
    /// on this segment may still be the only visible one of its point.
    pub fn holds_point_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        match self {
            Self::Appendable(id_tracker) => id_tracker.holds_point_moves_beyond(resolution),
            Self::Immutable(id_tracker) => id_tracker
                .point_moves()
                .is_some_and(|moves| moves.holds_moves_beyond(resolution)),
            Self::DiskResident(id_tracker) => id_tracker
                .point_moves()
                .is_some_and(|moves| moves.holds_moves_beyond(resolution)),
        }
    }

    /// The slots to delete now, per the shard's `settled` gate and `masked`, the slots of this
    /// segment other segments' settled moved-in records name.
    pub fn resolve_point_moves(
        &self,
        settled: &impl Fn(SlotRef) -> bool,
        masked: Option<&RoaringBitmap>,
    ) -> MoveResolution {
        match self {
            Self::Appendable(id_tracker) => id_tracker.resolve_point_moves(settled, masked),
            Self::Immutable(id_tracker) => id_tracker.resolve_point_moves(settled, masked),
            Self::DiskResident(id_tracker) => id_tracker.resolve_point_moves(settled, masked),
        }
    }

    /// Delete what `resolution` names, returning the slots that were live as a delete-only delta.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        match self {
            Self::Appendable(id_tracker) => id_tracker.apply_point_moves(resolution),
            Self::Immutable(id_tracker) => id_tracker.apply_point_moves(resolution),
            Self::DiskResident(id_tracker) => id_tracker.apply_point_moves(resolution),
        }
    }
}
