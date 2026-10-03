//! A segment opened to retire points: the id tracker alone, since a delete
//! never reads the payload or the vectors.

use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::storage_version::{StorageVersion as _, VERSION_FILE};
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{
    CachedFs, CachedReadFs, UniversalRead, UniversalReadFsAsync, read_json_via,
};

use super::WriterIdTrackerState;
use super::lookup::{WRITER_POPULATE, build_cached_fs};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead as _;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::types::{PointIdType, SegmentConfig, SegmentState, SeqNumberType};

/// The id-tracker half of a [`LookupSegment`](super::LookupSegment): enough to
/// locate points, compare their versions and resume a writer that retires
/// them, without opening any storage.
pub struct TrackerLookup<Fs: UniversalReadFsAsync> {
    fs: CachedFs<Fs>,
    pub segment_path: PathBuf,
    id_tracker: ReadOnlyIdTrackerEnum<Fs::File>,
    /// What [`UpdateOnlySegmentEnum::open`](super::UpdateOnlySegmentEnum::open)
    /// is given; an appendable writer that only retires never uses it.
    pub segment_config: SegmentConfig,
    pub appendable: bool,
}

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// Open the id tracker of the segment at `segment_path`, prefetching its
    /// files with the state and version files in one round.
    pub fn open(fs: Fs, segment_path: &Path) -> OperationResult<Self> {
        let mut fs = build_cached_fs(fs, segment_path)?;
        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(&fs, segment_path.join(SEGMENT_STATE_FILE))?;
        ReadOnlyIdTrackerEnum::preopen(&fs, segment_path, WRITER_POPULATE)?;
        futures::executor::block_on(fs.wait_all());

        if SegmentVersion::load_universal(&fs, segment_path)?.is_none() {
            return Err(OperationError::FileNotFound {
                path: segment_path.join(VERSION_FILE),
            });
        }
        let id_tracker =
            ReadOnlyIdTrackerEnum::detect_and_load(&fs, segment_path, None, WRITER_POPULATE)?;
        fs.rotate_cache_file_info();

        Ok(Self {
            fs,
            segment_path: segment_path.to_path_buf(),
            id_tracker,
            appendable: config.is_appendable(),
            segment_config: config,
        })
    }

    /// See [`LookupSegment::locate_points`](super::LookupSegment::locate_points).
    pub fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        locate_points(&self.id_tracker, point_ids, callback)
    }

    /// See [`LookupSegment::point_versions`](super::LookupSegment::point_versions).
    pub fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
        point_versions(&self.id_tracker, internal_ids)
    }

    /// See [`LookupSegment::writer_state`](super::LookupSegment::writer_state).
    pub fn writer_state(&self) -> WriterIdTrackerState {
        WriterIdTrackerState::of(&self.id_tracker)
    }

    /// Catch the id tracker up with what was written since the open or the
    /// previous reload.
    pub fn live_reload(&mut self) -> OperationResult<()> {
        let Self { fs, id_tracker, .. } = self;
        fs.cache_file_info()?;
        let futs = id_tracker.live_preload(fs)?;
        futures::executor::block_on(async {
            futures::join!(fs.wait_all(), futures::future::join_all(futs))
        });
        id_tracker.live_reload(fs)?;
        fs.rotate_cache_file_info();
        Ok(())
    }
}

/// Deferred heads are included, so a point shadowed by an optimization in
/// progress resolves to its latest slot.
pub(super) fn locate_points<S: UniversalRead>(
    id_tracker: &ReadOnlyIdTrackerEnum<S>,
    point_ids: impl IntoIterator<Item = PointIdType>,
    callback: impl FnMut(PointIdType, PointOffsetType),
) -> OperationResult<()> {
    id_tracker.resolve_external_ids(point_ids, DeferredBehavior::WithDeferred, callback)
}

/// A slot the tracker has no version for is absent from the map; it counts as
/// `0`, the version an unwritten point compares as.
pub(super) fn point_versions<S: UniversalRead>(
    id_tracker: &ReadOnlyIdTrackerEnum<S>,
    internal_ids: &[PointOffsetType],
) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
    let mut versions = AHashMap::with_capacity(internal_ids.len());
    id_tracker.internal_versions_batch(internal_ids.iter().copied(), |internal_id, version| {
        versions.insert(internal_id, version);
    })?;
    Ok(versions)
}
