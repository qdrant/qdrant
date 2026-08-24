use std::path::{Path, PathBuf};

use common::mmap::Advice::Normal;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyAppendableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::mappings_storage::mappings_path;
use crate::id_tracker::mutable_id_tracker::versions_storage::versions_path;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::PointIdType;

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    pub(super) fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Advice(Normal),
        }
    }

    /// Schedule background prefetch of the mappings log and versions file that
    /// [`open`](Self::open) reads via [`live_reload`](Self::live_reload).
    ///
    /// Either file may not exist yet — the writer only creates them once it
    /// flushes the first point, and [`open`](Self::open) treats a missing file
    /// as an empty storage — so absence is tolerated here too.
    pub fn preopen(fs: &impl CachedReadFs<File = S>, segment_path: &Path) -> OperationResult<()> {
        let options = Self::open_options();

        fs.schedule_open(&mappings_path(segment_path), Some(options), None)
            .ok_not_found()?;
        fs.schedule_open(&versions_path(segment_path), Some(options), None)
            .ok_not_found()?;

        Ok(())
    }

    /// Open a read-only view over the appendable ID tracker data at `segment_path`, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// The mappings and versions files may not exist yet — the writer only creates them once it
    /// flushes the first point, exactly as
    /// [`MutableIdTracker::open`](crate::id_tracker::mutable_id_tracker::MutableIdTracker::open)
    /// tolerates. A missing file is treated as an empty storage (not an error) and opened lazily
    /// once it appears.
    ///
    /// Unlike `MutableIdTracker::open` this never writes to the storage. The initial state is loaded
    /// by running the same reconciliation as [`Self::live_reload`] from an empty tracker: the whole
    /// mappings log and versions file are consumed, applying only committed points (a partial
    /// trailing entry is simply not consumed and picked up on a later reload).
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: impl Into<PathBuf>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        // The bootstrap below opens through the raw fs passed here, bypassing
        // any prefetch pool. Later reloads open through the fs their caller
        // provides instead (typically a caching wrapper with a fresh snapshot).
        let mut tracker = Self {
            segment_path: segment_path.into(),
            internal_to_version: Vec::new(),
            mappings: PointMappings::new(
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                deferred_internal_id,
            ),
            pending_inserts: Default::default(),
            max_claimed_internal_id: None,
            mappings_read_to: 0,
            // Opened lazily by `live_reload`: the files may not exist until the writer flushes.
            mappings_file: None,
            versions_file: None,
        };

        // Load the existing data the same way a live-reload consumes appended data. The reported
        // delta (the whole committed set as inserts) is irrelevant for an initial open.
        tracker.live_reload(fs)?;

        #[cfg(debug_assertions)]
        tracker.mappings.assert_mappings();

        Ok(tracker)
    }

    /// Byte offset just past the last complete entry consumed from the mappings log, where the next
    /// appended entry belongs.
    ///
    /// Entries vary in length, so a writer cannot recover this from the file and has to resume from
    /// here. A torn tail sits above it and is cut off by the next append.
    pub fn mappings_read_to(&self) -> u64 {
        self.mappings_read_to
    }

    /// Highest slot the mappings log has claimed, `None` while it has claimed none: the slot a
    /// writer resumes above.
    ///
    /// Counts every slot the log ever handed out, including those no longer reachable by external
    /// id and those whose version was never committed.
    pub fn max_claimed_internal_id(&self) -> Option<PointOffsetType> {
        self.max_claimed_internal_id
    }

    /// External ids the mappings log has inserted whose slots the versions array does not cover, in
    /// arbitrary order.
    ///
    /// Each is a point this view withholds because its data may be half-written. A writer resuming
    /// from this view retires them.
    pub fn pending_inserts(&self) -> impl Iterator<Item = PointIdType> + '_ {
        self.pending_inserts.keys().copied()
    }

    /// Open the file at `path` read-only, returning `None` if it does not exist.
    ///
    /// A read-only follower's mappings/versions files are absent while empty (the writer never
    /// writes an empty file), so `NotFound` means an empty storage rather than an error. We open
    /// directly and map `NotFound` to `None` instead of probing with `exists` first, to avoid a
    /// second round-trip on object storage. Lazy backends (e.g. S3) touch the object only on the
    /// first read, so a missing object can instead surface as `NotFound` from a later `len`/`read`
    /// — `live_reload` tolerates that case too.
    pub(super) fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
    ) -> OperationResult<Option<S>> {
        let options = Self::open_options();
        Ok(fs.open(path, options, Default::default()).ok_not_found()?)
    }
}
