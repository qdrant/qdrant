use std::path::{Path, PathBuf};

use common::mmap::Advice::Normal;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyAppendableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::point_mappings::PointMappings;

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
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
        fs: &CachedReadFs<S::Fs>,
        segment_path: impl Into<PathBuf>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        // The tracker keeps a filesystem handle to re-open the append-only
        // files on later reloads, so it must retain the *raw* backend — the
        // `CachedReadFs` snapshot goes stale as soon as the writer appends.
        // The bootstrap below consequently opens through the raw fs too,
        // bypassing the prefetch pool (its handles are simply dropped).
        let mut tracker = Self {
            segment_path: segment_path.into(),
            fs: fs.inner().clone(),
            internal_to_version: Vec::new(),
            mappings: PointMappings::new(
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                deferred_internal_id,
            ),
            pending_inserts: Default::default(),
            mappings_read_to: 0,
            // Opened lazily by `live_reload`: the files may not exist until the writer flushes.
            mappings_file: None,
            versions_file: None,
        };

        // Load the existing data the same way a live-reload consumes appended data. The reported
        // delta (the whole committed set as inserts) is irrelevant for an initial open.
        tracker.live_reload()?;

        #[cfg(debug_assertions)]
        tracker.mappings.assert_mappings();

        Ok(tracker)
    }

    /// Open the file at `path` read-only, returning `None` if it does not exist.
    ///
    /// A read-only follower's mappings/versions files are absent while empty (the writer never
    /// writes an empty file), so `NotFound` means an empty storage rather than an error. We open
    /// directly and map `NotFound` to `None` instead of probing with `exists` first, to avoid a
    /// second round-trip on object storage. Lazy backends (e.g. S3) touch the object only on the
    /// first read, so a missing object can instead surface as `NotFound` from a later `len`/`read`
    /// — `live_reload` tolerates that case too.
    pub(super) fn try_open(fs: &S::Fs, path: &Path) -> OperationResult<Option<S>> {
        let options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Normal),
        };
        Ok(fs.open(path, options, Default::default()).ok_not_found()?)
    }
}
