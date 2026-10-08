use std::io::Cursor;
use std::path::PathBuf;

use common::generic_consts::Sequential;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, ReadRange, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
};
use futures::future::BoxFuture;

use super::{ReadOnlyAppendableIdTracker, TrackerFiles};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::change::MappingChange;
use crate::id_tracker::mutable_id_tracker::mappings_storage::{mappings_path, read_mappings_iter};
use crate::id_tracker::mutable_id_tracker::versions_storage::{
    VERSION_ELEMENT_SIZE, versions_path,
};
use crate::types::SeqNumberType;

/// Set of point offsets that changed during a [`ReadOnlyAppendableIdTracker::live_reload`].
///
/// A point is only reported once its version is flushed (the version is written last, so its
/// presence means the point's data is fully committed). Both vectors are sorted ascending.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveReloadResult {
    /// Offsets that became available: their version just became readable and they are live.
    pub inserted: Vec<PointOffsetType>,
    /// Offsets that were previously reported as available and are now deleted.
    pub deleted: Vec<PointOffsetType>,
}

/// What [`ReadOnlyAppendableIdTracker::probe_committed`] learned about the tracker files before
/// the directory listing snapshot is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackerProbe {
    /// Neither tracker file changed since the last reload, so the listing can be skipped.
    /// `max_committed_id` is the versions length observed by the probe.
    Unchanged { max_committed_id: PointOffsetType },
    /// A tracker file changed. The reload commits points below `max_committed_id`, the versions
    /// length observed by the probe, which every file in the following listing covers.
    Changed { max_committed_id: PointOffsetType },
    /// The tracker detects changes only by comparing listing snapshots (immutable and
    /// disk-resident trackers rewrite `deleted.dat` in place), so the listing is always needed.
    Unknown,
}

impl TrackerProbe {
    /// `true` if the probe proved the tracker files unchanged.
    pub fn is_unchanged(&self) -> bool {
        match self {
            TrackerProbe::Unchanged {
                max_committed_id: _,
            } => true,
            TrackerProbe::Changed {
                max_committed_id: _,
            }
            | TrackerProbe::Unknown => false,
        }
    }

    /// Upper bound for the points the following reload commits, `None` if the tracker has none.
    pub fn max_committed_id(&self) -> Option<PointOffsetType> {
        match self {
            TrackerProbe::Unchanged { max_committed_id }
            | TrackerProbe::Changed { max_committed_id } => Some(*max_committed_id),
            TrackerProbe::Unknown => None,
        }
    }
}

impl LiveReloadResult {
    /// `true` if this delta carries no changes.
    pub fn is_empty(&self) -> bool {
        self.inserted.is_empty() && self.deleted.is_empty()
    }

    /// Fold a freshly-read delta into this not-yet-applied one, keeping both lists
    /// sorted ascending and deduplicated.
    ///
    /// Used to retain unapplied changes across a failed reload: the next reload
    /// folds in the tracker's new delta and replays the union, so no change is
    /// dropped. Offsets are monotonic (a deleted offset is never reused), so the
    /// only cross-conflict is an offset that was inserted (but not yet applied)
    /// and then deleted — it is ultimately gone, so it is dropped from `inserted`
    /// and kept in `deleted`. That way a component which already ingested it
    /// during the failed attempt drops it when the union is replayed.
    pub fn merge(&mut self, other: LiveReloadResult) {
        let LiveReloadResult { inserted, deleted } = other;

        self.inserted.extend(inserted);
        self.inserted.sort_unstable();
        self.inserted.dedup();

        self.deleted.extend(deleted);
        self.deleted.sort_unstable();
        self.deleted.dedup();

        let deleted = &self.deleted;
        self.inserted
            .retain(|offset| deleted.binary_search(offset).is_err());
    }
}

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Measure how far the writer has committed, before the directory listing snapshot is taken.
    ///
    /// Refreshes `versions.dat` and `mappings.dat` through the inner filesystem (live-reloading
    /// held handles in place, opening missing ones) and reports the observed versions length as
    /// `max_committed_id`. The tracker's visible state is unchanged until [`Self::live_reload`].
    pub async fn probe_committed<Fs: UniversalReadFsAsync<File = S>>(
        &self,
        inner_fs: &Fs,
    ) -> OperationResult<TrackerProbe> {
        // Held across the refresh IO. Only refreshes take it, and the caller serializes those.
        let mut files = self.files.lock().await;
        let TrackerFiles { mappings, versions } = &mut *files;

        // Refreshed concurrently, so the mappings may be observed older than the versions. That is
        // safe: a point becomes visible only once both its insert and its version are read, so a
        // missing insert just defers the point, and the grown mappings file flags the next probe
        // as changed.
        futures::try_join!(
            Self::refresh_file(versions, inner_fs, versions_path(&self.segment_path)),
            Self::refresh_file(mappings, inner_fs, mappings_path(&self.segment_path)),
        )?;

        let v_len = match versions {
            Some(file) => (file.len::<u8>()? / VERSION_ELEMENT_SIZE) as usize,
            None => 0,
        };
        let m_bytes = match mappings {
            Some(file) => file.len::<u8>()?,
            None => 0,
        };

        let versions_changed = v_len != self.internal_to_version.len();
        let mappings_changed = m_bytes != self.mappings_read_to;

        let max_committed_id = v_len as PointOffsetType;
        Ok(if versions_changed || mappings_changed {
            TrackerProbe::Changed { max_committed_id }
        } else {
            TrackerProbe::Unchanged { max_committed_id }
        })
    }

    /// Live-reload a held handle in place, or open it through `inner_fs` once the file exists.
    async fn refresh_file<Fs: UniversalReadFsAsync<File = S>>(
        file: &mut Option<S>,
        inner_fs: &Fs,
        path: PathBuf,
    ) -> OperationResult<()> {
        match file {
            Some(file) => {
                if let Some(fut) = file.live_preload(|_| None).ok_not_found()? {
                    fut.await;
                    file.live_reload().ok_not_found()?;
                }
            }
            None => {
                *file = inner_fs
                    .open_async(path, Self::open_options(), Default::default())
                    .await
                    .ok_not_found()?;
            }
        }
        Ok(())
    }

    /// Post-LIST preloading on `CachedFs`. Appendable tracker reloads its handles during
    /// [`Self::probe_committed`] on the inner filesystem, so this is a no-op.
    pub fn live_preload(
        &self,
        _fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        Ok(Vec::new())
    }

    /// Consume mapping and version changes appended to storage since the last reload.
    ///
    /// File handles are refreshed via [`UniversalRead::live_reload`] so data appended by the writer
    /// becomes visible; not-yet-opened files are opened lazily through `fs` (a caching wrapper's
    /// prefetch pool serves these opens when staged). Both result lists are sorted ascending.
    ///
    /// The writer flushes mappings before data before versions, so a point's version appears last
    /// and marks it as fully committed. Inserts are therefore driven by the versions file: an
    /// offset is reported as inserted only once its version becomes readable (and it is still live
    /// in the mapping). A point that is mapped but whose version is not flushed yet is intentionally
    /// withheld (its data may be partial) and reported on a later reload once its version lands.
    /// Deletes are driven by the mapping and need no version, a deleted point's version is
    /// considered gone.
    ///
    /// Deletes apply immediately, but reported inserts stay staged, invisible to readers, until
    /// [`Self::publish_staged`]; reloading again before that reports them again.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        max_committed_id: Option<PointOffsetType>,
    ) -> OperationResult<LiveReloadResult> {
        let preloaded = max_committed_id.is_some();

        // Append versions flushed since the last reload (mappings are flushed before versions).
        // `committed` is the exclusive offset bound for which versions exist, i.e. the commit mark.
        let committed = self.reload_versions(fs, max_committed_id, preloaded)? as PointOffsetType;

        // Consume new mapping changes. Inserts are buffered until committed (their version exists);
        // deletes act on the committed mapping immediately, or cancel a still-pending insert.
        let changes = self.read_new_mapping_changes(fs, preloaded)?;

        for change in &changes {
            log::trace!(target: "live-reload", "Read mapping in {:?} change: {:?}", self.segment_path, change);
        }

        let mut deleted = Vec::new();
        for change in &changes {
            match *change {
                MappingChange::Insert(external_id, internal_id) => {
                    self.max_claimed_internal_id =
                        self.max_claimed_internal_id.max(Some(internal_id));
                    // A re-insert supersedes a staged one, which a failed reload may have handed to
                    // components already
                    if let Some(staged) = self.staged_inserts.remove(&external_id) {
                        deleted.push(staged);
                    }
                    self.unversioned_inserts.insert(external_id, internal_id);
                }
                MappingChange::Delete(external_id) => {
                    // A point can be both committed (an old offset) and pending (a not-yet-committed
                    // re-insert at a new offset). A delete removes it from both. Report the deleted
                    // offset only if it was previously reported, linked or staged.
                    self.unversioned_inserts.remove(&external_id);
                    if let Some(staged) = self.staged_inserts.remove(&external_id) {
                        deleted.push(staged);
                    }
                    if let Some(internal_id) = self.mappings.drop(external_id) {
                        deleted.push(internal_id);
                    }
                }
            }
        }

        let versioned = self
            .unversioned_inserts
            .extract_if(|_, &mut internal_id| internal_id < committed);
        self.staged_inserts.extend(versioned);

        // Every staged insert is reported, including those an unpublished reload reported before.
        let mut inserted = Vec::new();
        for (external_id, &internal_id) in &self.staged_inserts {
            // An upsert re-links an existing external id to a new offset; the previously-committed
            // offset it displaces is dead once linked and must be reported as deleted.
            if let Some(previous) = self.mappings.peek_link(external_id, internal_id)
                && previous != internal_id
            {
                deleted.push(previous);
            }
            inserted.push(internal_id);
        }

        // `staged_inserts` iterates in arbitrary hash order; both result lists are sorted ascending.
        inserted.sort_unstable();
        deleted.sort_unstable();
        deleted.dedup();

        Ok(LiveReloadResult { inserted, deleted })
    }

    /// Link the inserts the last reload reported, making them visible to readers.
    ///
    /// Call once every component has ingested that reload's delta, so the tracker never exposes an
    /// offset a component cannot serve.
    pub fn publish_staged(&mut self) {
        for (external_id, internal_id) in self.staged_inserts.drain() {
            self.mappings.set_link(external_id, internal_id);
        }
    }

    /// Read mapping changes appended after the last consumed offset, advancing `mappings_read_to`.
    ///
    /// The read stops at the last fully-readable entry; a partial trailing entry is left in place
    /// so it can be consumed on a later reload once the writer flushed it completely.
    fn read_new_mapping_changes(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        preloaded: bool,
    ) -> OperationResult<Vec<MappingChange>> {
        // The mappings file is absent until the writer flushes the first point; open it lazily once
        // it appears. Until then there is nothing to read.
        let mappings_file = &mut self.files.get_mut().mappings;
        if mappings_file.is_none() || !preloaded {
            match mappings_file.as_mut() {
                Some(file) => {
                    // Refresh the handle to observe data appended by the writer. A lazily-opened handle whose
                    // object does not exist yet (e.g. S3) reports `NotFound` here or from `len`; treat that as
                    // an empty file.
                    file.live_reload().ok_not_found()?;
                }
                None => {
                    *mappings_file = Self::try_open(fs, &mappings_path(&self.segment_path))?;
                }
            }
        }
        let Some(file) = mappings_file.as_mut() else {
            return Ok(Vec::new());
        };

        let Some(file_len) = file.len::<u8>().ok_not_found()? else {
            return Ok(Vec::new());
        };

        // Defensive: committed entries are never removed, but a flush may truncate a partial
        // trailing entry. If the file ever ends up shorter than our read position, continue from
        // the new end rather than reading past EOF.
        let start = self.mappings_read_to.min(file_len);
        if start < self.mappings_read_to {
            log::warn!(
                "Read-only appendable ID tracker mappings file is shorter than expected ({file_len} < {} bytes), continuing from end of file",
                self.mappings_read_to,
            );
        }
        if start >= file_len {
            self.mappings_read_to = start;
            return Ok(Vec::new());
        }

        let bytes = file.read::<_, u8>(ReadRange::new(start, file_len - start), Sequential)?;
        let mut reader = Cursor::new(bytes.as_ref());

        let mut changes = Vec::new();
        for change in read_mappings_iter(&mut reader) {
            changes.push(change?);
        }
        let consumed = reader.position();

        self.mappings_read_to = start + consumed;

        Ok(changes)
    }

    /// Append versions flushed since the last reload, returning the new committed version count.
    ///
    /// Versions are an append-only delta: `internal_to_version` is kept exactly as long as the
    /// flushed versions file. A point that is mapped but whose version is not flushed yet has no
    /// slot, so [`internal_version`](crate::id_tracker::IdTrackerRead::internal_version) returns
    /// `None` for it (it is never given a fake version) until its version is appended here. We do
    /// not read versions for deleted points, a deleted point's version is considered gone.
    fn reload_versions(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        max_committed_id: Option<PointOffsetType>,
        preloaded: bool,
    ) -> OperationResult<usize> {
        // The versions file is absent until the writer flushes the first point; open it lazily once
        // it appears. Until then no version is committed.
        let versions_file = &mut self.files.get_mut().versions;
        if versions_file.is_none() || !preloaded {
            match versions_file.as_mut() {
                Some(versions_file) => {
                    // Refresh the handle to observe data appended by the writer. A lazily-opened handle whose
                    // object does not exist yet (e.g. S3) reports `NotFound` here or from `len`; treat that as
                    // an empty file (no committed versions).
                    versions_file.live_reload().ok_not_found()?;
                }
                None => {
                    *versions_file = Self::try_open(fs, &versions_path(&self.segment_path))?;
                }
            }
        }
        let Some(versions_file) = versions_file.as_mut() else {
            return Ok(self.internal_to_version.len());
        };

        // Disjoint field borrow so the read (from `versions_file`) can extend `internal_to_version`.
        let internal_to_version = &mut self.internal_to_version;

        // Floor the raw byte length to whole elements: a partially-written trailing version (a torn
        // flush) is ignored, only fully-written versions are loaded. We read the byte length rather
        // than `len::<SeqNumberType>()` on purpose, some backends debug-assert the file length is a
        // whole number of elements, which a torn flush violates.
        let Some(versions_bytes) = versions_file.len::<u8>().ok_not_found()? else {
            return Ok(internal_to_version.len());
        };
        let mut versions_len = (versions_bytes / VERSION_ELEMENT_SIZE) as usize;

        if let Some(target) = max_committed_id {
            versions_len = versions_len.min(target as usize);
        }

        let loaded_len = internal_to_version.len();

        // Append the newly flushed tail. Anything beyond `versions_len` is not flushed yet and
        // stays absent until a later reload (the mapped-but-versionless case).
        if versions_len > loaded_len {
            let tail = versions_file.read::<_, SeqNumberType>(
                ReadRange::new(
                    (loaded_len as u64) * VERSION_ELEMENT_SIZE,
                    (versions_len - loaded_len) as u64,
                ),
                Sequential,
            )?;
            internal_to_version.extend_from_slice(&tail);
        }

        Ok(internal_to_version.len())
    }
}
