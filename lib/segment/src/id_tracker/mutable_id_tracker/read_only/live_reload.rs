use std::io::Cursor;

use common::generic_consts::Sequential;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, ReadRange, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::ReadOnlyAppendableIdTracker;
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

/// Preloaded state and opened file handles from [`ReadOnlyAppendableIdTracker::live_preload_inner`].
#[derive(Debug)]
pub struct IdTrackerPreload<S: UniversalRead> {
    /// Upper bound (exclusive) for internal IDs whose versions can be committed during this reload.
    pub max_committed_id: PointOffsetType,
    pub versions_file: S,
    pub mappings_file: S,
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
    /// Preload `versions.dat` and `mappings.dat` directly on the inner filesystem
    /// before taking the directory listing snapshot.
    ///
    /// This anchors `max_committed_id` to the versions length observed before the
    /// snapshot, ensuring `reload_versions` never commits beyond what was flushed
    /// at preload time. Returns the opened file handles so `live_reload` can reuse
    /// them directly without opening them again.
    pub async fn live_preload_inner<Fs: UniversalReadFsAsync<File = S>>(
        &self,
        inner_fs: &Fs,
    ) -> OperationResult<(bool, Option<IdTrackerPreload<S>>)> {
        let v_path = versions_path(&self.segment_path);
        let m_path = mappings_path(&self.segment_path);

        let options = Self::open_options();

        let v_file = inner_fs
            .open_async(v_path, options, Default::default())
            .await
            .ok_not_found()?;
        let m_file = inner_fs
            .open_async(m_path, options, Default::default())
            .await
            .ok_not_found()?;

        let Some((v_file, m_file)) = v_file.zip(m_file) else {
            return Ok((false, None));
        };

        let bytes = v_file.len::<u8>()?;
        let v_len = (bytes / VERSION_ELEMENT_SIZE) as usize;
        let m_bytes = m_file.len::<u8>()?;

        let versions_changed = v_len != self.internal_to_version.len();
        let mappings_changed = m_bytes != self.mappings_read_to;
        let changed = versions_changed || mappings_changed;

        let preload = IdTrackerPreload {
            max_committed_id: v_len as PointOffsetType,
            versions_file: v_file,
            mappings_file: m_file,
        };

        Ok((changed, Some(preload)))
    }

    /// Stage what the next [`live_reload`](Self::live_reload) does per file: a
    /// reopen for held handles, a prefetch for files it opens lazily. Absence
    /// is tolerated the same way the reload tolerates it.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let options = Self::open_options();
        let mut futs: Vec<BoxFuture<'static, ()>> = Vec::new();
        for (file, path) in [
            (&self.versions_file, versions_path(&self.segment_path)),
            (&self.mappings_file, mappings_path(&self.segment_path)),
        ] {
            match file {
                Some(file) => {
                    futs.extend(
                        file.live_preload(|p| fs.cached_file_info(p))
                            .ok_not_found()?
                            .map(FutureExt::boxed),
                    );
                }
                None => {
                    fs.schedule_open(&path, Some(options), None);
                }
            };
        }
        Ok(futs)
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
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        preload: Option<IdTrackerPreload<S>>,
    ) -> OperationResult<LiveReloadResult> {
        let max_committed_id = preload.as_ref().map(|p| p.max_committed_id);
        let had_preloaded = preload.is_some();
        if let Some(preload) = preload {
            self.versions_file = Some(preload.versions_file);
            self.mappings_file = Some(preload.mappings_file);
        }

        // Append versions flushed since the last reload (mappings are flushed before versions).
        // `committed` is the exclusive offset bound for which versions exist, i.e. the commit mark.
        let committed =
            self.reload_versions(fs, max_committed_id, had_preloaded)? as PointOffsetType;

        // Consume new mapping changes. Inserts are buffered until committed (their version exists);
        // deletes act on the committed mapping immediately, or cancel a still-pending insert.
        let changes = self.read_new_mapping_changes(fs, had_preloaded)?;

        for change in &changes {
            log::trace!(target: "live-reload", "Read mapping in {:?} change: {:?}", self.segment_path, change);
        }

        let mut deleted = Vec::new();
        for change in &changes {
            match *change {
                MappingChange::Insert(external_id, internal_id) => {
                    self.max_claimed_internal_id =
                        self.max_claimed_internal_id.max(Some(internal_id));
                    self.pending_inserts.insert(external_id, internal_id);
                }
                MappingChange::Delete(external_id) => {
                    // A point can be both committed (an old offset) and pending (a not-yet-committed
                    // re-insert at a new offset). A delete removes it from both. Report the deleted
                    // offset only if it was committed (and therefore previously reported).
                    self.pending_inserts.remove(&external_id);
                    if let Some(internal_id) = self.mappings.drop(external_id) {
                        deleted.push(internal_id);
                    }
                }
            }
        }

        let drained = self
            .pending_inserts
            .extract_if(|_, &mut internal_id| internal_id < committed);
        let mut inserted = Vec::new();
        for (external_id, internal_id) in drained {
            // An upsert re-links an existing external id to a new offset; the previously-committed
            // offset it displaces is now dead and must be reported as deleted.
            if let Some(previous) = self.mappings.set_link(external_id, internal_id)
                && previous != internal_id
            {
                deleted.push(previous);
            }
            inserted.push(internal_id);
        }

        // `extract_if` drains in arbitrary hash order; both result lists are sorted ascending.
        inserted.sort_unstable();
        deleted.sort_unstable();
        deleted.dedup();

        Ok(LiveReloadResult { inserted, deleted })
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
        if !preloaded {
            match self.mappings_file.as_mut() {
                Some(file) => {
                    // Refresh the handle to observe data appended by the writer. A lazily-opened handle whose
                    // object does not exist yet (e.g. S3) reports `NotFound` here or from `len`; treat that as
                    // an empty file.
                    file.live_reload().ok_not_found()?;
                }
                None => {
                    self.mappings_file = Self::try_open(fs, &mappings_path(&self.segment_path))?;
                }
            }
        }
        let Some(file) = self.mappings_file.as_mut() else {
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
        if !preloaded {
            match self.versions_file.as_mut() {
                Some(versions_file) => {
                    // Refresh the handle to observe data appended by the writer. A lazily-opened handle whose
                    // object does not exist yet (e.g. S3) reports `NotFound` here or from `len`; treat that as
                    // an empty file (no committed versions).
                    versions_file.live_reload().ok_not_found()?;
                }
                None => {
                    self.versions_file = Self::try_open(fs, &versions_path(&self.segment_path))?;
                }
            }
        }
        let Some(versions_file) = self.versions_file.as_mut() else {
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
