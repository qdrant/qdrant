use std::io::Cursor;

use common::generic_consts::Sequential;
use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};

use super::ReadOnlyAppendableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::change::MappingChange;
use crate::id_tracker::mutable_id_tracker::mappings_storage::read_mappings_iter;
use crate::id_tracker::mutable_id_tracker::versions_storage::VERSION_ELEMENT_SIZE;
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

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Consume mapping and version changes appended to storage since the last reload.
    ///
    /// File handles are refreshed via [`UniversalRead::reopen`] so data appended by the writer
    /// becomes visible. Both result lists are sorted ascending.
    ///
    /// The writer flushes mappings before data before versions, so a point's version appears last
    /// and marks it as fully committed. Inserts are therefore driven by the versions file: an
    /// offset is reported as inserted only once its version becomes readable (and it is still live
    /// in the mapping). A point that is mapped but whose version is not flushed yet is intentionally
    /// withheld (its data may be partial) and reported on a later reload once its version lands.
    /// Deletes are driven by the mapping and need no version, a deleted point's version is
    /// considered gone.
    pub fn live_reload(&mut self) -> OperationResult<LiveReloadResult> {
        // Number of committed versions before this reload. Offsets below this were already reported.
        let committed_before = self.internal_to_version.len() as PointOffsetType;

        // Apply new mapping changes, collecting offsets that a delete removed from the mapping.
        let changes = self.read_new_mapping_changes()?;
        let mut deleted = Vec::new();
        for change in &changes {
            match *change {
                MappingChange::Insert(external_id, internal_id) => {
                    self.mappings.set_link(external_id, internal_id);
                }
                MappingChange::Delete(external_id) => {
                    // Resolve the internal id before dropping, the mapping is gone afterwards.
                    // A successful resolve means the point was live right before this delete.
                    if let Some(internal_id) = self.mappings.internal_id(&external_id) {
                        self.mappings.drop(external_id);
                        deleted.push(internal_id);
                    }
                }
            }
        }

        // Append versions flushed since the last reload (mappings are flushed before versions).
        let committed_now = self.reload_versions()? as PointOffsetType;

        // Inserts: offsets whose version just became readable and that are live in the mapping.
        let inserted: Vec<PointOffsetType> = (committed_before..committed_now)
            .filter(|&internal_id| !self.mappings.is_deleted_point(internal_id))
            .collect();

        // Deletes: keep only offsets that were previously committed (so we reported them as
        // inserted) and are still deleted after applying the whole batch (guards re-inserts).
        deleted.retain(|&internal_id| {
            internal_id < committed_before && self.mappings.is_deleted_point(internal_id)
        });
        deleted.sort_unstable();
        deleted.dedup();

        Ok(LiveReloadResult { inserted, deleted })
    }

    /// Read mapping changes appended after the last consumed offset, advancing `mappings_read_to`.
    ///
    /// The read stops at the last fully-readable entry; a partial trailing entry is left in place
    /// so it can be consumed on a later reload once the writer flushed it completely.
    fn read_new_mapping_changes(&mut self) -> OperationResult<Vec<MappingChange>> {
        // Refresh the handle to observe data appended by the writer.
        self.mappings_file.reopen()?;
        let file = &self.mappings_file;

        let file_len = file.len::<u8>()?;

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

        let bytes = file.read::<Sequential, u8>(ReadRange {
            byte_offset: start,
            length: file_len - start,
        })?;
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
    fn reload_versions(&mut self) -> OperationResult<usize> {
        // Refresh the handle to observe data appended by the writer.
        self.versions_file.reopen()?;

        // Split the borrow so the read (from `versions_file`) can extend `internal_to_version`.
        let Self {
            segment_path: _,
            internal_to_version,
            mappings: _,
            mappings_read_to: _,
            mappings_file: _,
            versions_file,
        } = self;

        let loaded_len = internal_to_version.len() as u64;

        // Floor the raw byte length to whole elements: a partially-written trailing version (a torn
        // flush) is ignored, only fully-written versions are loaded. We read the byte length rather
        // than `len::<SeqNumberType>()` on purpose, some backends debug-assert the file length is a
        // whole number of elements, which a torn flush violates.
        let versions_len = versions_file.len::<u8>()? / VERSION_ELEMENT_SIZE;

        // Append the newly flushed tail. Anything beyond `versions_len` is not flushed yet and
        // stays absent until a later reload (the mapped-but-versionless case).
        if versions_len > loaded_len {
            let tail = versions_file.read::<Sequential, SeqNumberType>(ReadRange {
                byte_offset: loaded_len * VERSION_ELEMENT_SIZE,
                length: versions_len - loaded_len,
            })?;
            internal_to_version.extend_from_slice(&tail);
        }

        Ok(internal_to_version.len())
    }
}
