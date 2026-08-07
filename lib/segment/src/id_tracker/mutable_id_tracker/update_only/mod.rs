mod heal;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use common::mmap::{Advice, AdviceSetting};
use common::types::PointOffsetType;
use common::universal_io::{
    IsNotFound as _, OkNotFound as _, OpenOptions, Populate, UniversalAppend,
    UniversalWriteFileOps as _,
};

use super::change::{MappingChange, write_entry};
use super::mappings_storage::mappings_path;
use super::versions_storage::{
    VERSION_ELEMENT_SIZE, VersionsLayout, version_offset, versions_byte_len, versions_path,
    write_version,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::DELETED_POINT_VERSION;
use crate::types::{PointIdType, SeqNumberType};

/// A mapping mutation to record: claim a slot for an external id, or retire it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingOperation {
    /// Point this external id at a fresh slot, superseding whatever slot it
    /// held; the old slot keeps its data and stays unreachable by id.
    Insert(PointIdType),
    /// Retire this external id. Its slot is left as it is and never handed out
    /// again; tombstoning the data is the caller's business.
    Delete(PointIdType),
}

/// The write half of the appendable ID tracker for an update-only segment: it
/// records [`MappingOperation`]s into `mutable_id_tracker.mappings`, an
/// append-only log of [`MappingChange`] entries, and commits versions into
/// `mutable_id_tracker.versions`, a dense array of one version per slot.
/// Neither file is created until it is first written to.
///
/// Everything is appended through [`UniversalAppend`] and nothing is rewritten:
/// an insert claims a fresh slot above every slot the log has ever claimed and
/// supersedes the id's previous slot — hence *update-only*. A slot becomes
/// visible to readers exactly when the versions array covers it, so versions
/// may only extend that array; a rewrite of a covered slot is rejected rather
/// than written.
///
/// Every method that returns `Ok` has persisted what it wrote: it appends and
/// then runs the handle's [`Flusher`]. Nothing is buffered across calls, so
/// there is no separate flush step.
///
/// A crash between claiming a slot and committing its version abandons that
/// slot and the point on it: the data is whatever the writer got around to
/// writing, and which components wrote it is unknowable from here. The slot is
/// never handed out again — [`max_claimed_internal_id`](Self::max_claimed_internal_id)
/// counts it — and the point is retired for good, by a `Delete` [`new`](Self::new)
/// records for every inherited pending insert. A torn tail is healed by the next
/// write to either file, see
/// [`heal_versions`](Self::heal_versions) and
/// [`heal_mappings`](Self::heal_mappings).
///
/// [`Flusher`]: common::universal_io::Flusher
pub struct UpdateOnlyAppendableIdTracker<S: UniversalAppend> {
    segment_path: PathBuf,
    /// Highest slot the mappings log has claimed, `None` while it has claimed
    /// none; advanced only once the entries claiming the new slots are durable.
    ///
    /// Counts a claimed slot whether or not its point is still mapped and
    /// whether or not its version was ever committed, because a slot is spoken
    /// for the moment the log names it: components may have written data at it
    /// already.
    max_claimed_internal_id: Option<PointOffsetType>,
    /// Byte offset just past the last complete entry of the mappings log, where
    /// the next batch is appended; advanced only once that batch is durable.
    ///
    /// Carried rather than measured: entries vary in length, so the file's own
    /// length says nothing about where its last one ends. Appending here rather
    /// than at the end of the file makes a file that ends elsewhere a conflict.
    mappings_end: u64,
    /// Opens the two files, each created on its first write.
    fs: S::Fs,
}

impl<S: UniversalAppend> UpdateOnlyAppendableIdTracker<S> {
    /// All three of `max_claimed_internal_id`, `pending_inserts` and
    /// `mappings_end` must come from one and the same read of the mappings log —
    /// [`ReadOnlyAppendableIdTracker::max_claimed_internal_id`],
    /// [`ReadOnlyAppendableIdTracker::pending_inserts`] and
    /// [`ReadOnlyAppendableIdTracker::mappings_read_to`]. A segment with no log
    /// yet starts at `(None, [], 0)`.
    ///
    /// Take `max_claimed_internal_id` from the log and nowhere else. It is not
    /// the highest slot in use, and deriving it from what a reader exposes as
    /// its point set undercounts in two ways: a slot whose insert the log
    /// records but whose version was never committed is not in the mapping at
    /// all, and one whose external id was deleted afterwards is not even among
    /// the pending inserts. Both are nonetheless claimed — components may have
    /// written data at them — and handing one out again writes a second point
    /// over the remains of the first.
    ///
    /// `pending_inserts` is the other half of that: the points sitting on those
    /// claimed-but-unversioned slots. They are retired here and now, before this
    /// writer can be used at all — see
    /// [`retire_pending_inserts`](Self::retire_pending_inserts) — which is what
    /// makes opening a writer a write, and this fallible.
    ///
    /// `mappings_end` is not a hint: the first append cuts the file back to it
    /// (see [`heal_mappings`](Self::heal_mappings)), which drops a torn entry —
    /// or good data, if it lags for any other reason.
    ///
    /// [`ReadOnlyAppendableIdTracker::mappings_read_to`]:
    ///     super::read_only::ReadOnlyAppendableIdTracker::mappings_read_to
    /// [`ReadOnlyAppendableIdTracker::max_claimed_internal_id`]:
    ///     super::read_only::ReadOnlyAppendableIdTracker::max_claimed_internal_id
    /// [`ReadOnlyAppendableIdTracker::pending_inserts`]:
    ///     super::read_only::ReadOnlyAppendableIdTracker::pending_inserts
    pub fn new(
        fs: S::Fs,
        segment_path: impl Into<PathBuf>,
        max_claimed_internal_id: Option<PointOffsetType>,
        pending_inserts: impl IntoIterator<Item = PointIdType>,
        mappings_end: u64,
    ) -> OperationResult<Self> {
        let mut tracker = Self {
            segment_path: segment_path.into(),
            max_claimed_internal_id,
            mappings_end,
            fs,
        };
        tracker.retire_pending_inserts(pending_inserts)?;

        Ok(tracker)
    }

    /// Highest slot the mappings log has claimed, `None` while it has claimed
    /// none — including slots this writer claimed but has not versioned.
    pub fn max_claimed_internal_id(&self) -> Option<PointOffsetType> {
        self.max_claimed_internal_id
    }
}

impl<S: UniversalAppend> UpdateOnlyAppendableIdTracker<S> {
    /// Commit `versions` for `internal_ids`, extending the dense versions array
    /// and publishing those slots to readers.
    ///
    /// The ids may come in any order and must be slots this log has claimed
    /// that the array does not cover yet. An id the log never claimed, one the
    /// array already covers, and a duplicate are each rejected, and nothing is
    /// written.
    ///
    /// Claimed slots the call skips over are covered with
    /// [`DELETED_POINT_VERSION`] rather than refused. The array is dense, so
    /// there is no way to publish a slot without covering everything below it,
    /// and a skipped slot is one some writer claimed and abandoned. What keeps
    /// the point on such a slot from surfacing is not the value written here —
    /// [`DELETED_POINT_VERSION`] marks a deleted point but does not make one —
    /// it is the `Delete` that
    /// [`retire_pending_inserts`](Self::retire_pending_inserts) recorded when
    /// this writer was opened.
    ///
    /// [`DELETED_POINT_VERSION`]: crate::id_tracker::DELETED_POINT_VERSION
    pub fn set_internal_versions(
        &mut self,
        internal_ids: &[PointOffsetType],
        versions: &[SeqNumberType],
    ) -> OperationResult<()> {
        if internal_ids.len() != versions.len() {
            return Err(OperationError::service_error(format!(
                "Cannot set ID tracker versions: got {} internal ids and {} versions",
                internal_ids.len(),
                versions.len(),
            )));
        }

        if internal_ids.is_empty() {
            return Ok(());
        }

        let path = versions_path(&self.segment_path);
        let mut file = self.open_append(&path)?;

        // A torn tail is healed rather than refused: those bytes are a slot no
        // reader ever saw, and the entries below take their place.
        let file_len = Self::end_of_file(&file)?;
        let mut layout = VersionsLayout::of_len(file_len);
        if layout.partial_tail != 0 {
            // `heal_versions` takes the handle and drops it before rewriting.
            layout = self.heal_versions(&path, file, file_len)?;
            file = self.open_append(&path)?;
        }
        let covered_slots = layout.committed_slots;

        // The run to write, which the array can only take as a whole: from where
        // it ends through the highest id given.
        let &lowest_id = internal_ids.iter().min().expect("checked non-empty above");
        let &highest_id = internal_ids.iter().max().expect("checked non-empty above");

        if u64::from(lowest_id) < covered_slots {
            return Err(OperationError::service_error(format!(
                "ID tracker versions cannot be rewritten: slot {lowest_id} is among the {covered_slots} already committed",
            )));
        }
        // Publishing a slot means covering every slot below it, so a slot the
        // log never claimed cannot be committed even at the top of the range.
        if self.max_claimed_internal_id < Some(highest_id) {
            return Err(OperationError::service_error(format!(
                "ID tracker versions can only be set for slots the mappings log has claimed: slot {highest_id} was never claimed (the log claimed up to {:?})",
                self.max_claimed_internal_id,
            )));
        }

        // One entry per slot of the run, placed by slot. A slot left `None` is
        // one the call skipped, which the filler below covers.
        let slot_count = u64::from(highest_id) + 1 - covered_slots;
        let mut run = vec![None; slot_count as usize];
        for (internal_id, version) in internal_ids.iter().zip(versions) {
            let slot = &mut run[(u64::from(*internal_id) - covered_slots) as usize];
            if slot.replace(*version).is_some() {
                return Err(OperationError::service_error(format!(
                    "ID tracker versions can only be set once per slot: slot {internal_id} is given twice",
                )));
            }
        }

        let mut versions_buffer = Vec::with_capacity(versions_byte_len(slot_count) as usize);
        for version in run {
            write_version(
                &mut versions_buffer,
                version.unwrap_or(DELETED_POINT_VERSION),
            )?;
        }
        debug_assert_eq!(
            layout.committed_len() + versions_buffer.len() as u64,
            version_offset(highest_id) + VERSION_ELEMENT_SIZE,
            "the run must end just past the highest slot's entry",
        );

        // One entry per buffer: the backend places the whole run in a single
        // operation, and the entry boundaries stay visible to it.
        file.append_batch(
            layout.committed_len(),
            versions_buffer.chunks_exact(VERSION_ELEMENT_SIZE as usize),
        )?;
        (file.flusher())()?;

        Ok(())
    }

    /// Record `operations` in the mappings log, in the given order, returning
    /// one `(external id, slot)` pair per insert; deletes claim no slot.
    ///
    /// Slots are consecutive above every slot the log has claimed, and stay
    /// invisible to readers until
    /// [`set_internal_versions`](Self::set_internal_versions) covers them.
    pub fn insert_operations(
        &mut self,
        operations: &[MappingOperation],
    ) -> OperationResult<Vec<(PointIdType, PointOffsetType)>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        // `None` once the slot space is exhausted, which only matters if another
        // insert actually asks for one.
        let mut next_internal_id = match self.max_claimed_internal_id {
            Some(max_claimed_internal_id) => max_claimed_internal_id.checked_add(1),
            None => Some(0),
        };

        // Entries are variable-length, so their bounds within the buffer are
        // recorded as they are written rather than derived afterwards.
        let mut changes_buffer = Vec::new();
        let mut entries = Vec::with_capacity(operations.len());
        let mut inserted = Vec::new();
        for operation in operations {
            let change = match operation {
                MappingOperation::Insert(external_id) => {
                    let internal_id = next_internal_id.ok_or_else(|| {
                        OperationError::service_error(format!(
                            "ID tracker ran out of internal ids at {:?}, cannot insert {external_id}",
                            self.max_claimed_internal_id,
                        ))
                    })?;
                    next_internal_id = internal_id.checked_add(1);
                    inserted.push((*external_id, internal_id));
                    MappingChange::Insert(*external_id, internal_id)
                }
                MappingOperation::Delete(external_id) => MappingChange::Delete(*external_id),
            };

            let start = changes_buffer.len();
            write_entry(&mut changes_buffer, change)?;
            entries.push(start..changes_buffer.len());
        }

        let path = mappings_path(&self.segment_path);
        let mut file = self.open_append(&path)?;

        // One entry per buffer, appended in order in a single operation, at the
        // end of the log rather than the end of the file — the two part ways
        // only after a write that tore or landed unacknowledged, and this is
        // where that becomes a conflict.
        let batch = || entries.iter().map(|entry| &changes_buffer[entry.clone()]);
        if let Err(err) = file.append_batch(self.mappings_end, batch()) {
            if !err.is_append_offset_conflict() {
                return Err(err.into());
            }
            // The conflict wrote nothing, so the file can be cut back to the
            // log's end and the batch appended anew. Drop this handle first —
            // healing replaces the path, which Windows refuses while an mmap
            // is still open.
            drop(file);
            self.heal_mappings(&path)?;
            file = self.open_append(&path)?;
            file.append_batch(self.mappings_end, batch())?;
        }
        (file.flusher())()?;

        // Only now do the entries and the slots they claim exist: a crash
        // before this point leaves the log ending where it did, and the next
        // call writes the very same entries at the very same offset.
        // `changes_buffer` is exactly the entries, back to back.
        self.mappings_end += changes_buffer.len() as u64;
        if let Some((_external_id, last_internal_id)) = inserted.last() {
            self.max_claimed_internal_id = Some(*last_internal_id);
        }

        Ok(inserted)
    }

    /// Retire every insert this writer inherited: each point whose slot the
    /// mappings log claimed and whose version no writer ever committed, recorded
    /// as a `Delete` in the log.
    ///
    /// Retired rather than adopted because such a point is in no state anyone
    /// can hand back. Its data was written by a writer that stopped partway, so
    /// some components hold it and others do not, and which is unknowable from
    /// here. Nor can the point simply be left alone: its slot has to be covered
    /// for any slot above it to be published, and the moment it is, readers take
    /// the point for committed and start serving whatever happens to sit on
    /// those components.
    ///
    /// This costs the point rather than only the unacknowledged update that
    /// created it — an update that abandoned its new slot has, by the same
    /// partial write, likely tombstoned the old one already, so there is no
    /// earlier state left to fall back to either.
    ///
    /// Runs at construction rather than lazily on the first write, so that no
    /// later write path can be added that forgets it and publishes one of these
    /// points. Writes nothing when there is nothing to retire.
    fn retire_pending_inserts(
        &mut self,
        pending_inserts: impl IntoIterator<Item = PointIdType>,
    ) -> OperationResult<()> {
        let operations: Vec<MappingOperation> = pending_inserts
            .into_iter()
            .map(MappingOperation::Delete)
            .collect();
        self.insert_operations(&operations)?;

        Ok(())
    }

    fn open_options() -> OpenOptions {
        OpenOptions {
            // Forced on by `open_append` regardless; spelled out for clarity.
            writeable: true,
            need_sequential: false,
            // Appends never read back, and on a remote backend populating would
            // fetch the whole file per call.
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Normal),
        }
    }

    /// Open the append handle for `path`, creating the file if it is not there
    /// yet.
    fn open_append(&self, path: &Path) -> OperationResult<S> {
        match self.fs.open_append(path, Self::open_options()) {
            Ok(file) => Ok(file),
            Err(err) if err.is_not_found() => {
                self.fs.create(path, 0)?;
                Ok(self.fs.open_append(path, Self::open_options())?)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// The file's end: the offset the next append must land at, and the
    /// compare-and-swap token that makes it checkable — a file that has moved
    /// on since this probe conflicts instead of being written twice or in the
    /// wrong place.
    ///
    /// `NotFound` means a lazy backend has not materialized the object yet, so
    /// it is empty and the append at 0 creates it.
    fn end_of_file(file: &S) -> OperationResult<u64> {
        Ok(file.len::<u8>().ok_not_found()?.unwrap_or(0))
    }
}
