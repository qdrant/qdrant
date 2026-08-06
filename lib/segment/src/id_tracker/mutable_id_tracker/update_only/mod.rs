#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::types::PointOffsetType;
use common::universal_io::{
    IsNotFound as _, OkNotFound as _, OpenOptions, Populate, UniversalAppend,
    UniversalWriteFileOps as _,
};

use super::change::{MappingChange, write_entry};
use super::mappings_storage::mappings_path;
use super::versions_storage::{
    VERSION_ELEMENT_SIZE, VersionsLayout, heal_versions_tail, version_offset, versions_byte_len,
    versions_path, write_version,
};
use crate::common::operation_error::{OperationError, OperationResult};
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
/// an insert claims a fresh slot above the highest one handed out so far and
/// supersedes the id's previous slot — hence *update-only*. A slot becomes
/// visible to readers exactly when the versions array covers it, so versions
/// may only extend that array contiguously; a hole or a rewrite is rejected
/// rather than written.
///
/// Every method that returns `Ok` has persisted what it wrote: it appends and
/// then runs the handle's [`Flusher`]. Nothing is buffered across calls, so
/// there is no separate flush step.
///
/// Two things are left to whoever opens the tracker. A crash between claiming a
/// slot and committing its version abandons that slot: `max_internal_id`
/// follows the mappings log, so it is not handed out again, but the versions
/// array then has a hole that every later
/// [`set_internal_versions`](Self::set_internal_versions) rejects until the
/// opener fills or tombstones it. And a torn tail in the mappings log: appends
/// land at the file's true end, which assumes the log ends at an entry
/// boundary. A torn tail in the versions array needs no opener — the next write
/// heals it (see [`heal_versions`](Self::heal_versions)).
///
/// [`Flusher`]: common::universal_io::Flusher
pub struct UpdateOnlyAppendableIdTracker<S: UniversalAppend> {
    segment_path: PathBuf,
    /// Highest slot handed out so far, `None` while none has been; advanced
    /// only once the entries claiming the new slots are durable.
    max_internal_id: Option<PointOffsetType>,
    /// Opens the two files, each created on its first write.
    fs: S::Fs,
}

impl<S: UniversalAppend> UpdateOnlyAppendableIdTracker<S> {
    pub fn new(
        fs: S::Fs,
        segment_path: impl Into<PathBuf>,
        max_internal_id: Option<PointOffsetType>,
    ) -> Self {
        Self {
            segment_path: segment_path.into(),
            max_internal_id,
            fs,
        }
    }
}

impl<S: UniversalAppend> UpdateOnlyAppendableIdTracker<S> {
    /// Commit `versions` for `internal_ids`, extending the dense versions array
    /// and publishing those slots to readers.
    ///
    /// The ids must be exactly the slots the array does not cover yet, in any
    /// order. Anything else — an already covered slot, a hole, a duplicate — is
    /// rejected and nothing is written.
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
            layout = self.heal_versions(&path, &file, file_len)?;
            // Whatever the healed file is, it is not the one this handle holds.
            file = self.open_append(&path)?;
        }
        let covered_slots = layout.committed_slots;

        let mut changes: Vec<(PointOffsetType, SeqNumberType)> = internal_ids
            .iter()
            .copied()
            .zip(versions.iter().copied())
            .collect();
        changes.sort_unstable_by_key(|(internal_id, _version)| *internal_id);

        let mut versions_buffer =
            Vec::with_capacity(versions_byte_len(changes.len() as u64) as usize);
        for (index, (internal_id, version)) in changes.iter().enumerate() {
            // Sorted ascending, the ids must run `covered_slots, covered_slots + 1, ...`:
            // anything lower is already published, anything higher leaves a
            // hole, and a duplicate trips the same check.
            let expected = covered_slots + index as u64;
            if u64::from(*internal_id) != expected {
                return Err(OperationError::service_error(format!(
                    "ID tracker versions can only be appended for consecutive slots: expected slot {expected}, got {internal_id} ({covered_slots} slots are already committed)",
                )));
            }
            debug_assert_eq!(
                version_offset(*internal_id),
                layout.committed_len() + versions_buffer.len() as u64,
                "version entry must land at its slot's offset",
            );

            write_version(&mut versions_buffer, *version)?;
        }

        // One entry per buffer: the backend places the whole run in a single
        // operation, and the entry boundaries stay visible to it.
        file.append_batch(
            layout.committed_len(),
            versions_buffer.chunks_exact(VERSION_ELEMENT_SIZE as usize),
        )?;
        (file.flusher())()?;

        Ok(())
    }

    /// Heal a versions file that ends mid-entry, per [`heal_versions_tail`],
    /// and return the layout it is left with.
    ///
    /// An append-only backend cannot truncate, so the file is shrunk the only
    /// way it can be: the committed prefix is read back out of it and put in
    /// its place as a whole file — one object write on a remote backend, one
    /// atomic replacement locally.
    ///
    /// `file` is invalidated by that write and must not be used again: it is
    /// the length before healing that its handle holds, and locally the file it
    /// was opened on has been replaced. The healed layout is returned so a
    /// caller that has already probed the length does not probe it again.
    fn heal_versions(
        &self,
        path: &Path,
        file: &S,
        file_len: u64,
    ) -> OperationResult<VersionsLayout> {
        heal_versions_tail(path, file_len, |healthy_len| {
            let committed = file.read_bytes(0..healthy_len, Sequential, align_of::<u8>())?;
            self.fs.atomic_save(path, &committed)?;
            Ok(())
        })
    }

    /// Record `operations` in the mappings log, in the given order, returning
    /// one `(external id, slot)` pair per insert; deletes claim no slot.
    ///
    /// Slots are consecutive above the highest one handed out so far, and stay
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
        let mut next_internal_id = match self.max_internal_id {
            Some(max_internal_id) => max_internal_id.checked_add(1),
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
                            self.max_internal_id,
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
        let end_of_file = Self::end_of_file(&file)?;
        // One entry per buffer, appended in order in a single operation.
        file.append_batch(
            end_of_file,
            entries.iter().map(|entry| &changes_buffer[entry.clone()]),
        )?;
        (file.flusher())()?;

        // Only now do the slots exist: a crash before this point leaves them
        // unclaimed, and the next call hands the very same ones out again.
        if let Some((_external_id, last_internal_id)) = inserted.last() {
            self.max_internal_id = Some(*last_internal_id);
        }

        Ok(inserted)
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
