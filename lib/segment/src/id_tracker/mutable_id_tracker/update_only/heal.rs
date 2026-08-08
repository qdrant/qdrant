use std::cmp::Ordering;
use std::path::Path;

use common::generic_consts::Sequential;
use common::universal_io::{UniversalAppend, UniversalWriteFileOps as _};

use super::UpdateOnlyAppendableIdTracker;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::mutable_id_tracker::versions_storage::{VersionsLayout, heal_versions_tail};

impl<S: UniversalAppend> UpdateOnlyAppendableIdTracker<S> {
    /// Heal a versions file that ends mid-entry, per [`heal_versions_tail`], and return the layout
    /// it is left with.
    ///
    /// An append-only backend cannot truncate, so the file is shrunk the only way it can be: the
    /// committed prefix is read back and put in its place as a whole file.
    ///
    /// Takes ownership of `file` and drops it before the rewrite, Windows cannot replace a path
    /// that still has an mmap open. Callers open a fresh handle afterwards.
    pub(super) fn heal_versions(
        &self,
        path: &Path,
        file: S,
        file_len: u64,
    ) -> OperationResult<VersionsLayout> {
        heal_versions_tail(path, file_len, move |healthy_len| {
            let committed = file
                .read_bytes(0..healthy_len, Sequential, align_of::<u8>())?
                .to_vec();
            // Release the mmap before replacing the path.
            drop(file);
            self.fs.atomic_save(path, &committed)?;
            Ok(())
        })
    }

    /// Cut whatever sits past the end of the log off the mappings file, so the next append lands on
    /// an entry boundary.
    ///
    /// Reached only from a conflicted append. The excess is either a torn entry or a batch that
    /// landed unacknowledged; the two cannot be told apart without parsing the log, and need not
    /// be. Nothing the caller asked for is written yet and its slots are still unclaimed, since
    /// `max_claimed_internal_id` and `mappings_end` only move on a durable append, so both are
    /// answered by cutting back and writing the batch again where it belonged.
    ///
    /// Shrinks like [`heal_versions`](Self::heal_versions); what differs is the boundary, which the
    /// log cannot carry and [`new`](Self::new) is handed.
    ///
    /// The caller must drop any open handle on `path` first, Windows cannot replace a path that
    /// still has an mmap open. Opens its own handle, drops it before the rewrite, and leaves the
    /// caller to open a fresh one.
    pub(super) fn heal_mappings(&self, path: &Path) -> OperationResult<()> {
        let file = self.open_append(path)?;
        let file_len = Self::end_of_file(&file)?;

        match file_len.cmp(&self.mappings_end) {
            Ordering::Greater => {
                log::warn!(
                    "Mutable ID tracker mappings file holds {} bytes past the end of its log, dropping them: {}",
                    file_len - self.mappings_end,
                    path.display(),
                );
                let log = file
                    .read_bytes(0..self.mappings_end, Sequential, align_of::<u8>())?
                    .to_vec();
                // Release the mmap before replacing the path.
                drop(file);
                self.fs.atomic_save(path, &log)?;
                Ok(())
            }
            // Entries the log counts on are not in the file: it was truncated under us, or this
            // writer was handed an end that never existed. Either way appending to it would frame
            // the next entry off whatever is there.
            Ordering::Less => Err(OperationError::service_error(format!(
                "ID tracker mappings file is shorter than the log it should hold ({file_len} < {} bytes), cannot append mappings to {}",
                self.mappings_end,
                path.display(),
            ))),
            // Rejected at an offset the file does end at: someone else appended between the append
            // and this check, which the single-writer contract rules out.
            Ordering::Equal => Err(OperationError::service_error(format!(
                "ID tracker mappings file ends at the end of its log ({} bytes) yet rejected an append there, another writer is appending to {}",
                self.mappings_end,
                path.display(),
            ))),
        }
    }
}
