//! Delete-only writer for [`DiskIdTracker`](super::DiskIdTracker)'s `deleted`
//! file, for backends that can only append, never seek-and-overwrite.
//!
//! `DiskIdTracker` builds the segment (the immutable mapping plus the initial
//! `deleted`/`versions` state); this type only ever [`open`](Self::open)s an
//! already-built segment and applies further deletions to it. Deleting a
//! point in [`DiskIdTracker`] seeks and flips its bit in place, which needs
//! random-offset writes ([`UniversalWrite`](common::universal_io::UniversalWrite));
//! this type instead rewrites the file whole through
//! [`UniversalWriteFileOps::atomic_save`](common::universal_io::UniversalWriteFileOps::atomic_save)
//! on every [`delete_batch`](Self::delete_batch) call, which only needs
//! [`UniversalAppend`] (append-only backends implement `atomic_save` too,
//! e.g. as a full-object overwrite). The file stays byte-for-byte identical
//! to [`DiskIdTracker`]'s format, so [`DiskIdTracker`]/[`ReadOnlyDiskIdTracker`](super::ReadOnlyDiskIdTracker)
//! can open the result unchanged.
//!
//! It does not touch the `versions` file — deletion here is purely the
//! `deleted` bit, not a version bump — nor is there a read surface (no
//! [`IdTrackerRead`](crate::id_tracker::IdTrackerRead)) or mapping lookup:
//! callers already know the internal offsets to delete, so nothing here needs
//! the `i2e`/`e2i` mapping reader.
//!
//! Rewriting the whole file on every call is wasteful for a segment with
//! frequent deletions; an incremental append-only delete log is a natural
//! follow-up once that matters.

mod delete;
mod lifecycle;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use common::bitvec::BitVec;
use common::universal_io::UniversalAppend;

/// Delete-only, append-only-backend writer over an existing
/// [`DiskIdTracker`](super::DiskIdTracker) segment. See the module docs.
#[derive(Debug)]
pub struct UpdateOnlyDiskIdTracker<S: UniversalAppend> {
    path: PathBuf,
    /// Filesystem handle, retained to run [`atomic_save`](common::universal_io::UniversalWriteFileOps::atomic_save)
    /// from [`delete_batch`](Self::delete_batch).
    fs: S::Fs,

    /// Resident deleted set; rewritten whole on every [`delete_batch`](Self::delete_batch) call.
    deleted: BitVec,
}

impl<S: UniversalAppend> UpdateOnlyDiskIdTracker<S> {
    /// Approximate resident RAM: the deleted bitvec.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            fs: _,
            deleted,
        } = self;
        deleted.capacity().div_ceil(u8::BITS as usize)
    }
}
