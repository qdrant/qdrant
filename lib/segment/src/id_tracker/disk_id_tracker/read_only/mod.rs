//! Disk-resident, read-only id tracker over the
//! [on-disk format](super::on_disk_format).
//!
//! Guarantees:
//!
//! - resident RAM does not scale with point count (only the
//!   [`DiskMappingReader`] core is held), except for
//!   [compact versions](super::compact_versions), held at 4 bytes per point;
//! - read-by-id never loads the full deleted set — deletion is a single lazy
//!   `get_bit`;
//! - the full deleted set is materialized at most once, and only for paths
//!   that need the whole slice (search, scroll, counts, the
//!   [`live_reload`](ReadOnlyDiskIdTracker::live_reload) baseline).

mod id_tracker_read;
mod lifecycle;
mod live_reload;
#[cfg(test)]
mod moves_tests;
mod versions;

use std::path::PathBuf;
use std::sync::OnceLock;

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::UniversalRead;

use self::versions::ReadOnlyVersions;
use super::on_disk_format::{e2i_path, i2e_path, is_uuid_path};
use super::reader::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::point_moves::SlotMoves;

/// Read-only id tracker backed by the on-disk format files, read lazily
/// through a [`UniversalRead`] backend.
pub struct ReadOnlyDiskIdTracker<S: UniversalRead> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers, sparse index, `is_uuid`).
    reader: DiskMappingReader<S>,

    versions: ReadOnlyVersions<S>,
    /// Kept for per-point `get_bit`; replaced with a freshly opened handle on
    /// every [`Self::live_reload`].
    deleted_file: StoredBitSlice<S>,

    /// Full deleted set. NOT loaded on open or by point lookups. Materialized on
    /// the first search/scroll/count/reload and reused; invalidated by `live_reload`.
    deleted_full: OnceLock<BitVec>,

    /// Point moves, when this tracker resolves them. Deletion is then answered from
    /// [`DiskMoves::effective`] instead of the file, see
    /// [`point_moves`](crate::id_tracker::point_moves).
    moves: Option<Box<DiskMoves<S>>>,
}

/// The point moves of a disk-resident tracker that resolves them.
#[derive(Debug)]
pub(super) struct DiskMoves<S: UniversalRead> {
    pub(super) moves: SlotMoves<S>,
    /// The deleted mask as last read: the baseline new tombstones are found against.
    pub(super) raw: BitVec,
    /// What reads see: the mask without the held tombstones, plus the slots settled moves masked.
    pub(super) effective: BitVec,
}

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            i2e_path(&self.path),
            e2i_path(&self.path),
            is_uuid_path(&self.path),
            self.versions.path(&self.path),
            deleted_path(&self.path),
        ]
    }

    /// Lazily materialize the full deleted set; never called by point lookups.
    /// Storage errors propagate.
    ///
    /// Manual fallible init (std `OnceLock` has no stable `get_or_try_init`): on a
    /// race both threads read the same on-disk state (`live_reload` needs `&mut`,
    /// so it can't interleave), so the loser's `set` failing is harmless.
    fn deleted_full(&self) -> OperationResult<&BitVec> {
        if let Some(materialized) = self.deleted_full.get() {
            return Ok(materialized);
        }
        let materialized = self.deleted_file.read_all()?.into_owned();
        let _ = self.deleted_full.set(materialized);
        Ok(self.deleted_full.get().expect("just set"))
    }

    /// The full deleted set, if already materialized; never triggers the load.
    pub fn deleted_full_if_materialized(&self) -> Option<&BitVec> {
        match &self.moves {
            Some(moves) => Some(&moves.effective),
            None => self.deleted_full.get(),
        }
    }
}
