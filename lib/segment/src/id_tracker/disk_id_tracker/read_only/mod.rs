//! Disk-resident, read-only id tracker over the
//! [on-disk format](super::on_disk_format).
//!
//! Guarantees:
//!
//! - resident RAM does not scale with point count (only the
//!   [`DiskMappingReader`] core is held);
//! - read-by-id never loads the full deleted set — deletion is a single lazy
//!   `get_bit`;
//! - the full deleted set is materialized at most once, and only for paths
//!   that need the whole slice (search, scroll, counts, the
//!   [`live_reload`](ReadOnlyDiskIdTracker::live_reload) baseline).

mod id_tracker_read;
mod lifecycle;
mod live_reload;

use std::path::PathBuf;
use std::sync::OnceLock;

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{TypedStorage, UniversalRead};

use super::on_disk_format::{e2i_path, i2e_path, is_uuid_path};
use super::reader::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::{deleted_path, version_mapping_path};
use crate::types::SeqNumberType;

/// Read-only id tracker backed by the on-disk format files, read lazily
/// through a [`UniversalRead`] backend.
pub struct ReadOnlyDiskIdTracker<S: UniversalRead> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers, sparse index, `is_uuid`).
    reader: DiskMappingReader<S>,

    versions: TypedStorage<S, SeqNumberType>,
    versions_len: u64,
    /// Kept for per-point `get_bit`; replaced with a freshly opened handle on
    /// every [`Self::live_reload`].
    deleted_file: StoredBitSlice<S>,

    /// Full deleted set. NOT loaded on open or by point lookups. Materialized on
    /// the first search/scroll/count/reload and reused; invalidated by `live_reload`.
    deleted_full: OnceLock<BitVec>,
}

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            i2e_path(&self.path),
            e2i_path(&self.path),
            is_uuid_path(&self.path),
            version_mapping_path(&self.path),
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

    /// Whether the full deleted set has been materialized. Test-only: used to
    /// assert that read-by-id lookups do not trigger a full load.
    #[cfg(test)]
    pub(crate) fn deleted_full_materialized(&self) -> bool {
        self.deleted_full.get().is_some()
    }
}
