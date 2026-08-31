//! The write half of the disk-resident id tracker for an update-only segment.

use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::tombstone_points_in_stored_mask;
use crate::types::PointIdType;

/// The mapping is frozen, so the one thing to write is the stored deleted
/// mask — which the disk-resident format keeps in the same file as the in-RAM
/// immutable format, hence the shared implementation. Needs only reads plus
/// [`atomic_save`] from the backend, so object stores qualify.
///
/// [`atomic_save`]: UniversalWriteFileOps::atomic_save
pub struct UpdateOnlyDiskIdTracker<Fs: UniversalAppendFs> {
    fs: Fs,
    segment_path: PathBuf,
    /// Consumed by the first [`tombstone_points`](Self::tombstone_points) in
    /// place of reading the mask file.
    deleted: Option<BitVec>,
}

impl<Fs: UniversalAppendFs> UpdateOnlyDiskIdTracker<Fs> {
    /// `deleted` is the mask as the read phase held it in memory, when it
    /// did; nothing is read here.
    pub fn new(fs: Fs, segment_path: &Path, deleted: Option<BitVec>) -> Self {
        Self {
            fs,
            segment_path: segment_path.to_path_buf(),
            deleted,
        }
    }

    /// Retire the given points by marking the slots they occupy in the stored
    /// deleted mask — the only thing written, the data on those slots stays.
    /// The mask is replaced whole, see [`StoredBitSlice::atomic_update`].
    ///
    /// [`StoredBitSlice::atomic_update`]: common::stored_bitslice::StoredBitSlice::atomic_update
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        tombstone_points_in_stored_mask(&self.fs, &self.segment_path, &mut self.deleted, points)
    }
}
