//! The write half of the in-RAM immutable id tracker for an update-only
//! segment.

use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalWriteFileOps};

use super::deleted_storage::tombstone_points_in_stored_mask;
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// The mapping is frozen, so the one thing to write is the stored deleted
/// mask ([`DELETED_FILE_NAME`](super::DELETED_FILE_NAME)). Needs only reads
/// plus [`atomic_save`] from the backend, so object stores qualify.
///
/// [`atomic_save`]: UniversalWriteFileOps::atomic_save
pub struct UpdateOnlyImmutableIdTracker<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> {
    fs: S::Fs,
    segment_path: PathBuf,
    /// Consumed by the first [`tombstone_points`](Self::tombstone_points) in
    /// place of reading the mask file.
    deleted: Option<BitVec>,
}

impl<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> UpdateOnlyImmutableIdTracker<S> {
    /// `deleted` is the mask as the read phase held it in memory, when it
    /// did; nothing is read here.
    pub fn new(fs: S::Fs, segment_path: &Path, deleted: Option<BitVec>) -> Self {
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
        if points.is_empty() {
            return Ok(());
        }
        let seed = self.deleted.take();
        tombstone_points_in_stored_mask::<S>(&self.fs, &self.segment_path, seed, points)
    }
}
