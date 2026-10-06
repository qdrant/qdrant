//! The write half of the in-RAM immutable id tracker for an update-only
//! segment.

use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppendFs, UniversalWriteFs};

use super::deleted_storage::tombstone_points_in_stored_mask;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::point_moves::{PointMovesWriter, Retirement, moved_out_entries};
use crate::types::PointIdType;

/// The mapping is frozen, so the one thing to write is the stored deleted
/// mask ([`DELETED_FILE_NAME`](super::DELETED_FILE_NAME)). Needs only reads
/// plus [`atomic_save`] from the backend, so object stores qualify.
///
/// [`atomic_save`]: UniversalWriteFs::atomic_save
pub struct UpdateOnlyImmutableIdTracker {
    segment_path: PathBuf,
    /// Consumed by the first [`tombstone_points`](Self::tombstone_points) in
    /// place of reading the mask file.
    deleted: Option<BitVec>,
    /// The segment's move log, see [`point_moves`](crate::id_tracker::point_moves).
    moves: PointMovesWriter,
}

impl UpdateOnlyImmutableIdTracker {
    /// `deleted` is the mask as the read phase held it in memory, when it
    /// did; nothing is read here.
    pub fn new(segment_path: &Path, deleted: Option<BitVec>) -> OperationResult<Self> {
        Ok(Self {
            segment_path: segment_path.to_path_buf(),
            deleted,
            moves: PointMovesWriter::new(segment_path),
        })
    }

    /// Retire the points of `retirements`. The moved-out records of the moves among them are
    /// appended first and the mask is replaced second, so a follower that reads a tombstone can
    /// also read the move it belongs to.
    pub fn retire_points<Fs>(&mut self, fs: &Fs, retirements: &[Retirement]) -> OperationResult<()>
    where
        Fs: UniversalAppendFs,
    {
        self.moves.append(fs, &moved_out_entries(retirements))?;
        let points: Vec<(PointIdType, PointOffsetType)> = retirements
            .iter()
            .map(|retirement| (retirement.id, retirement.slot))
            .collect();
        tombstone_points_in_stored_mask(fs, &self.segment_path, &mut self.deleted, &points)
    }

    /// Retire the given points by marking the slots they occupy in the stored
    /// deleted mask — the only thing written, the data on those slots stays.
    /// The mask is replaced whole, see [`StoredBitSlice::atomic_update`].
    ///
    /// [`StoredBitSlice::atomic_update`]: common::stored_bitslice::StoredBitSlice::atomic_update
    pub fn tombstone_points<Fs>(
        &mut self,
        fs: &Fs,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()>
    where
        Fs: UniversalWriteFs,
    {
        tombstone_points_in_stored_mask(fs, &self.segment_path, &mut self.deleted, points)
    }
}
