//! The write half of the in-RAM immutable id tracker for an update-only
//! segment.

use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;

use super::deleted_storage::tombstone_points_in_segment;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::delete_versions::{DeleteLogWriter, DeleteVersioning};
use crate::types::{PointIdType, SeqNumberType};

/// The mapping is frozen, so the one thing to write is the deletes: to the
/// delete log, or the stored deleted mask
/// ([`DELETED_FILE_NAME`](super::DELETED_FILE_NAME)) without delete versions.
/// Needs only reads, appends and [`atomic_save`] from the backend, so object
/// stores qualify.
///
/// [`atomic_save`]: common::universal_io::UniversalWriteFs::atomic_save
pub struct UpdateOnlyImmutableIdTracker {
    segment_path: PathBuf,
    /// Consumed by the first [`tombstone_points`](Self::tombstone_points) in
    /// place of reading the mask file.
    deleted: Option<BitVec>,
    /// Records the deletes with their versions.
    log: DeleteLogWriter,
}

impl UpdateOnlyImmutableIdTracker {
    /// `deleted` is the mask as the read phase held it in memory, when it
    /// did; nothing is read here.
    pub fn new(segment_path: &Path, deleted: Option<BitVec>) -> OperationResult<Self> {
        Ok(Self {
            segment_path: segment_path.to_path_buf(),
            deleted,
            log: DeleteLogWriter::new(segment_path),
        })
    }

    /// Retire the given points — the data on their slots stays — in the delete
    /// log or the deleted mask, see [`tombstone_points_in_segment`].
    pub fn tombstone_points<Fs>(
        &mut self,
        fs: &Fs,
        points: &[(PointIdType, PointOffsetType)],
        version: SeqNumberType,
        versioning: DeleteVersioning,
    ) -> OperationResult<()>
    where
        Fs: UniversalAppendFs,
    {
        tombstone_points_in_segment(
            fs,
            &self.segment_path,
            &mut self.deleted,
            &mut self.log,
            points,
            version,
            versioning,
        )
    }
}
