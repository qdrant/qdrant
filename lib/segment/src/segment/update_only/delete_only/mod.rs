//! The write phase for an immutable segment: retiring points, and nothing
//! else.

use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, UniversalRead, UniversalWriteFileOps};

use super::DeleteOnlyIdTrackerState;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::types::PointIdType;

/// A segment open for deletes: nothing in it can grow, so the only thing a
/// batch can do here is retire points that are already there. Needs only
/// reads plus [`atomic_save`] from the backend, so object stores qualify.
///
/// [`atomic_save`]: UniversalWriteFileOps::atomic_save
pub struct DeleteOnlySegment<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> {
    fs: S::Fs,
    segment_path: PathBuf,
    /// Consumed by the first [`tombstone_points`](Self::tombstone_points) in
    /// place of reading the mask file.
    id_tracker_state: Option<DeleteOnlyIdTrackerState>,
}

impl<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> DeleteOnlySegment<S> {
    /// Open the segment directory at `segment_path` for deletes; nothing is
    /// read.
    pub fn open(
        fs: S::Fs,
        segment_path: &Path,
        id_tracker_state: Option<DeleteOnlyIdTrackerState>,
    ) -> Self {
        Self {
            fs,
            segment_path: segment_path.to_path_buf(),
            id_tracker_state,
        }
    }

    /// Retire the given points by marking the slots they occupy in the
    /// deleted-points bitmask (`id_tracker.deleted`) — the only thing
    /// written, the data on those slots stays. The mask is replaced whole,
    /// see [`StoredBitSlice::atomic_update`].
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        if points.is_empty() {
            return Ok(());
        }

        let seed = self.id_tracker_state.take().map(|state| state.deleted);

        StoredBitSlice::<S>::atomic_update(
            &self.fs,
            deleted_path(&self.segment_path),
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            seed,
            |mask| {
                for &(point_id, internal_id) in points {
                    let slot = internal_id as usize;
                    // A slot beyond the mask names a point this segment cannot hold.
                    if slot >= mask.len() {
                        return Err(OperationError::service_error(format!(
                            "cannot tombstone point {point_id} of segment {}: slot {internal_id} \
                             is beyond its deleted mask ({} slots)",
                            self.segment_path.display(),
                            mask.len(),
                        )));
                    }
                    mask.set(slot, true);
                }
                Ok(())
            },
        )?
    }
}
