use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, UniversalRead, UniversalWriteFileOps};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::PointIdType;

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";

pub(crate) fn deleted_path(base: &Path) -> PathBuf {
    base.join(DELETED_FILE_NAME)
}

/// Retire `points` by setting the slots they occupy in the stored deleted
/// mask of the segment at `segment_path`. The mask is replaced whole, see
/// [`StoredBitSlice::atomic_update`]; `seed` is consumed in place of reading
/// the mask file when the caller held it in memory — and kept for a later
/// call when `points` is empty and nothing is written.
pub(crate) fn tombstone_points_in_stored_mask<S: UniversalRead<Fs: UniversalWriteFileOps>>(
    fs: &S::Fs,
    segment_path: &Path,
    seed: &mut Option<BitVec>,
    points: &[(PointIdType, PointOffsetType)],
) -> OperationResult<()> {
    if points.is_empty() {
        return Ok(());
    }
    StoredBitSlice::<S>::atomic_update(
        fs,
        deleted_path(segment_path),
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        },
        Default::default(),
        seed.take(),
        |mask| {
            for &(point_id, internal_id) in points {
                let slot = internal_id as usize;
                // A slot beyond the mask names a point this segment cannot hold.
                if slot >= mask.len() {
                    return Err(OperationError::service_error(format!(
                        "cannot tombstone point {point_id} of segment {}: slot {internal_id} \
                         is beyond its deleted mask ({} slots)",
                        segment_path.display(),
                        mask.len(),
                    )));
                }
                mask.set(slot, true);
            }
            Ok(())
        },
    )?
}
