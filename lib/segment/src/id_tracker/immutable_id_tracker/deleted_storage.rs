use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, UniversalAppendFs};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::delete_versions::{DeleteLogEntry, DeleteLogWriter, DeleteVersioning};
use crate::types::{PointIdType, SeqNumberType};

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";

pub(crate) fn deleted_path(base: &Path) -> PathBuf {
    base.join(DELETED_FILE_NAME)
}

fn mask_options() -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
}

/// Retire `points` by setting the slots they occupy in the stored deleted mask of the segment at
/// `segment_path`. The mask is replaced whole, see [`StoredBitSlice::atomic_update`]; `seed` is
/// consumed in place of reading the mask file when the caller held it in memory, and kept for a
/// later call when `points` is empty and nothing is written.
///
/// The deletes go to the segment's delete log first, each with `version`, the version of the batch
/// that deleted the points, and only then to the mask, so the log never lags the mask. See
/// [`DeleteLogWriter::append`] for when there is a log to write to.
pub(crate) fn tombstone_points_in_stored_mask<Fs>(
    fs: &Fs,
    segment_path: &Path,
    seed: &mut Option<BitVec>,
    log: &mut DeleteLogWriter,
    points: &[(PointIdType, PointOffsetType)],
    version: SeqNumberType,
    versioning: DeleteVersioning,
) -> OperationResult<()>
where
    Fs: UniversalAppendFs,
{
    if points.is_empty() {
        return Ok(());
    }

    let mask = match seed.take() {
        Some(mask) => mask,
        None => {
            // Dropped before the save: Windows cannot replace a mapped file.
            let stored = StoredBitSlice::open(
                fs,
                deleted_path(segment_path),
                mask_options(),
                Default::default(),
            )?;
            stored.read_all()?.into_owned()
        }
    };

    for &(point_id, internal_id) in points {
        // A slot beyond the mask names a point this segment cannot hold.
        if internal_id as usize >= mask.len() {
            return Err(OperationError::service_error(format!(
                "cannot tombstone point {point_id} of segment {}: slot {internal_id} is beyond its \
                 deleted mask ({} slots)",
                segment_path.display(),
                mask.len(),
            )));
        }
    }

    let entries: Vec<DeleteLogEntry> = points
        .iter()
        .map(|&(_point_id, slot)| DeleteLogEntry { slot, version })
        .collect();
    log.append(fs, versioning, &mask, &entries)?;

    StoredBitSlice::atomic_update(
        fs,
        deleted_path(segment_path),
        mask_options(),
        Default::default(),
        Some(mask),
        |mask| {
            for &(_point_id, internal_id) in points {
                mask.set(internal_id as usize, true);
            }
            OperationResult::Ok(())
        },
    )?
}
