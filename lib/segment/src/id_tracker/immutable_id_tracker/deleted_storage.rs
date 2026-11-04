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

/// Retire `points` in the segment at `segment_path`, see
/// [`delete_versions`](crate::id_tracker::delete_versions).
///
/// With [`DeleteVersioning::Record`], the deletes are appended to the segment's delete log, each
/// with `version`, the version of the batch that deleted the points; the mask stays as it is.
///
/// With [`DeleteVersioning::Legacy`], the slots are set in the stored deleted mask, which is
/// replaced whole, see [`StoredBitSlice::atomic_update`]. `seed` is consumed in place of reading
/// the mask file when the caller held it in memory, and kept for a later call when nothing is
/// written.
pub(crate) fn tombstone_points_in_segment<Fs>(
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

    match versioning {
        DeleteVersioning::Record => {
            let slot_count = match seed {
                Some(mask) => mask.len(),
                None => open_mask(fs, segment_path)?.bit_len() as usize,
            };
            check_slots(segment_path, slot_count, points)?;
            let entries: Vec<DeleteLogEntry> = points
                .iter()
                .map(|&(_point_id, slot)| DeleteLogEntry { slot, version })
                .collect();
            log.append(fs, &entries)
        }
        DeleteVersioning::Legacy => {
            let mask = match seed.take() {
                Some(mask) => mask,
                // Dropped before the save: Windows cannot replace a mapped file.
                None => open_mask(fs, segment_path)?.read_all()?.into_owned(),
            };
            check_slots(segment_path, mask.len(), points)?;
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
    }
}

fn open_mask<Fs: UniversalAppendFs>(
    fs: &Fs,
    segment_path: &Path,
) -> OperationResult<StoredBitSlice<Fs::File>> {
    Ok(StoredBitSlice::open(
        fs,
        deleted_path(segment_path),
        mask_options(),
        Default::default(),
    )?)
}

/// A slot beyond the mask names a point this segment cannot hold.
fn check_slots(
    segment_path: &Path,
    slot_count: usize,
    points: &[(PointIdType, PointOffsetType)],
) -> OperationResult<()> {
    for &(point_id, internal_id) in points {
        if internal_id as usize >= slot_count {
            return Err(OperationError::service_error(format!(
                "cannot tombstone point {point_id} of segment {}: slot {internal_id} is beyond its \
                 deleted mask ({slot_count} slots)",
                segment_path.display(),
            )));
        }
    }
    Ok(())
}
