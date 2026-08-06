use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;

use crate::common::buffered_update_bitslice::BitmaskPaths;

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";

/// Compact-format counterpart of [`DELETED_FILE_NAME`]; only one of the two
/// exists in a segment.
pub const DELETED_MASK_FILE_NAME: &str = "id_tracker.deleted_mask";

pub(crate) fn deleted_path(base: &Path) -> PathBuf {
    base.join(DELETED_FILE_NAME)
}

/// Both file names the deleted flags may be persisted under.
pub(crate) fn deleted_paths(base: &Path) -> BitmaskPaths {
    BitmaskPaths::new(deleted_path(base), base.join(DELETED_MASK_FILE_NAME))
}

/// Ascending offsets of the deleted points of a tracker holding
/// `total_point_count` points.
///
/// `deleted` covers the points the mappings know about; anything past its end
/// is a point the mappings never got, and counts as deleted.
pub(crate) fn deleted_offsets(
    deleted: &BitSlice,
    total_point_count: usize,
) -> impl Iterator<Item = u64> + '_ {
    debug_assert!(deleted.len() <= total_point_count);
    deleted
        .iter_ones()
        .map(|offset| offset as u64)
        .chain(deleted.len() as u64..total_point_count as u64)
}
