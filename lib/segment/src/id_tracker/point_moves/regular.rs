//! A move log found by a regular id tracker.
//!
//! Only the serverless writer appends to move logs, and only its read-only trackers expect them.
//! A moved copy has no tombstone, its moved-out record retires it, so a regular tracker that
//! ignored a log would show the old copy of a moved point next to the new one.

use std::io;
use std::path::Path;

use common::types::PointOffsetType;

use super::format::decode_entries;
use super::{moved_out_slots, point_moves_path};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTracker;

/// Delete the copies the move log of the segment at `segment_path` names as moved out, through the
/// regular delete path of `id_tracker`, which persists them as tombstones. No log is expected here:
/// finding one fails a debug assertion, and logs a warning otherwise.
pub fn apply_unexpected_point_moves(
    id_tracker: &mut impl IdTracker,
    segment_path: &Path,
) -> OperationResult<()> {
    let Some(moved_out) = read_moved_out(segment_path)? else {
        return Ok(());
    };

    let path = point_moves_path(segment_path);
    debug_assert!(
        false,
        "regular id tracker found a point move log at {}",
        path.display(),
    );
    log::warn!(
        "Regular id tracker found a point move log at {}, deleting the copies it moved out",
        path.display(),
    );

    delete_moved_out(id_tracker, &moved_out)
}

/// The local slots the moved-out records of the move log of the segment at `segment_path` name,
/// `None` without a log.
pub(super) fn read_moved_out(segment_path: &Path) -> OperationResult<Option<Vec<PointOffsetType>>> {
    let bytes = match fs_err::read(point_moves_path(segment_path)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let (entries, _) = decode_entries(&bytes);
    Ok(Some(moved_out_slots(&entries)))
}

/// Delete the copies on `slots` that `id_tracker` shows, through its regular delete path.
pub(super) fn delete_moved_out(
    id_tracker: &mut impl IdTracker,
    slots: &[PointOffsetType],
) -> OperationResult<()> {
    // A mask may be padded past the last point
    let points = id_tracker.total_point_count();
    for &internal_id in slots {
        if (internal_id as usize) < points && !id_tracker.is_deleted_point(internal_id) {
            id_tracker.drop_internal(internal_id)?;
        }
    }
    Ok(())
}
