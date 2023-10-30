use bitvec::vec::BitVec;
use common::types::PointOffsetType;

/// Set deleted state in given bitvec.
///
/// Grows bitvec automatically if it is not big enough.
///
/// Returns previous deleted state of the given point.
#[inline]
pub fn bitvec_set_deleted(bitvec: &mut BitVec, point_id: PointOffsetType, deleted: bool) -> bool {
    // Set deleted flag if bitvec is large enough, no need to check bounds
    if (point_id as usize) < bitvec.len() {
        return unsafe { bitvec.replace_unchecked(point_id as usize, deleted) };
    }

    // Bitvec is too small; grow and set the deletion flag, no need to check bounds
    if deleted {
        bitvec.resize(point_id as usize + 1, false);
        unsafe { bitvec.set_unchecked(point_id as usize, true) };
    }
    false
}
