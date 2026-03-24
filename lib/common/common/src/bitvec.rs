use bitvec::order::{BitOrder, Lsb0};
use bitvec::store::BitStore;

use crate::types::PointOffsetType;

pub type BitVec = bitvec::vec::BitVec<u64, Lsb0>;
pub type BitSlice = bitvec::slice::BitSlice<u64, Lsb0>;

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

pub trait BitSliceExt {
    /// Get a single bit from the slice.
    /// A convenience wrapper around [`BitSlice::get`].
    fn get_bit(&self, index: usize) -> Option<bool>;
}

impl<T: BitStore, O: BitOrder> BitSliceExt for bitvec::slice::BitSlice<T, O> {
    #[inline]
    fn get_bit(&self, index: usize) -> Option<bool> {
        self.get(index).as_deref().copied()
    }
}
