use bitvec::order::{BitOrder, Lsb0};
use bitvec::store::BitStore;

use crate::types::PointOffsetType;

/// Element type backing every [`BitVec`]/[`BitSlice`] in the codebase, including the on-disk
/// representations in [`stored_bitslice`](crate::stored_bitslice) and
/// [`stored_bitmask`](crate::stored_bitmask).
///
/// `bitvec` only implements `BitStore` for integers up to the target's pointer width, so 32-bit
/// targets (wasm32) cannot use `u64`. The narrower element is format-compatible: with `Lsb0`
/// ordering on a little-endian target, global bit `i` lives in byte `i / 8` at bit `i % 8`
/// regardless of element width, so a file written by either reads identically on the other —
/// except that a 32-bit writer may leave a length that is a multiple of 4 but not of 8, whose last
/// four bytes a 64-bit reader would drop. 32-bit builds are read-only, so that does not arise.
#[cfg(target_pointer_width = "64")]
pub type BitStoreType = u64;
#[cfg(not(target_pointer_width = "64"))]
pub type BitStoreType = u32;

pub type BitVec = bitvec::vec::BitVec<BitStoreType, Lsb0>;
pub type BitSlice = bitvec::slice::BitSlice<BitStoreType, Lsb0>;

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

/// Wrapper around [BitVec] with following assumptions:
/// - Out-of-range points are considered deleted/not active.
/// - The bitvec never grows.
pub struct DeletedBitVec {
    bits: BitVec,
    count: usize,
}

impl DeletedBitVec {
    pub fn new(deleted: BitVec) -> Self {
        Self {
            count: deleted.count_ones(),
            bits: deleted,
        }
    }

    #[inline]
    pub fn is_active(&self, point_id: PointOffsetType) -> bool {
        self.bits.get_bit(point_id as usize) == Some(false)
    }

    pub fn mark_deleted(&mut self, point_id: PointOffsetType) -> bool {
        let newly_marked = self.is_active(point_id);
        if newly_marked {
            self.bits.set(point_id as usize, true);
            self.count += 1;
        }
        newly_marked
    }

    pub fn deleted_count(&self) -> usize {
        self.count
    }

    pub fn active_count(&self) -> usize {
        self.bits.len() - self.count
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.bits.capacity().div_ceil(u8::BITS as usize)
    }
}
