use bitvec::order::BitOrder;
use bitvec::slice::BitSlice;
use bitvec::store::BitStore;
use bitvec::vec::BitVec;

pub trait OptionExt {
    /// `replace` if the given `value` is `Some`
    fn replace_if_some(&mut self, value: Self);
}

impl<T> OptionExt for Option<T> {
    #[inline]
    fn replace_if_some(&mut self, value: Self) {
        if let Some(value) = value {
            self.replace(value);
        }
    }
}

pub trait BitSliceExt {
    /// Get a single bit from the slice.
    /// A convenience wrapper around [`BitSlice::get`].
    fn get_bit(&self, index: usize) -> Option<bool>;
}

pub trait BitVecExt {
    fn set_all(&mut self, bit: bool);
}

impl<T: BitStore, O: BitOrder> BitSliceExt for BitSlice<T, O> {
    #[inline]
    fn get_bit(&self, index: usize) -> Option<bool> {
        self.get(index).as_deref().copied()
    }
}

impl<T: BitStore, O: BitOrder> BitVecExt for BitVec<T, O> {
    fn set_all(&mut self, bit: bool) {
        self.as_raw_mut_slice().fill_with(|| {
            BitStore::new(if bit {
                !<<T as bitvec::store::BitStore>::Mem>::ZERO
            } else {
                <<T as bitvec::store::BitStore>::Mem>::ZERO
            })
        });
    }
}
