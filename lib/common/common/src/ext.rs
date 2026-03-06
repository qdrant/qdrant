use bitvec::order::BitOrder;
use bitvec::slice::BitSlice;
use bitvec::store::BitStore;

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

impl<T: BitStore, O: BitOrder> BitSliceExt for BitSlice<T, O> {
    #[inline]
    fn get_bit(&self, index: usize) -> Option<bool> {
        self.get(index).as_deref().copied()
    }
}
