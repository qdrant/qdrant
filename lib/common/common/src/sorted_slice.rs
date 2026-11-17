use std::ops::Deref;

pub struct SortedSlice<'a, T: PartialOrd>(&'a [T]);

impl<'a, T: PartialOrd> SortedSlice<'a, T> {
    pub fn new(slice: &'a [T]) -> Option<Self> {
        if slice.is_sorted() {
            Some(Self(slice))
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the slice is sorted.
    pub unsafe fn new_unchecked(slice: &'a [T]) -> Self {
        debug_assert!(slice.is_sorted());
        Self(slice)
    }
}

impl<T: PartialOrd> Deref for SortedSlice<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: PartialOrd> IntoIterator for &'a SortedSlice<'a, T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
