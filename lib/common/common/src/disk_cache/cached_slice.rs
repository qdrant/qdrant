#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;

use super::CachedFile;

/// Abstraction to simulate a [T], but it is backed by cache.
pub struct CachedSlice<T> {
    cached_file: CachedFile,
    r#type: PhantomData<T>,
}

impl<T> CachedSlice<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Clone + bytemuck::Pod,
{
    pub fn new(cached_file: CachedFile) -> Self {
        Self {
            cached_file,
            r#type: PhantomData,
        }
    }

    pub fn get_range(&self, range: Range<usize>) -> Cow<'_, [T]> {
        self.cached_file.get_range(range)
    }

    #[cfg(test)]
    pub fn get(&self, idx: usize) -> Cow<'_, T> {
        let slice = self.get_range(idx..idx + 1);
        match slice {
            Cow::Borrowed(slice) => Cow::Borrowed(&slice[0]),
            Cow::Owned(mut vec) => Cow::Owned(vec.pop().unwrap()),
        }
    }

    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = Cow<'_, T>> + '_ {
        let total_len = self.len();
        (0..total_len).map(|idx| self.get(idx))
    }

    #[expect(clippy::len_without_is_empty)] // Doesn't make sense to cache 0-length files
    pub fn len(&self) -> usize {
        self.cached_file.len() / mem::size_of::<T>()
    }
}
