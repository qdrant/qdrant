#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;

use bytemuck::PodCastError;

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

    pub fn get_range(&self, range: Range<usize>) -> Result<Cow<'_, [T]>, PodCastError> {
        let t_size = mem::size_of::<T>();
        assert!(t_size != 0, "cannot transmute zero-sized type");

        let byte_range = range.start * t_size..range.end * t_size;
        let data = self.cached_file.get_range(byte_range);

        let cow = match data {
            Cow::Borrowed(bytes) => {
                let slice = bytemuck::try_cast_slice(bytes)?;
                Cow::Borrowed(slice)
            }
            Cow::Owned(vec_u8) => {
                let vec = bytemuck::try_cast_vec(vec_u8).map_err(|(err, _)| err)?;
                Cow::Owned(vec)
            }
        };

        Ok(cow)
    }

    #[cfg(test)]
    pub fn get(&self, idx: usize) -> Result<Cow<'_, T>, PodCastError> {
        let slice = self.get_range(idx..idx + 1)?;
        let cow = match slice {
            Cow::Borrowed(slice) => Cow::Borrowed(&slice[0]),
            Cow::Owned(mut vec) => Cow::Owned(vec.pop().unwrap()),
        };
        Ok(cow)
    }

    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = Cow<'_, T>> + '_ {
        let total_len = self.len();
        (0..total_len).map(|idx| self.get(idx).unwrap())
    }

    #[expect(clippy::len_without_is_empty)] // Doesn't make sense to cache 0-length files
    pub fn len(&self) -> usize {
        self.cached_file.len() / mem::size_of::<T>()
    }
}
