#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::ops::Range;

use zerocopy::{FromBytes, Immutable, KnownLayout};

use super::CachedFile;

/// Abstraction to simulate a [T], but it is backed by cache.
pub struct CachedSlice<T> {
    cached_file: CachedFile,
    r#type: PhantomData<T>,
}

impl<T> CachedSlice<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Clone + FromBytes + Immutable + KnownLayout,
{
    pub fn new(cached_file: CachedFile) -> Self {
        Self {
            cached_file,
            r#type: PhantomData,
        }
    }

    pub fn get(&self, idx: usize) -> Cow<'_, T> {
        let slice = self.get_range(idx..idx + 1);
        match slice {
            Cow::Borrowed(slice) => Cow::Borrowed(&slice[0]),
            Cow::Owned(mut vec) => Cow::Owned(vec.pop().unwrap()),
        }
    }

    pub fn get_range(&self, range: Range<usize>) -> Cow<'_, [T]> {
        let t_size = mem::size_of::<T>();
        assert!(t_size != 0, "cannot transmute zero-sized type");

        let byte_range = range.start * t_size..range.end * t_size;
        let data = self.cached_file.get_range(byte_range);

        match data {
            Cow::Borrowed(bytes) => {
                let slice = <[T]>::ref_from_bytes(bytes)
                    .expect("byte slice is not a valid &[T] (alignment or size mismatch)");
                Cow::Borrowed(slice)
            }
            Cow::Owned(vec_u8) => Cow::Owned(transmute_zerocopy_vec(vec_u8)),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Cow<'_, T>> + '_ {
        let total_len = self.len();
        (0..total_len).map(|idx| self.get(idx))
    }

    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.cached_file.len() / mem::size_of::<T>()
    }
}

fn transmute_zerocopy_vec<T>(mut vec_u8: Vec<u8>) -> Vec<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Clone + FromBytes + Immutable + KnownLayout,
{
    let t_size = mem::size_of::<T>();

    // Make sure `capacity` is also an exact multiple of `size_of::<T>()`
    vec_u8.shrink_to_fit();

    debug_assert_eq!(
        vec_u8.len() % t_size,
        0,
        "byte length {} is not a multiple of element size {}",
        vec_u8.len(),
        t_size,
    );
    debug_assert_eq!(
        vec_u8.capacity() % t_size,
        0,
        "byte capacity {} is not a multiple of element size {}",
        vec_u8.capacity(),
        t_size,
    );

    let len = vec_u8.len() / t_size;
    let cap = vec_u8.capacity() / t_size;
    let mut vec_u8 = ManuallyDrop::new(vec_u8);
    let ptr = vec_u8.as_mut_ptr().cast::<T>();

    // SAFETY:
    // - `T: FromBytes` (zerocopy) guarantees that any initialized
    //   byte pattern constitutes a valid `T` value.
    // - `shrink_to_fit()` brings capacity down to len,
    //   so `cap * size_of::<T>()` matches the allocation size.
    // - The buffer originates from a `ManuallyDrop<Vec<u8>>`;
    //   `ManuallyDrop` prevents double-free.
    unsafe { Vec::from_raw_parts(ptr, len, cap) }
}
