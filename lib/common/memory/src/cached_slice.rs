use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::ops::Range;

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::cached_data::CachedData;

pub struct CachedSlice<T> {
    data: CachedData,
    r#type: PhantomData<T>,
}

impl<T> CachedSlice<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: FromBytes + Immutable + KnownLayout,
{
    pub fn get_range(&self, range: Range<usize>) -> Cow<'_, [T]>
    where
        <[T] as ToOwned>::Owned: From<Vec<T>>,
    {
        let t_size = mem::size_of::<T>();
        assert!(t_size != 0, "cannot transmute zero-sized type");

        let byte_range = range.start * t_size..range.end * t_size;
        let data = self.data.get_range(byte_range);

        match data {
            Cow::Borrowed(bytes) => {
                let slice = <[T]>::ref_from_bytes(bytes)
                    .expect("byte slice is not a valid &[T] (alignment or size mismatch)");
                Cow::Borrowed(slice)
            }
            Cow::Owned(mut vec_u8) => {
                // Make sure `capacity` is alsoan exact multiple of `size_of::<T>()`
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
                let vec_t = unsafe { Vec::from_raw_parts(ptr, len, cap) };
                Cow::Owned(vec_t)
            }
        }
    }
}
