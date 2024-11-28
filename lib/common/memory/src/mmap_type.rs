//! Typed memory maps
//!
//! This module adds type to directly map types and a slice of types onto a memory mapped file.
//! The typed memory maps can be directly used as if it were that type.
//!
//! Types:
//! - [`MmapType`]
//! - [`MmapSlice`]
//! - [`MmapBitSlice`]
//!
//! Various additional functions are added for use within Qdrant, such as `flusher` to obtain a
//! flusher handle to explicitly flush the underlying memory map at a later time.
//!
//! # Safety
//!
//! Code in this module is `unsafe` and very error prone. It is therefore compacted in this single
//! module to make it easier to review, to make it easier to check for soundness, and to make it
//! easier to reason about. The interface provided by types in this module is as-safe-as-possible
//! and uses `unsafe` where appropriate.
//!
//! Please prevent touching code in this file. If modifications must be done, please do so with the
//! utmost care. Security is critical here as this is an easy place to introduce undefined
//! behavior. Problems caused by this are very hard to debug.

use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::{fmt, mem, slice};

use bitvec::slice::BitSlice;
use memmap2::MmapMut;

use crate::madvise::{Advice, AdviceSetting};
use crate::mmap_ops;

/// Result for mmap errors.
type Result<T> = std::result::Result<T, Error>;

pub type MmapFlusher = Box<dyn FnOnce() -> Result<()> + Send>;

/// Type `T` on a memory mapped file
///
/// Functions as if it is `T` because this implements [`Deref`] and [`DerefMut`].
///
/// # Safety
///
/// This directly maps (transmutes) the type onto the memory mapped data. This is dangerous and
/// very error prone and must be used with utmost care. Types holding references are not supported
/// for example. Malformed data in the mmap will break type `T` and will cause undefined behavior.
pub struct MmapType<T>
where
    T: ?Sized + 'static,
{
    /// Type accessor: mutable reference to access the type
    ///
    /// This has the same lifetime as the backing `mmap`, and thus this struct. A borrow must
    /// never be leased out for longer.
    ///
    /// Since we own this reference inside this struct, we can guarantee we never lease it out for
    /// longer.
    ///
    /// # Safety
    ///
    /// This is an alias to the data inside `mmap`. We should prevent using both together at all
    /// costs because the Rust compiler assumes `noalias` for optimization.
    ///
    /// See: <https://doc.rust-lang.org/nomicon/aliasing.html>
    r#type: &'static mut T,
    /// Type storage: memory mapped file as backing store for type
    ///
    /// Has an exact size to fit the type.
    ///
    /// This should never be accessed directly, because it shares a mutable reference with
    /// `r#type`. That must be used instead. The sole purpose of this is to keep ownership of the
    /// mmap, and to allow properly cleaning up when this struct is dropped.
    mmap: Arc<MmapMut>,
}

impl<T: ?Sized> fmt::Debug for MmapType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapType")
            .field("mmap", &self.mmap)
            .finish_non_exhaustive()
    }
}

impl<T> MmapType<T>
where
    T: Sized + 'static,
{
    /// Transform a mmap into a typed mmap of type `T`.
    ///
    /// # Safety
    ///
    /// Unsafe because malformed data in the mmap may break type `T` resulting in undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap doesn't match size `T`
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_prefix_to_type_unbounded`]
    pub unsafe fn from(mmap_with_type: MmapMut) -> Self {
        Self::try_from(mmap_with_type).unwrap()
    }

    /// Transform a mmap into a typed mmap of type `T`.
    ///
    /// Returns an error when the mmap has an incorrect size.
    ///
    /// # Safety
    ///
    /// Unsafe because malformed data in the mmap may break type `T` resulting in undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_prefix_to_type_unbounded`]
    pub unsafe fn try_from(mut mmap_with_type: MmapMut) -> Result<Self> {
        let r#type = mmap_prefix_to_type_unbounded(&mut mmap_with_type)?;
        let mmap = Arc::new(mmap_with_type);
        Ok(Self { r#type, mmap })
    }
}

impl<T> MmapType<[T]>
where
    T: 'static,
{
    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    ///
    /// Returns an error when the mmap has an incorrect size.
    ///
    /// # Warning
    ///
    /// This does not support slices, because those cannot be transmuted directly because it has
    /// extra parts. See [`MmapSlice`], [`MmapType::slice_from`] and
    /// [`std::slice::from_raw_parts`].
    ///
    /// # Safety
    ///
    /// Unsafe because malformed data in the mmap may break type `T` resulting in undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_slice_unbounded`]
    pub unsafe fn try_slice_from(mut mmap_with_slice: MmapMut) -> Result<Self> {
        let r#type = mmap_to_slice_unbounded(&mut mmap_with_slice, 0)?;
        let mmap = Arc::new(mmap_with_slice);
        Ok(Self { r#type, mmap })
    }
}

impl<T> MmapType<T>
where
    T: ?Sized + 'static,
{
    /// Get flusher to explicitly flush mmap at a later time
    pub fn flusher(&self) -> MmapFlusher {
        // TODO: if we explicitly flush when dropping this type, we can switch to a weak reference
        // here to only flush if it hasn't been done already
        Box::new({
            let mmap = self.mmap.clone();
            move || {
                mmap.flush()?;
                Ok(())
            }
        })
    }

    /// Call [`memmap2::MmapMut::unchecked_advise`] on the underlying mmap.
    ///
    /// # Safety
    ///
    /// See [`memmap2::UncheckedAdvice`] doc.
    #[cfg(unix)]
    pub unsafe fn unchecked_advise(&self, advice: memmap2::UncheckedAdvice) -> std::io::Result<()> {
        self.mmap.unchecked_advise(advice)
    }
}

impl<T> Deref for MmapType<T>
where
    T: ?Sized + 'static,
{
    type Target = T;

    // Has explicit 'bounded lifetime to clarify the inner reference never outlives this struct,
    // even though the reference has a static lifetime internally.
    #[allow(clippy::needless_lifetimes)]
    fn deref<'bounded>(&'bounded self) -> &'bounded Self::Target {
        self.r#type
    }
}

impl<T> DerefMut for MmapType<T>
where
    T: ?Sized + 'static,
{
    // Has explicit 'bounded lifetime to clarify the inner reference never outlives this struct,
    // even though the reference has a static lifetime internally.
    #[allow(clippy::needless_lifetimes)]
    fn deref_mut<'bounded>(&'bounded mut self) -> &'bounded mut Self::Target {
        self.r#type
    }
}

/// Slice of type `T` on a memory mapped file
///
/// Functions as if it is `&[T]` because this implements [`Deref`] and [`DerefMut`].
///
/// A helper because [`MmapType`] doesn't support slices directly.
pub struct MmapSlice<T>
where
    T: Sized + 'static,
{
    mmap: MmapType<[T]>,
}

impl<T> fmt::Debug for MmapSlice<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapSlice")
            .field("mmap", &self.mmap)
            .finish_non_exhaustive()
    }
}

impl<T> MmapSlice<T> {
    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    ///
    /// This method is specifically intended for slices.
    ///
    /// # Safety
    ///
    /// Unsafe because malformed data in the mmap may break type `T` resulting in undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap isn't a multiple of size `T`
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_slice_unbounded`]
    pub unsafe fn from(mmap_with_slice: MmapMut) -> Self {
        Self::try_from(mmap_with_slice).unwrap()
    }

    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    ///
    /// This method is specifically intended for slices.
    ///
    /// Returns an error when the mmap has an incorrect size.
    ///
    /// # Safety
    ///
    /// Unsafe because malformed data in the mmap may break type `T` resulting in undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_slice_unbounded`]
    pub unsafe fn try_from(mmap_with_slice: MmapMut) -> Result<Self> {
        MmapType::try_slice_from(mmap_with_slice).map(|mmap| Self { mmap })
    }

    /// Get flusher to explicitly flush mmap at a later time
    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn create(path: &Path, mut iter: impl ExactSizeIterator<Item = T>) -> Result<()> {
        let file_len = iter.len() * mem::size_of::<T>();

        let _file = mmap_ops::create_and_ensure_length(path, file_len)?;

        let mmap = mmap_ops::open_write_mmap(
            path,
            AdviceSetting::Advice(Advice::Normal), // We only write sequentially
            false,
        )?;

        let mut mmap_slice = unsafe { Self::try_from(mmap)? };

        mmap_slice.fill_with(|| iter.next().expect("iterator size mismatch"));

        mmap_slice.flusher()()?;

        Ok(())
    }
}

impl<T> Deref for MmapSlice<T> {
    type Target = MmapType<[T]>;

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl<T> DerefMut for MmapSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}

/// [`BitSlice`] on a memory mapped file
///
/// Functions as if it is a [`BitSlice`] because this implements [`Deref`] and [`DerefMut`].
#[derive(Debug)]
pub struct MmapBitSlice {
    mmap: MmapType<BitSlice>,
}

impl MmapBitSlice {
    /// Minimum file size for the mmap file, in bytes.
    const MIN_FILE_SIZE: usize = mem::size_of::<usize>();

    /// Transform a mmap into a [`BitSlice`].
    ///
    /// A (non-zero) header size in bytes may be provided to omit from the BitSlice data.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap isn't a multiple of the inner [`BitSlice`] type
    /// - panics when the mmap data is not correctly aligned to the inner [`BitSlice`] type
    /// - panics when the header size isn't a multiple of the inner [`BitSlice`] type
    /// - See: [`mmap_to_slice_unbounded`]
    pub fn from(mmap: MmapMut, header_size: usize) -> Self {
        Self::try_from(mmap, header_size).unwrap()
    }

    /// Transform a mmap into a [`BitSlice`].
    ///
    /// Returns an error when the mmap has an incorrect size.
    ///
    /// A (non-zero) header size in bytes may be provided to omit from the BitSlice data.
    ///
    /// # Panics
    ///
    /// - panics when the mmap data is not correctly aligned to the inner [`BitSlice`] type
    /// - panics when the header size isn't a multiple of the inner [`BitSlice`] type
    /// - See: [`mmap_to_slice_unbounded`]
    pub fn try_from(mut mmap: MmapMut, header_size: usize) -> Result<Self> {
        let data = unsafe { mmap_to_slice_unbounded(&mut mmap, header_size)? };
        let bitslice = BitSlice::from_slice_mut(data);
        let mmap = Arc::new(mmap);

        Ok(Self {
            mmap: MmapType {
                r#type: bitslice,
                mmap,
            },
        })
    }

    /// Get flusher to explicitly flush mmap at a later time
    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn create(path: &Path, bitslice: &BitSlice) -> Result<()> {
        let bits_count = bitslice.len();
        let bytes_count = bits_count
            .div_ceil(u8::BITS as usize)
            .next_multiple_of(Self::MIN_FILE_SIZE);

        let _file = mmap_ops::create_and_ensure_length(path, bytes_count)?;

        let mmap = mmap_ops::open_write_mmap(
            path,
            AdviceSetting::Advice(Advice::Normal), // We only write sequentially
            false,
        )?;

        let mut mmap_bitslice = MmapBitSlice::try_from(mmap, 0)?;

        mmap_bitslice.fill_with(|idx| {
            bitslice
                .get(idx)
                .map(|bitref| bitref.as_ref().to_owned())
                // mmap bitslice can be bigger than bitslice because it must align with size of `usize`
                .unwrap_or(false)
        });

        mmap_bitslice.flusher()()?;

        Ok(())
    }
}

impl Deref for MmapBitSlice {
    type Target = BitSlice;

    fn deref(&self) -> &BitSlice {
        &self.mmap
    }
}

impl DerefMut for MmapBitSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}

/// Typed mmap errors.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Mmap length must be {0} to match the size of type, but it is {1}")]
    SizeExact(usize, usize),
    #[error("Mmap length must be at least {0} to match the size of type, but it is {1}")]
    SizeLess(usize, usize),
    #[error("Mmap length must be multiple of {0} to match the size of type, but it is {1}")]
    SizeMultiple(usize, usize),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("File not found: {0}")]
    MissingFile(String),
}

/// Get a second mutable reference for type `T` from the given mmap
///
/// # Warning
///
/// The returned reference is unbounded. The user must ensure it never outlives the `mmap` type.
///
/// # Safety
///
/// - unsafe because we create a second (unbounded) mutable reference
/// - malformed data in the mmap may break the transmuted type `T` resulting in undefined behavior
///
/// # Panics
///
/// - panics when the mmap data is not correctly aligned for type `T`
unsafe fn mmap_prefix_to_type_unbounded<'unbnd, T>(mmap: &mut MmapMut) -> Result<&'unbnd mut T>
where
    T: Sized,
{
    let size_t = mem::size_of::<T>();

    // Assert size
    if mmap.len() < size_t {
        return Err(Error::SizeLess(size_t, mmap.len()));
    }

    // Obtain unbounded bytes slice into mmap
    let bytes: &'unbnd mut [u8] = {
        let slice = mmap.deref_mut();
        slice::from_raw_parts_mut(slice.as_mut_ptr(), size_t)
    };

    // Assert alignment and size
    assert_alignment::<_, T>(bytes);

    #[cfg(debug_assertions)]
    if mmap.len() != size_t {
        log::warn!(
            "Mmap length {} is not equal to size of type {}",
            mmap.len(),
            size_t,
        );
    }

    #[cfg(debug_assertions)]
    if bytes.len() != mem::size_of::<T>() {
        return Err(Error::SizeExact(mem::size_of::<T>(), bytes.len()));
    }

    let ptr = bytes.as_mut_ptr().cast::<T>();
    Ok(unsafe { &mut *ptr })
}

/// Get a second mutable reference for a slice of type `T` from the given mmap
///
/// A (non-zero) header size in bytes may be provided to omit from the BitSlice data.
///
/// # Warning
///
/// The returned reference is unbounded. The user must ensure it never outlives the `mmap` type.
///
/// # Safety
///
/// - unsafe because we create a second (unbounded) mutable reference
/// - malformed data in the mmap may break the transmuted slice for type `T` resulting in undefined
///   behavior
///
/// # Panics
///
/// - panics when the mmap data is not correctly aligned for type `T`
/// - panics when the header size isn't a multiple of size `T`
unsafe fn mmap_to_slice_unbounded<'unbnd, T>(
    mmap: &mut MmapMut,
    header_size: usize,
) -> Result<&'unbnd mut [T]>
where
    T: Sized,
{
    let size_t = mem::size_of::<T>();

    // Assert size
    if size_t == 0 {
        // For zero-sized T, data part must be zero-sized as well, we cannot have infinite slice
        debug_assert_eq!(
            mmap.len().saturating_sub(header_size),
            0,
            "mmap data must be zero-sized, because size T is zero",
        );
    } else {
        // Must be multiple of size T
        debug_assert_eq!(header_size % size_t, 0, "header not multiple of size T");
        if mmap.len() % size_t != 0 {
            return Err(Error::SizeMultiple(size_t, mmap.len()));
        }
    }

    // Obtain unbounded bytes slice into mmap
    let bytes: &'unbnd mut [u8] = {
        let slice = mmap.deref_mut();
        &mut slice::from_raw_parts_mut(slice.as_mut_ptr(), slice.len())[header_size..]
    };

    // Assert alignment and bytes size
    assert_alignment::<_, T>(bytes);
    debug_assert_eq!(bytes.len() + header_size, mmap.len());

    // Transmute slice types
    Ok(slice::from_raw_parts_mut(
        bytes.as_mut_ptr().cast::<T>(),
        bytes.len().checked_div(size_t).unwrap_or(0),
    ))
}

/// Assert slice `&[S]` is correctly aligned for type `T`.
///
/// # Panics
///
/// Panics when alignment is wrong.
fn assert_alignment<S, T>(bytes: &[S]) {
    assert_eq!(
        bytes.as_ptr().align_offset(mem::align_of::<T>()),
        0,
        "type must be aligned",
    );
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::iter;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::{Builder, NamedTempFile};

    use super::*;
    use crate::madvise::AdviceSetting;
    use crate::mmap_ops;

    fn create_temp_mmap_file(len: usize) -> NamedTempFile {
        let tempfile = Builder::new()
            .prefix("test.")
            .suffix(".mmap")
            .tempfile()
            .unwrap();
        tempfile.as_file().set_len(len as u64).unwrap();
        tempfile
    }

    #[test]
    fn test_open_zero_type() {
        check_open_zero_type::<()>(());
        check_open_zero_type::<u8>(0);
        check_open_zero_type::<usize>(0);
        check_open_zero_type::<f32>(0.0);
    }

    fn check_open_zero_type<T: Sized + PartialEq + Debug + 'static>(zero: T) {
        let bytes = mem::size_of::<T>();
        let tempfile = create_temp_mmap_file(bytes);
        let mmap =
            mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();

        let mmap_type: MmapType<T> = unsafe { MmapType::from(mmap) };
        assert_eq!(mmap_type.deref(), &zero);
    }

    #[test]
    fn test_open_zero_slice() {
        check_open_zero_slice::<()>(0, ());
        check_open_zero_slice::<u8>(0, 0);
        check_open_zero_slice::<u8>(1, 0);
        check_open_zero_slice::<u8>(131, 0);
        check_open_zero_slice::<usize>(0, 0);
        check_open_zero_slice::<usize>(1, 0);
        check_open_zero_slice::<usize>(131, 0);
        check_open_zero_slice::<f32>(0, 0.0);
        check_open_zero_slice::<f32>(1, 0.0);
        check_open_zero_slice::<f32>(131, 0.0);
    }

    #[test]
    #[should_panic]
    fn test_open_zero_slice_infinite_length() {
        // A slice with zero-sized type T can never be more than 0 bytes
        check_open_zero_slice::<()>(1, ());
    }

    fn check_open_zero_slice<T: Sized + PartialEq + Debug + 'static>(len: usize, zero: T) {
        let bytes = mem::size_of::<T>() * len;
        let tempfile = create_temp_mmap_file(bytes);
        let mmap =
            mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();

        let mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
        assert_eq!(mmap_slice.len(), len);
        assert!(mmap_slice.iter().all(|i| i == &zero));
    }

    #[test]
    fn test_reopen_random() {
        let mut rng = StdRng::seed_from_u64(42);
        check_reopen_random::<(), _>(0, || rng.gen());
        check_reopen_random::<u8, _>(0, || rng.gen());
        check_reopen_random::<u8, _>(1, || rng.gen());
        check_reopen_random::<u8, _>(131, || rng.gen());
        check_reopen_random::<usize, _>(0, || rng.gen());
        check_reopen_random::<usize, _>(1, || rng.gen());
        check_reopen_random::<usize, _>(131, || rng.gen());
        check_reopen_random::<f32, _>(0, || rng.gen());
        check_reopen_random::<f32, _>(1, || rng.gen());
        check_reopen_random::<f32, _>(131, || rng.gen());
    }

    fn check_reopen_random<T, R>(len: usize, rng: R)
    where
        T: Sized + Copy + PartialEq + Debug + 'static,
        R: FnMut() -> T,
    {
        let bytes = mem::size_of::<T>() * len;
        let tempfile = create_temp_mmap_file(bytes);

        let template: Vec<T> = iter::repeat_with(rng).take(len).collect();

        // Write random values from template into mmap
        {
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let mut mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
            assert_eq!(mmap_slice.len(), len);
            mmap_slice.copy_from_slice(&template);
        }

        // Reopen and assert values from template
        {
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
            assert_eq!(mmap_slice.as_ref(), template);
        }
    }

    #[test]
    fn test_bitslice() {
        check_bitslice_with_header(0, 0);
        check_bitslice_with_header(0, 128);
        check_bitslice_with_header(512, 0);
        check_bitslice_with_header(512, 256);
        check_bitslice_with_header(11721 * 8, 256);
    }

    fn check_bitslice_with_header(bits: usize, header_size: usize) {
        let bytes = (mem::size_of::<usize>() * bits / 8) + header_size;
        let tempfile = create_temp_mmap_file(bytes);

        // Fill bitslice
        {
            let mut rng = StdRng::seed_from_u64(42);
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let mut mmap_bitslice = MmapBitSlice::from(mmap, header_size);
            (0..bits).for_each(|i| mmap_bitslice.set(i, rng.gen()));
        }

        // Reopen and assert contents
        {
            let mut rng = StdRng::seed_from_u64(42);
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let mmap_bitslice = MmapBitSlice::from(mmap, header_size);
            (0..bits).for_each(|i| assert_eq!(mmap_bitslice[i], rng.gen::<bool>()));
        }
    }

    #[test]
    fn test_zero_sized_type() {
        {
            let tempfile = create_temp_mmap_file(0);
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let result = unsafe { MmapType::<()>::try_from(mmap).unwrap() };
            assert_eq!(result.deref(), &());
        }

        {
            let tempfile = create_temp_mmap_file(0);
            let mmap =
                mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
            let result = unsafe { MmapSlice::<()>::try_from(mmap).unwrap() };
            assert_eq!(result.as_ref(), &[]);
            assert_alignment::<_, ()>(result.as_ref());
        }
    }

    #[test]
    fn test_double_read_mmap() {
        // Create and open a tmp file
        // Mmap it with write access
        // then mmap it with read access
        // Check that the data is synchronized

        let tempfile = create_temp_mmap_file(1024);
        let mut mmap_write =
            mmap_ops::open_write_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
        let mmap_read = mmap_ops::open_read_mmap(
            tempfile.path(),
            AdviceSetting::Advice(Advice::Sequential),
            false,
        )
        .unwrap();

        mmap_write[333] = 42;

        assert_eq!(mmap_read[333], 42);
    }
}
