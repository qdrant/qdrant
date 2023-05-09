use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::{mem, slice};

use bitvec::slice::BitSlice;
use memmap2::MmapMut;

use crate::common::Flusher;

/// Type `T` on a memory mapped file
///
/// This implements [`Deref`] and [`DerefMut`], so this type may be used as if it is type `T`.
pub struct MmapType<T>
where
    T: ?Sized + 'static,
{
    /// Type accessor: mutable reference to access the type
    ///
    /// This has the same lifetime as the backing [`mmap`], and thus this struct. A borrow must
    /// never be leased out for longer.
    ///
    /// Since we own this reference inside this struct, we can guarantee we never lease it out for
    /// longer.
    r#type: &'static mut T,
    /// Type storage: memory mapped file as backing store for type
    ///
    /// Has an exact size to fit the type.
    ///
    /// This should never be accessed directly, because it shares a mutable reference with
    /// `r#type`. That must be used instead. The sole purpose of this is to keep ownership of the
    /// mmap, and to allow properly cleaning up when this struct is dropped.
    ///
    /// Uses a mutex because mutable access is needed for locking pages in memory.
    mmap: Arc<Mutex<MmapMut>>,
}

impl<T> MmapType<T>
where
    T: Sized + 'static,
{
    /// Transform a mmap into a typed mmap of type `T`.
    ///
    /// # Unsafe
    ///
    /// Unsafe because malformed data in the mmap may break type `T`.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap doesn't match size `T`
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_type_unbounded`]
    pub unsafe fn from(mut mmap: MmapMut) -> Self {
        let r#type = unsafe { mmap_to_type_unbounded(&mut mmap) };
        let mmap = Arc::new(Mutex::new(mmap));
        Self { r#type, mmap }
    }
}

impl<T> MmapType<[T]>
where
    T: 'static,
{
    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    ///
    /// # Unsafe
    ///
    /// Unsafe because malformed data in the mmap may break type `T`.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap isn't a multiple of size `T`
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_slice_unbounded`]
    pub unsafe fn slice_from(mut mmap: MmapMut) -> Self {
        let r#type = unsafe { mmap_to_slice_unbounded(&mut mmap, 0) };
        let mmap = Arc::new(Mutex::new(mmap));
        Self { r#type, mmap }
    }
}

impl<T> MmapType<T>
where
    T: ?Sized + 'static,
{
    /// Lock memory mapped pages in memory
    ///
    /// See [`MmapMut::lock`] for details.
    #[cfg(unix)]
    pub fn lock(&mut self) -> io::Result<()> {
        self.mmap.lock().unwrap().flush()
    }

    /// Get flusher to explicitly flush mmap at a later time
    pub fn flusher(&self) -> Flusher {
        // TODO: if we explicitly flush when dropping this type, we can switch to a weak reference
        // here to only flush if it hasn't been done already
        Box::new({
            let mmap = self.mmap.clone();
            move || {
                mmap.lock().unwrap().flush()?;
                Ok(())
            }
        })
    }
}

impl<T> Deref for MmapType<T>
where
    T: ?Sized + 'static,
{
    type Target = T;

    // Uses explicit lifetimes to clarify the inner reference never outlives this struct, even
    // though it has a static internal lifetime.
    #[allow(clippy::needless_lifetimes)]
    fn deref<'a>(&'a self) -> &'a Self::Target {
        self.r#type
    }
}

impl<T> DerefMut for MmapType<T>
where
    T: ?Sized + 'static,
{
    // Uses explicit lifetimes to clarify the inner reference never outlives this struct, even
    // though it has a static internal lifetime.
    #[allow(clippy::needless_lifetimes)]
    fn deref_mut<'a>(&'a mut self) -> &'a mut Self::Target {
        self.r#type
    }
}

/// Slice of type `T` on a memory mapped file
///
/// This implements [`Deref`] and [`DerefMut`], so this type may be used as if it is a slice of
/// type `T`.
pub struct MmapSlice<T>
where
    T: Sized + 'static,
{
    mmap: MmapType<[T]>,
}

impl<T> MmapSlice<T> {
    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    ///
    /// # Unsafe
    ///
    /// Unsafe because malformed data in the mmap may break type `T`.
    ///
    /// # Panics
    ///
    /// - panics when the size of the mmap isn't a multiple of size `T`
    /// - panics when the mmap data is not correctly aligned for type `T`
    /// - See: [`mmap_to_slice_unbounded`]
    pub unsafe fn from(mmap: MmapMut) -> Self {
        Self {
            mmap: MmapType::slice_from(mmap),
        }
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
/// This implements [`Deref`] and [`DerefMut`], so this type may be used as if it is a
/// [`BitSlice`].
pub struct MmapBitSlice {
    mmap: MmapType<BitSlice>,
}

impl MmapBitSlice {
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
    pub fn from(mut mmap: MmapMut, header_size: usize) -> Self {
        let data = unsafe { mmap_to_slice_unbounded(&mut mmap, header_size) };
        let bitslice = BitSlice::from_slice_mut(data);
        let mmap = Arc::new(Mutex::new(mmap));

        Self {
            mmap: MmapType {
                r#type: bitslice,
                mmap,
            },
        }
    }

    /// Lock memory mapped pages in memory
    ///
    /// See [`MmapMut::lock`] for details.
    #[cfg(unix)]
    pub fn lock(&mut self) -> io::Result<()> {
        self.mmap.lock()
    }

    /// Get flusher to explicitly flush mmap at a later time
    pub fn flusher(&self) -> Flusher {
        self.mmap.flusher()
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

/// Get a second mutable reference for type `T` from the given mmap
///
/// # Warning
///
/// The returned reference is unbounded. The user must ensure it never outlives the `mmap` type.
///
/// # Unsafe
///
/// - unsafe because we create a second (unbounded) mutable reference
/// - malformed data in the mmap may break the transmuted type `T`
///
/// # Panics
///
/// - panics when the size of the mmap doesn't match size `T`
/// - panics when the mmap data is not correctly aligned for type `T`
unsafe fn mmap_to_type_unbounded<'unbnd, T>(mmap: &mut MmapMut) -> &'unbnd mut T
where
    T: Sized,
{
    // Obtain unbounded bytes slice into mmap
    let bytes: &'unbnd mut [u8] = {
        let slice = mmap.deref_mut();
        slice::from_raw_parts_mut(slice.as_mut_ptr(), slice.len())
    };

    // Assert alignment and size
    assert_alignment::<_, T>(bytes);
    debug_assert_eq!(mmap.len(), bytes.len());
    assert_eq!(bytes.len(), mem::size_of::<T>());

    let ptr = bytes.as_ptr() as *mut T;
    unsafe { &mut *ptr }
}

/// Get a second mutable reference for a slice of type `T` from the given mmap
///
/// A (non-zero) header size in bytes may be provided to omit from the BitSlice data.
///
/// # Warning
///
/// The returned reference is unbounded. The user must ensure it never outlives the `mmap` type.
///
/// # Unsafe
///
/// - unsafe because we create a second (unbounded) mutable reference
/// - malformed data in the mmap may break the transmuted slice for type `T`
///
/// # Panics
///
/// - panics when the size of the mmap isn't a multiple of size `T`
/// - panics when the mmap data is not correctly aligned for type `T`
/// - panics when the header size isn't a multiple of size `T`
unsafe fn mmap_to_slice_unbounded<'unbnd, T>(
    mmap: &mut MmapMut,
    header_size: usize,
) -> &'unbnd mut [T]
where
    T: Sized,
{
    // Obtain unbounded bytes slice into mmap
    let bytes: &'unbnd mut [u8] = {
        let slice = mmap.deref_mut();
        &mut slice::from_raw_parts_mut(slice.as_mut_ptr(), slice.len())[header_size..]
    };

    // Assert alignment and size
    assert_alignment::<_, T>(bytes);
    debug_assert_eq!(mmap.len(), bytes.len() + header_size);
    debug_assert_eq!(
        header_size % mem::size_of::<T>(),
        0,
        "header not multiple of size T",
    );
    assert_eq!(
        bytes.len() % mem::size_of::<T>(),
        0,
        "bytes not multiple of size T",
    );

    // Transmute slice types
    slice::from_raw_parts_mut(bytes.as_ptr() as *mut T, bytes.len() / mem::size_of::<T>())
}

/// Assert slice `&[S]` is correctly aligned for type `T`.
///
/// # Panics
///
/// Panics when alignment is incorrect.
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
    use crate::common::mmap_ops;

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
    fn test_open_zero() {
        check_open_zero_type::<u8>(0);
        check_open_zero_type::<usize>(0);
        check_open_zero_type::<f32>(0.0);
        check_open_zero_slice::<u8>(0, 0);
        check_open_zero_slice::<u8>(1, 0);
        check_open_zero_slice::<u8>(123, 0);
        check_open_zero_slice::<usize>(0, 0);
        check_open_zero_slice::<usize>(1, 0);
        check_open_zero_slice::<usize>(123, 0);
        check_open_zero_slice::<f32>(0, 0.0);
        check_open_zero_slice::<f32>(1, 0.0);
        check_open_zero_slice::<f32>(123, 0.0);
    }

    fn check_open_zero_type<T: Sized + PartialEq + Debug + 'static>(zero: T) {
        let bytes = mem::size_of::<T>();
        let tempfile = create_temp_mmap_file(bytes);
        let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();

        let mmap_type: MmapType<T> = unsafe { MmapType::from(mmap) };
        assert_eq!(mmap_type.deref(), &zero);
    }

    fn check_open_zero_slice<T: Sized + PartialEq + Debug + 'static>(len: usize, zero: T) {
        let bytes = mem::size_of::<T>() * len;
        let tempfile = create_temp_mmap_file(bytes);
        let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();

        let mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
        assert_eq!(mmap_slice.len(), len);
        assert!(mmap_slice.iter().all(|i| i == &zero));
    }

    #[test]
    fn test_reopen_random() {
        let mut rng = StdRng::seed_from_u64(42);
        check_reopen_random::<u8, _>(0, || rng.gen());
        check_reopen_random::<u8, _>(1, || rng.gen());
        check_reopen_random::<u8, _>(123, || rng.gen());
        check_reopen_random::<usize, _>(0, || rng.gen());
        check_reopen_random::<usize, _>(1, || rng.gen());
        check_reopen_random::<usize, _>(123, || rng.gen());
        check_reopen_random::<f32, _>(0, || rng.gen());
        check_reopen_random::<f32, _>(1, || rng.gen());
        check_reopen_random::<f32, _>(123, || rng.gen());
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
            let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();
            let mut mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
            assert_eq!(mmap_slice.len(), len);
            mmap_slice.copy_from_slice(&template);
        }

        // Reopen and assert values from template
        {
            let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();
            let mmap_slice: MmapSlice<T> = unsafe { MmapSlice::from(mmap) };
            assert_eq!(mmap_slice.as_ref(), template);
        }
    }

    #[test]
    fn test_bitslice() {
        check_bitslice_with_header(512, 0);
        check_bitslice_with_header(512, 256);
    }

    fn check_bitslice_with_header(bits: usize, header_size: usize) {
        let bytes = (mem::size_of::<usize>() * bits / 8) + header_size;
        let tempfile = create_temp_mmap_file(bytes);

        // Fill bitslice
        {
            let mut rng = StdRng::seed_from_u64(42);
            let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();
            let mut mmap_bitslice = MmapBitSlice::from(mmap, header_size);
            (0..bits).for_each(|i| mmap_bitslice.set(i, rng.gen()));
        }

        // Reopen and assert contents
        {
            let mut rng = StdRng::seed_from_u64(42);
            let mmap = mmap_ops::open_write_mmap(tempfile.path()).unwrap();
            let mmap_bitslice = MmapBitSlice::from(mmap, header_size);
            (0..bits).for_each(|i| assert_eq!(mmap_bitslice[i], rng.gen::<bool>()));
        }
    }
}
