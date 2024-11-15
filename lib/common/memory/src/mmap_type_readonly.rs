//! Typed read-ponly memory maps
//!
//! This module adds type to directly map types and a slice of types onto a memory mapped file.
//! The typed memory maps can be directly used as if it were that type.
//!
//! Types:
//! - [`MmapTypeReadOnly`]
//! - [`MmapSliceReadOnly`]
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

use std::ops::Deref;
use std::sync::Arc;
use std::{fmt, mem, slice};

use memmap2::Mmap;

use crate::mmap_type::Error;

/// Result for mmap errors.
type Result<T> = std::result::Result<T, Error>;

/// Type `T` on a memory mapped file
///
/// Functions as if it is `T` because this implements [`Deref`]
///
/// # Safety
///
/// This directly maps (transmutes) the type onto the memory mapped data. This is dangerous and
/// very error prone and must be used with utmost care. Types holding references are not supported
/// for example. Malformed data in the mmap will break type `T` and will cause undefined behavior.
pub struct MmapTypeReadOnly<T>
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
    r#type: &'static T,
    /// Type storage: memory mapped file as backing store for type
    ///
    /// Has an exact size to fit the type.
    ///
    /// This should never be accessed directly, because it shares a mutable reference with
    /// `r#type`. That must be used instead. The sole purpose of this is to keep ownership of the
    /// mmap, and to allow properly cleaning up when this struct is dropped.
    mmap: Arc<Mmap>,
}

impl<T: ?Sized> fmt::Debug for MmapTypeReadOnly<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapTypeReadOnly")
            .field("mmap", &self.mmap)
            .finish_non_exhaustive()
    }
}

impl<T> MmapTypeReadOnly<T>
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
    pub unsafe fn from(mmap_with_type: Mmap) -> Self {
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
    pub unsafe fn try_from(mmap_with_type: Mmap) -> Result<Self> {
        let r#type = mmap_prefix_to_type_unbounded(&mmap_with_type)?;
        let mmap = Arc::new(mmap_with_type);
        Ok(Self { r#type, mmap })
    }
}

impl<T> MmapTypeReadOnly<[T]>
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
    /// extra parts. See [`MmapSliceReadOnly`], [`MmapTypeReadOnly::slice_from`] and
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
    pub unsafe fn try_slice_from(mmap_with_slice: Mmap) -> Result<Self> {
        let r#type = mmap_to_slice_unbounded(&mmap_with_slice, 0)?;
        let mmap = Arc::new(mmap_with_slice);
        Ok(Self { r#type, mmap })
    }
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
unsafe fn mmap_prefix_to_type_unbounded<'unbnd, T>(mmap: &Mmap) -> Result<&'unbnd T>
where
    T: Sized,
{
    let size_t = mem::size_of::<T>();

    // Assert size
    if mmap.len() < size_t {
        return Err(Error::SizeLess(size_t, mmap.len()));
    }

    // Obtain unbounded bytes slice into mmap
    let bytes: &'unbnd [u8] = {
        let slice = mmap.deref();
        slice::from_raw_parts(slice.as_ptr(), size_t)
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

    let ptr = bytes.as_ptr().cast::<T>();
    Ok(unsafe { &*ptr })
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
unsafe fn mmap_to_slice_unbounded<'unbnd, T>(mmap: &Mmap, header_size: usize) -> Result<&'unbnd [T]>
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
    let bytes: &'unbnd [u8] = {
        let slice = mmap.deref();
        &slice::from_raw_parts(slice.as_ptr(), slice.len())[header_size..]
    };

    // Assert alignment and bytes size
    assert_alignment::<_, T>(bytes);
    debug_assert_eq!(bytes.len() + header_size, mmap.len());

    // Transmute slice types
    Ok(slice::from_raw_parts(
        bytes.as_ptr().cast::<T>(),
        bytes.len().checked_div(size_t).unwrap_or(0),
    ))
}

impl<T> Deref for MmapTypeReadOnly<T>
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

/// Slice of type `T` on a memory mapped file
///
/// Functions as if it is `&[T]` because this implements [`Deref`]
///
/// A helper because [`MmapTypeReadOnly`] doesn't support slices directly.
pub struct MmapSliceReadOnly<T>
where
    T: Sized + 'static,
{
    mmap: MmapTypeReadOnly<[T]>,
}

impl<T> fmt::Debug for MmapSliceReadOnly<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapSliceReadOnly")
            .field("mmap", &self.mmap)
            .finish_non_exhaustive()
    }
}

impl<T> MmapSliceReadOnly<T> {
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
    pub unsafe fn from(mmap_with_slice: Mmap) -> Self {
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
    pub unsafe fn try_from(mmap_with_slice: Mmap) -> Result<Self> {
        MmapTypeReadOnly::try_slice_from(mmap_with_slice).map(|mmap| Self { mmap })
    }
}

impl<T> Deref for MmapSliceReadOnly<T> {
    type Target = MmapTypeReadOnly<[T]>;

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
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
    fn test_open_from() {
        const SIZE: usize = 1024;
        let tempfile = create_temp_mmap_file(SIZE);
        let mmap = mmap_ops::open_read_mmap(tempfile.path(), AdviceSetting::Global, false).unwrap();
        let result = unsafe { MmapSliceReadOnly::<u64>::try_from(mmap).unwrap() };

        assert_eq!(result.len(), SIZE / size_of::<u64>());

        assert_eq!(result[10], 0);
    }
}
