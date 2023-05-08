use std::fs::OpenOptions;
use std::mem;
use std::mem::size_of;
use std::path::Path;

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::entry::entry_point::OperationResult;
use crate::madvise;

pub fn create_and_ensure_length(path: &Path, length: usize) -> OperationResult<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    file.set_len(length as u64)?;
    Ok(())
}

pub fn open_read_mmap(path: &Path) -> OperationResult<Mmap> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(true)
        .create(true)
        .open(path)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;
    Ok(mmap)
}

pub fn open_write_mmap(path: &Path) -> OperationResult<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;
    Ok(mmap)
}

pub fn transmute_from_u8_to_mut<'a, T>(data: &mut [u8]) -> &'a mut T {
    debug_assert_eq!(data.len(), size_of::<T>());
    let ptr = data.as_ptr() as *mut T;
    unsafe { &mut *ptr }
}

pub fn transmute_to_u8<T>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v as *const T as *const u8, mem::size_of_val(v)) }
}

pub fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

pub fn transmute_from_u8_to_mut_slice<T>(data: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

/// Transmute a `&mut [u8]` and `&mut [T]` and share mutable reference
///
/// # Safety
///
/// This is unsafe because this creates a second mutable reference to the given `data`.
///
/// The caller must ensure that:
/// - the two mutable references are never mutably accessed at the same time
/// - the returned reference never outlives `data`
pub unsafe fn transmute_from_u8_to_mut_slice_shared<'a, T>(data: &mut [u8]) -> &'a mut [T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    std::slice::from_raw_parts_mut(ptr, len)
}

pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, mem::size_of_val(v)) }
}

pub fn write_to_mmap<T>(mmap: &mut MmapMut, offset: usize, data: T) {
    debug_assert!(offset + size_of::<T>() <= mmap.len());
    let ptr = mmap.as_mut_ptr();
    unsafe {
        let byte_data = transmute_to_u8(&data);
        std::ptr::copy_nonoverlapping(byte_data.as_ptr(), ptr.add(offset), byte_data.len());
    }
}
