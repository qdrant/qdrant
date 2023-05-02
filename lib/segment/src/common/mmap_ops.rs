use std::fs::OpenOptions;
use std::mem;
use std::mem::size_of;
use std::path::Path;

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::entry::entry_point::OperationResult;
use crate::madvise;

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

pub fn transmute_from_u8<T>(data: &[u8]) -> &T {
    debug_assert!(data.len() == size_of::<T>());
    let ptr = data.as_ptr() as *const T;
    unsafe { &*ptr }
}

pub fn transmute_to_u8<T>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v as *const T as *const u8, mem::size_of_val(v)) }
}

pub fn transmute_from_u8_to_array<T>(data: &[u8]) -> &[T] {
    debug_assert!(data.len() % size_of::<T>() == 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

pub fn transmute_from_u8_to_mut_array<T>(data: &mut [u8]) -> &mut [T] {
    debug_assert!(data.len() % size_of::<T>() == 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

pub fn transmute_to_u8_array<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, mem::size_of_val(v)) }
}
