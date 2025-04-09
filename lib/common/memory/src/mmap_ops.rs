use std::fs::{File, OpenOptions};
use std::mem::{align_of, size_of};
use std::path::Path;
use std::{io, mem, ptr};

use memmap2::{Mmap, MmapMut};

use crate::madvise::{self, AdviceSetting, Madviseable};

pub const TEMP_FILE_EXTENSION: &str = "tmp";

pub fn create_and_ensure_length(path: &Path, length: usize) -> io::Result<File> {
    if path.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // Don't truncate because we explicitly set the length later
            .truncate(false)
            .open(path)?;
        file.set_len(length as u64)?;

        Ok(file)
    } else {
        let temp_path = path.with_extension(TEMP_FILE_EXTENSION);
        {
            // create temporary file with the required length
            // Temp file is used to avoid situations, where crash happens between file creation and setting the length
            let temp_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Don't truncate because we explicitly set the length later
                .truncate(false)
                .open(&temp_path)?;
            temp_file.set_len(length as u64)?;
        }

        std::fs::rename(&temp_path, path)?;

        OpenOptions::new().read(true).write(true).open(path)
    }
}

pub fn open_read_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> io::Result<Mmap> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)?;

    let mmap = unsafe { Mmap::map(&file)? };

    // Populate before advising
    // Because we want to read data with normal advice
    if populate {
        mmap.populate();
    }

    madvise::madvise(&mmap, advice.resolve())?;

    Ok(mmap)
}

pub fn open_write_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> io::Result<MmapMut> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };

    // Populate before advising
    // Because we want to read data with normal advice
    if populate {
        mmap.populate();
    }

    madvise::madvise(&mmap, advice.resolve())?;

    Ok(mmap)
}
pub fn transmute_from_u8<T>(v: &[u8]) -> &T {
    debug_assert_eq!(v.len(), size_of::<T>());

    debug_assert_eq!(
        v.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        v.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        v.as_ptr().align_offset(align_of::<T>()),
    );

    unsafe { &*v.as_ptr().cast::<T>() }
}

pub fn transmute_to_u8<T>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(ptr::from_ref::<T>(v).cast::<u8>(), mem::size_of_val(v)) }
}

pub fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);

    debug_assert_eq!(
        data.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into slice of {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        data.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        data.as_ptr().align_offset(align_of::<T>()),
    );

    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr().cast::<T>();
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

pub fn transmute_from_u8_to_mut_slice<T>(data: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);

    debug_assert_eq!(
        data.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into mutable slice of {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        data.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        data.as_ptr().align_offset(align_of::<T>()),
    );

    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr().cast::<T>();
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), mem::size_of_val(v)) }
}
