use std::env;
use std::fs::{File, OpenOptions};
use std::mem;
use std::mem::size_of;
use std::ops;
use std::path::Path;
use std::time;

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::{entry::entry_point::{OperationResult, OperationError}, madvise::Madviseable};
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

    open_mmap(path, &file, |file, opts| unsafe { opts.map(file) })
}

pub fn open_write_mmap(path: &Path) -> OperationResult<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)?;

    open_mmap(path, &file, |file, opts| unsafe { opts.map_mut(file) })
}

fn open_mmap<F, T, E>(path: &Path, file: &File, open: F) -> OperationResult<T>
where
    F: FnOnce(&File, &MmapOptions) -> Result<T, E>,
    T: ops::Deref<Target = [u8]> + Madviseable,
    OperationError: From<E>,
{
    let mut opts = MmapOptions::new();

    if let Ok(_) = env::var("MAP_POPULATE") {
        log::info!("Loading mmap {path:?} with MAP_POPULATE");
        opts.populate();
    }

    let instant = time::Instant::now();
    let mmap = open(&file, &opts)?;
    log::info!("Loading mmap {path:?} took {:?}", instant.elapsed());

    if let Ok(_) = env::var("MADV_WILLNEED")
        .or_else(|_| env::var("MADV_WILLNEED_FIRST"))
        .or_else(|_| env::var("MADV_WILLNEED_SPARSE")) {

        log::info!("Advising mmap {path:?} with MADV_WILLNEED");

        madvise::madvise(&mmap, madvise::Advice::WillNeed)?;

        if let Ok(_) = env::var("MADV_WILLNEED_FIRST") {
            log::info!("Reading the first byte of mmap {path:?} to force page read-ahead");
            let instant = time::Instant::now();
            let _ = mmap[0];
            log::info!("Reading the first byte of mmap {path:?} took {:?}", instant.elapsed());
        } else if let Ok(_) = env::var("MADV_WILLNEED_SPARSE") {
            log::info!("Making 10 sparse reads of mmap {path:?} to force page read-ahead");

            let mut timings = Vec::with_capacity(9);

            for iter in 0..10 {
                let index = mmap.len() / 11 * iter;

                let instant = time::Instant::now();
                let _ = mmap[index];
                timings.push(instant.elapsed());
            }

            log::info!("Sparse reads of mmap {path:?} took {timings:?}");
        }
    }

    if let Ok(_) = env::var("MMAP_READ_ALL") {
        log::info!("Reading mmap {path:?} (size: {} bytes) to populate caches", mmap.len());

        let mut dst = vec![0; 8096];

        let instant = time::Instant::now();

        for iter in 0..(mmap.len() / dst.len()) {
            let start = dst.len() * iter;
            let end = start + dst.len();

            dst.copy_from_slice(&mmap[start..end]);
        }

        let rem = mmap.len() % dst.len();
        let start = mmap.len() - rem;

        if rem > 0 {
            dst[..rem].copy_from_slice(&mmap[start..]);
        }

        log::info!("Reading mmap {path:?} took {:?}", instant.elapsed());
    }

    madvise::madvise(&mmap, madvise::get_global())?;

    Ok(mmap)
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

pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, mem::size_of_val(v)) }
}
