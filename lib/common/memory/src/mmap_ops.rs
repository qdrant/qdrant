use std::fs::OpenOptions;
use std::hint::black_box;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{io, mem, ops, time};

use memmap2::{Mmap, MmapMut};

use crate::madvise;
use crate::madvise::Madviseable;

pub fn create_and_ensure_length(path: &Path, length: usize) -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    file.set_len(length as u64)?;
    Ok(())
}

pub fn open_read_mmap(path: &Path) -> io::Result<Mmap> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(true)
        .create(true)
        .open(path)?;

    let mmap = unsafe { Mmap::map(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;

    Ok(mmap)
}

pub fn open_write_mmap(path: &Path) -> io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;

    Ok(mmap)
}

#[derive(Clone, Debug)]
pub struct PrefaultMmapPages {
    mmap: Arc<Mmap>,
    path: Option<PathBuf>,
}

impl PrefaultMmapPages {
    pub fn new(mmap: Arc<Mmap>, path: Option<impl Into<PathBuf>>) -> Self {
        Self {
            mmap,
            path: path.map(Into::into),
        }
    }

    pub fn exec(&self) {
        prefault_mmap_pages(self.mmap.as_ref(), self.path.as_deref());
    }
}

fn prefault_mmap_pages<T>(mmap: &T, path: Option<&Path>)
where
    T: Madviseable + ops::Deref<Target = [u8]>,
{
    let separator = path.map_or("", |_| " "); // space if `path` is `Some` or nothing
    let path = path.unwrap_or(Path::new("")); // path if `path` is `Some` or nothing

    log::trace!("Reading mmap{separator}{path:?} to populate cache...");

    let instant = time::Instant::now();

    let mut dst = [0; 8096];

    for chunk in mmap.chunks(dst.len()) {
        dst[..chunk.len()].copy_from_slice(chunk);
    }

    black_box(dst);

    log::trace!(
        "Reading mmap{separator}{path:?} to populate cache took {:?}",
        instant.elapsed()
    );
}
pub fn transmute_from_u8<T>(v: &[u8]) -> &T {
    debug_assert_eq!(v.len(), size_of::<T>());
    unsafe { &*(v.as_ptr() as *const T) }
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
