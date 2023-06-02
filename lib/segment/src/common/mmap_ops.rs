use std::fs::OpenOptions;
use std::hint::black_box;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{io, mem, time};

use memmap2::{Mmap, MmapMut};

use crate::entry::entry_point::OperationResult;
use crate::madvise;
use crate::madvise::Madviseable;

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

    let mmap = unsafe { Mmap::map(&file)? };
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

#[derive(Clone, Debug)]
pub struct PrefaultMmapPages {
    mmap: PrefaultableMmap,
    path: Option<PathBuf>,
}

impl PrefaultMmapPages {
    pub fn from_mmap(mmap: Arc<Mmap>, path: Option<impl Into<PathBuf>>) -> Self {
        Self {
            mmap: mmap.into(),
            path: path.map(Into::into),
        }
    }

    pub fn from_mmap_mut(mmap: Arc<MmapMut>, path: Option<impl Into<PathBuf>>) -> Option<Self> {
        let task = Self {
            mmap: mmap.try_into().ok()?,
            path: path.map(Into::into),
        };

        Some(task)
    }

    pub fn exec(&self) {
        self.mmap.prefault(self.path.as_deref())
    }
}

#[derive(Clone, Debug)]
pub enum PrefaultableMmap {
    Mmap(Arc<Mmap>),
    MmapMut(Arc<MmapMut>),
}

impl PrefaultableMmap {
    pub fn prefault(&self, path: Option<&Path>) {
        let sep = path.map_or("", |_| " "); // space if `path` is `Some` or nothing
        let path = path.unwrap_or(Path::new("")); // path if `path` is `Some` or nothing

        let instant = time::Instant::now();

        if self.prefault_with_madv_populate_read(sep, path) {
            log::trace!(
                "Prefaulted mmap{sep}{path:?} with MADV_POPULATE_READ in {:?}",
                instant.elapsed(),
            );
        } else if self.prefault_with_explicit_read(sep, path) {
            log::trace!(
                "Prefaulted mmap{sep}{path:?} with explicit read in {:?}",
                instant.elapsed(),
            );
        } else {
            log::warn!("Failed to prefault mmap{sep}{path:?}!");
        }
    }

    fn prefault_with_madv_populate_read(&self, sep: &str, path: &Path) -> bool {
        // Prefaulting memmapped pages with `MADV_POPULATE_READ` is only supported on Linux
        //
        // The `cfg!(...)` runtime check could have been expressed as `#[cfg(...)]` conditional
        // compilation directive, but ergonomics of `cfg!(...)` check is better. 🤷‍♀️

        if cfg!(target_os = "linux") {
            return false;
        }

        log::trace!("Prefaulting mmap {sep}{path:?} with MADV_POPULATE_READ...");

        let res = self.madvise(madvise::Advice::PopulateRead);

        if let Err(err) = &res {
            log::error!("Failed to prefault memmap{sep}{path:?} with MADV_POPULATE_READ: {err}");
        }

        res.is_ok()
    }

    fn prefault_with_explicit_read(&self, sep: &str, path: &Path) -> bool {
        // Prefaulting memmapped pages with explicit read is only supported for `Mmap`, but not `MmapMut`

        let Self::Mmap(mmap) = self else {
            return false;
        };

        log::trace!("Prefaulting mmap{sep}{path:?} with explicit read...");

        let mut dst = [0; 8096];

        for chunk in mmap.chunks(dst.len()) {
            dst[..chunk.len()].copy_from_slice(chunk);
        }

        black_box(dst);

        true
    }
}

impl Madviseable for PrefaultableMmap {
    fn madvise(&self, advice: madvise::Advice) -> io::Result<()> {
        match self {
            Self::Mmap(mmap) => mmap.madvise(advice),
            Self::MmapMut(mmap) => mmap.madvise(advice),
        }
    }
}

impl From<Arc<Mmap>> for PrefaultableMmap {
    fn from(mmap: Arc<Mmap>) -> Self {
        Self::Mmap(mmap)
    }
}

impl TryFrom<Arc<MmapMut>> for PrefaultableMmap {
    type Error = Unsupported;

    fn try_from(mmap: Arc<MmapMut>) -> Result<Self, Self::Error> {
        // Prefaulting `MmapMut` pages is only supported on Linux:
        // - because prefaulting with `MADV_POPULATE_READ` is only supported on Linux
        // - and prefaulting with explicit read is not supported for `MmapMut`
        //
        // The `cfg!(...)` runtime check could have been expressed as `#[cfg(...)]` conditional
        // compilation directive, but ergonomics of `cfg!(...)` check is better. 🤷‍♀️

        if cfg!(target_os = "linux") {
            Ok(Self::MmapMut(mmap))
        } else {
            Err(Unsupported)
        }
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
#[error("prefaulting MmapMut pages is only supported on Linux")]
pub struct Unsupported;

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
