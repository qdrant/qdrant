use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::{LazyLock, OnceLock};
use std::{io, mem, ptr};

use fs_err as fs;
use fs_err::{File, OpenOptions};
use fs4::FileExt;
use memmap2::{Mmap, MmapMut};

use super::advice::{AdviceSetting, Madviseable, madvise};

pub const TEMP_FILE_EXTENSION: &str = "tmp";

/// If multiple mmaps to the same file are supported in this environment
///
/// Some environments corrupt data on the file system if multiple memory maps are opened on the
/// same piece of data. This variable allows disabling the use of multiple memory maps at runtime.
/// An example of such environment is Docker on Windows with a mount into Windows.
pub static MULTI_MMAP_IS_SUPPORTED: LazyLock<bool> = LazyLock::new(|| {
    let mut supported = true;

    // Opt-out if multi-mmap support check at startup failed
    match MULTI_MMAP_SUPPORT_CHECK_RESULT.get() {
        Some(true) => {}
        Some(false) => {
            log::warn!(
                "Not using multi-mmap due to limited support, you may see reduced performance",
            );
            supported = false;
        }
        None => {
            // We hit this branch if MULTI_MMAP_SUPPORT_CHECK_RESULT was never explicitly set
            // In tests this can be safely ignored because we don't start through main.rs there
            log::warn!(
                "MULTI_MMAP_SUPPORT_CHECK_RESULT should be initialized before accessing MULTI_MMAP_IS_SUPPORTED"
            );
        }
    }

    // Opt-out if environment variable is set
    if supported && std::env::var_os("QDRANT_NO_MULTI_MMAP").is_some_and(|val| !val.is_empty()) {
        supported = false;
        log::warn!(
            "Not using multi-mmap because QDRANT_NO_MULTI_MMAP is set, you may see reduced performance"
        );
    }

    supported
});

/// If multi-mmap support is checked at Qdrant startup, the result is stored in this cell.
pub static MULTI_MMAP_SUPPORT_CHECK_RESULT: OnceLock<bool> = OnceLock::new();

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

        fs::rename(&temp_path, path)?;

        OpenOptions::new().read(true).write(true).open(path)
    }
}

/// Like [`create_and_ensure_length`], but also reserves the file's blocks on
/// the filesystem.
///
/// Files created this way are written through a `MAP_SHARED` memory map by
/// their callers, for example the gridstore page and tracker files. Without
/// the reservation those files stay sparse, and when the filesystem runs out
/// of free blocks mid-write the kernel raises `SIGBUS` and kills the process,
/// which no error handling can catch. With the reservation, a full disk
/// surfaces here as a regular `ENOSPC` error, before anything is written
/// through the mapping.
///
/// On filesystems that do not support `fallocate` this degrades to the
/// previous sparse behavior (with a warning), so that creating a store keeps
/// working everywhere.
pub fn create_and_ensure_length_allocated(path: &Path, length: usize) -> io::Result<File> {
    let file = create_and_ensure_length(path, length)?;
    reserve_blocks(&file, length)?;
    Ok(file)
}

/// Reserves physical blocks for the first `length` bytes of `file`.
fn reserve_blocks(file: &File, length: usize) -> io::Result<()> {
    if length == 0 {
        return Ok(());
    }

    match file.file().allocate(length as u64) {
        Ok(()) => Ok(()),
        // Filesystems without fallocate support, like some network or FUSE
        // mounts, keep the previous sparse behavior.
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
            ) || err.raw_os_error() == Some(38 /* ENOSYS on Linux */) =>
        {
            static WARNED: OnceLock<()> = OnceLock::new();
            if WARNED.set(()).is_ok() {
                log::warn!(
                    "Filesystem does not support reserving file blocks with fallocate; files will stay sparse, which can crash Qdrant with SIGBUS if the disk fills up while writing through a memory map: {err}"
                );
            }
            Ok(())
        }
        // Any other error, most importantly `ENOSPC`, fails the creation
        // here where callers can handle it.
        Err(err) => Err(err),
    }
}

pub fn open_read_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> io::Result<Mmap> {
    let file = OpenOptions::new().read(true).open(path)?;

    let mmap = unsafe { Mmap::map(&file)? };

    // Populate before advising
    // Because we want to read data with normal advice
    if populate {
        mmap.populate();
    }

    madvise(&mmap, advice.resolve())?;

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

    madvise(&mmap, advice.resolve())?;

    Ok(mmap)
}

/// # Safety
///
/// `data` must have correct alignment and size for `T` and contain correct bit patterns for the type `T`.
#[deprecated = "use `bytemuck` or `zerocopy`"]
pub unsafe fn transmute_from_u8<T>(v: &[u8]) -> &T {
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

/// # Safety
///
/// T must be a type with stable representation (POD type, Option with niche optimization, etc).
#[deprecated = "use `bytemuck` or `zerocopy`"]
pub unsafe fn transmute_to_u8<T: Sized>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(ptr::from_ref::<T>(v).cast::<u8>(), mem::size_of_val(v)) }
}

/// # Safety
///
/// `data` must have correct alignment for `T` and contain correct bit patterns for the type `T`.
#[deprecated = "use `bytemuck` or `zerocopy`"]
pub unsafe fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
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

/// # Safety
///
/// T must be a type with stable representation (POD type, Option with niche optimization, etc).
#[deprecated = "use `bytemuck` or `zerocopy`"]
pub unsafe fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), mem::size_of_val(v)) }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;

    #[test]
    fn test_create_and_ensure_length_is_sparse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.dat");

        create_and_ensure_length(&path, 64 * 1024).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 64 * 1024);
        assert!(
            metadata.blocks() * 512 < 64 * 1024,
            "expected a sparse file, but {} of {} bytes are allocated",
            metadata.blocks() * 512,
            metadata.len(),
        );
    }

    #[test]
    fn test_create_and_ensure_length_allocated_reserves_all_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reserved.dat");

        create_and_ensure_length_allocated(&path, 64 * 1024).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 64 * 1024);
        assert!(
            metadata.blocks() * 512 >= 64 * 1024,
            "expected a fully allocated file, but only {} of {} bytes are allocated",
            metadata.blocks() * 512,
            metadata.len(),
        );
    }

    #[test]
    fn test_create_and_ensure_length_allocated_reserves_grown_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grown.dat");

        create_and_ensure_length_allocated(&path, 8 * 1024).unwrap();
        create_and_ensure_length_allocated(&path, 64 * 1024).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 64 * 1024);
        assert!(
            metadata.blocks() * 512 >= 64 * 1024,
            "expected the grown file to be fully allocated, but only {} of {} bytes are allocated",
            metadata.blocks() * 512,
            metadata.len(),
        );
    }
}
