use std::mem::{align_of, size_of};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::{io, mem, ptr};

use fs_err as fs;
use fs_err::{File, OpenOptions};
use memmap2::{Mmap, MmapMut};

use crate::madvise::{self, AdviceSetting, Madviseable};

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

static READ_ONLY_MODE: AtomicBool = AtomicBool::new(false);

/// Sets the global read-only mode for memory-mapped file operations.
///
/// When enabled, file operations are restricted:
/// - Existing files cannot be resized
/// - New files cannot be created
/// - Write mmaps return a permission denied error
///
/// This should typically be set once at application startup before other threads
/// perform file operations.
pub fn set_read_only_mode(read_only: bool) {
    READ_ONLY_MODE.store(read_only, Ordering::Release);
}

fn is_read_only_mode() -> bool {
    READ_ONLY_MODE.load(Ordering::Acquire)
}

/// Returns whether read-only mode is currently enabled.
pub fn read_only_mode_enabled() -> bool {
    is_read_only_mode()
}

pub fn create_and_ensure_length(path: &Path, length: usize) -> io::Result<File> {
    if path.exists() {
        if is_read_only_mode() {
            let file = OpenOptions::new().read(true).open(path)?;
            let actual_len = file.metadata()?.len();
            if actual_len != length as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "File {path:?} has length {actual_len} but expected {length} (cannot modify in read-only mode)"
                    ),
                ));
            }
            Ok(file)
        } else {
            // Try to open with write permissions first
            match OpenOptions::new()
                .read(true)
                .write(true)
                // Don't truncate because we explicitly set the length later
                .truncate(false)
                .open(path)
            {
                Ok(file) => {
                    let actual_len = file.metadata()?.len();
                    if actual_len != length as u64 {
                        // Try to set length, but if we get permission denied (read-only filesystem),
                        // propagate the error
                        file.set_len(length as u64)?;
                    }
                    Ok(file)
                }
                Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                    // If we get permission denied (e.g., read-only filesystem),
                    // try opening in read-only mode and check length
                    let file = OpenOptions::new().read(true).open(path)?;
                    let actual_len = file.metadata()?.len();
                    if actual_len != length as u64 {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            format!(
                                "File {path:?} has length {actual_len} but expected {length} (cannot modify on read-only filesystem)"
                            ),
                        ));
                    }
                    Ok(file)
                }
                Err(err) => Err(err),
            }
        }
    } else {
        if is_read_only_mode() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("Read-only mode forbids creating {path:?}"),
            ));
        }
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

pub fn open_read_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> io::Result<Mmap> {
    let file = if is_read_only_mode() {
        OpenOptions::new().read(true).open(path)?
    } else {
        match OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
        {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                // If we get permission denied (e.g., read-only filesystem),
                // try opening in read-only mode
                OpenOptions::new().read(true).open(path)?
            }
            Err(err) => return Err(err),
        }
    };

    let mmap = unsafe { Mmap::map(&file)? };

    // Populate before advising
    // Because we want to read data with normal advice
    if populate {
        mmap.populate();
    }

    madvise::madvise(&mmap, advice.resolve())?;

    Ok(mmap)
}

/// Opens a write-oriented memory map.
///
/// # Read-only mode
///
/// When read-only mode is enabled, this function returns a permission denied error
/// if the file does not exist. If the file already exists, it opens it in read-only mode.
pub fn open_write_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> io::Result<MmapMut> {
    if is_read_only_mode() {
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("Read-only mode forbids creating {path:?}"),
            ));
        }
        // For existing files, open in read-only mode and return an error since we can't return MmapMut
        // ideally the caller should use open_read_mmap for existing files
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("Read-only mode forbids write mapping {path:?}"),
        ));
    }

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

pub fn transmute_to_u8<T: Sized>(v: &T) -> &[u8] {
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
