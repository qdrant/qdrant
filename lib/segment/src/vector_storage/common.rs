use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use fs_err::{File, OpenOptions};

use crate::common::operation_error::OperationResult;

static ASYNC_SCORER: AtomicBool = AtomicBool::new(false);

pub fn set_async_scorer(async_scorer: bool) {
    ASYNC_SCORER.store(async_scorer, Ordering::Relaxed);
}

pub fn get_async_scorer() -> bool {
    ASYNC_SCORER.load(Ordering::Relaxed)
}

/// Minimal number of bytes we read from disk in one go
/// WARN: this might be system dependent, so we assume 4Kb, which might be wrong
/// ToDo: read this from system
pub const PAGE_SIZE_BYTES: usize = 4096;

/// Number of vectors we read from storage in one batch
/// in case we need to score an iterator of vector ids
pub const VECTOR_READ_BATCH_SIZE: usize = 64;

#[cfg(any(test, feature = "testing"))]
pub const CHUNK_SIZE: usize = 512 * 1024;

/// Vector storage chunk size in bytes
#[cfg(not(any(test, feature = "testing")))]
pub const CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Ensure the given mmap file exists and is the given size.
///
/// # Arguments
/// * `path`: path of the file.
/// * `header`: header to set when the file is newly created.
/// * `size`: set the file size in bytes, filled with zeroes.
pub(crate) fn ensure_mmap_file_size(
    path: &Path,
    header: &[u8],
    size: Option<u64>,
) -> OperationResult<()> {
    // If it exists, only set the length
    if path.exists() {
        if let Some(size) = size {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(size)?;
        }
        return Ok(());
    }

    // Create file, and make it the correct size
    let mut file = File::create(path)?;
    file.write_all(header)?;
    if let Some(size) = size
        && size > header.len() as u64
    {
        file.set_len(size)?;
    }
    Ok(())
}
