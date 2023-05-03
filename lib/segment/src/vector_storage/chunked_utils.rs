use std::collections::HashMap;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use crate::common::mmap_ops::{create_and_ensure_length, open_write_mmap, write_to_mmap};
use crate::entry::entry_point::{OperationError, OperationResult};

const STATUS_FILE_NAME: &str = "status.json";

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

pub struct MmapStatus {
    pub len: usize,
}

pub fn status_file(directory: &Path) -> PathBuf {
    directory.join(STATUS_FILE_NAME)
}

pub fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
    let status_file = status_file(directory);
    if !status_file.exists() {
        {
            let length = std::mem::size_of::<usize>() as u64;
            create_and_ensure_length(&status_file, length as usize)?;
        }
        let mut mmap = open_write_mmap(&status_file)?;
        write_to_mmap(&mut mmap, 0, 0usize);
        mmap.flush()?;
        Ok(mmap)
    } else {
        open_write_mmap(&status_file)
    }
}

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(MMAP_CHUNKS_PATTERN_START)
        .and_then(|file_name| file_name.strip_suffix(MMAP_CHUNKS_PATTERN_END))
        .and_then(|file_name| file_name.parse::<usize>().ok())
}

pub fn read_mmaps(directory: &Path) -> OperationResult<Vec<MmapMut>> {
    let mut result = Vec::new();
    let mut mmap_files: HashMap<usize, _> = HashMap::new();
    for entry in directory.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let chunk_id = path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .and_then(check_mmap_file_name_pattern);

            if let Some(chunk_id) = chunk_id {
                mmap_files.insert(chunk_id, path);
            }
        }
    }

    let num_chunks = mmap_files.len();
    for chunk_id in 0..num_chunks {
        let mmap_file = mmap_files.remove(&chunk_id).ok_or_else(|| {
            OperationError::service_error(format!(
                "Missing mmap chunk {} in {}",
                chunk_id,
                directory.display()
            ))
        })?;
        let mmap = open_write_mmap(&mmap_file)?;
        result.push(mmap);
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    let chunk_file_name = format!(
        "{}{}{}",
        MMAP_CHUNKS_PATTERN_START, chunk_id, MMAP_CHUNKS_PATTERN_END
    );
    directory.join(chunk_file_name)
}

pub fn create_chunk(
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
) -> OperationResult<MmapMut> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;
    open_write_mmap(&chunk_file_path)
}
