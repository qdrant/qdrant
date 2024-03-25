use std::collections::HashMap;
use std::path::{Path, PathBuf};

use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};

use crate::common::mmap_type::MmapSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

/// Memory mapped chunk data.
pub type MmapChunk<T> = MmapSlice<T>;

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(MMAP_CHUNKS_PATTERN_START)
        .and_then(|file_name| file_name.strip_suffix(MMAP_CHUNKS_PATTERN_END))
        .and_then(|file_name| file_name.parse::<usize>().ok())
}

pub fn read_mmaps<T: PrimitiveVectorElement>(
    directory: &Path,
) -> OperationResult<Vec<MmapChunk<T>>> {
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
    let mut result = Vec::with_capacity(num_chunks);
    for chunk_id in 0..num_chunks {
        let mmap_file = mmap_files.remove(&chunk_id).ok_or_else(|| {
            OperationError::service_error(format!(
                "Missing mmap chunk {chunk_id} in {}",
                directory.display(),
            ))
        })?;
        let mmap = open_write_mmap(&mmap_file)?;
        let chunk = unsafe { MmapChunk::try_from(mmap)? };
        result.push(chunk);
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    directory.join(format!(
        "{MMAP_CHUNKS_PATTERN_START}{chunk_id}{MMAP_CHUNKS_PATTERN_END}",
    ))
}

pub fn create_chunk<T: PrimitiveVectorElement>(
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
) -> OperationResult<MmapChunk<T>> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;
    let mmap = open_write_mmap(&chunk_file_path)?;
    let chunk = unsafe { MmapChunk::try_from(mmap)? };
    Ok(chunk)
}
