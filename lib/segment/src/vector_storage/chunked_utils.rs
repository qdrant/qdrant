use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::MmapMut;

use crate::common::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8_to_mut_slice_shared,
};
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

pub struct MmapChunk {
    /// Memory mapped file for chunk data.
    ///
    /// This should never be accessed directly, because it shares a mutable reference with
    /// [`data`]. Use that instead. The sole purpose of this is to keep ownership of
    /// the mmap, and to properly clean it up when this struct is dropped.
    _mmap: Arc<MmapMut>,
    /// A convenient `&mut [VectorElementType]` view into the memory map file.
    ///
    /// This has the same lifetime as this struct, a borrow must never be leased out for longer.
    data: &'static mut [VectorElementType],
}

impl MmapChunk {
    pub fn new(mut mmap: MmapMut) -> Self {
        let data = unsafe { transmute_from_u8_to_mut_slice_shared(&mut mmap) };
        Self {
            _mmap: Arc::new(mmap),
            data,
        }
    }

    pub fn data(&self) -> &[VectorElementType] {
        self.data
    }

    pub fn data_mut(&mut self) -> &mut [VectorElementType] {
        self.data
    }

    pub fn flusher(&self) -> Flusher {
        let mmap = self._mmap.clone();
        Box::new(move || {
            mmap.flush()?;
            Ok(())
        })
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

pub fn read_mmaps(directory: &Path) -> OperationResult<Vec<MmapChunk>> {
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
        let chunk = MmapChunk::new(mmap);
        result.push(chunk);
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
) -> OperationResult<MmapChunk> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;
    let mmap = open_write_mmap(&chunk_file_path)?;
    let chunk = MmapChunk::new(mmap);
    Ok(chunk)
}
