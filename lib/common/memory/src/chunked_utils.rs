use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::madvise::{Advice, AdviceSetting};
use crate::mmap_ops::{create_and_ensure_length, open_read_mmap, open_write_mmap};
use crate::mmap_type::{Error as MmapError, MmapFlusher, MmapSlice};
use crate::mmap_type_readonly::MmapSliceReadOnly;

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

/// Memory mapped chunk data, that can be read and written and also maintain sequential read-only view
#[derive(Debug)]
pub struct UniversalMmapChunk<T: Sized + 'static> {
    mmap: MmapSlice<T>,
    mmap_seq: MmapSliceReadOnly<T>,
}

impl<T: Sized + 'static> UniversalMmapChunk<T> {
    pub fn as_slice(&self) -> &[T] {
        &self.mmap
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.mmap
    }

    pub fn as_seq_slice(&self) -> &[T] {
        &self.mmap_seq
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
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

pub fn read_mmaps<T: Sized>(
    directory: &Path,
    mlock: bool,
    populate: bool,
    advice: AdviceSetting,
) -> Result<Vec<UniversalMmapChunk<T>>, MmapError> {
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
            MmapError::MissingFile(format!(
                "Missing mmap chunk {chunk_id} in {}",
                directory.display(),
            ))
        })?;
        let mmap = open_write_mmap(&mmap_file, advice, populate)?;

        let mmap_seq =
            open_read_mmap(&mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;

        // If unix, lock the memory
        #[cfg(unix)]
        if mlock {
            mmap.lock()?;
        }
        // If not, log warning and continue
        #[cfg(not(unix))]
        if mlock {
            log::warn!("Can't lock vectors in RAM, is not supported on this platform");
        }

        let chunk = unsafe {
            UniversalMmapChunk {
                mmap: MmapSlice::try_from(mmap)?,
                mmap_seq: MmapSliceReadOnly::try_from(mmap_seq)?,
            }
        };
        result.push(chunk);
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    directory.join(format!(
        "{MMAP_CHUNKS_PATTERN_START}{chunk_id}{MMAP_CHUNKS_PATTERN_END}",
    ))
}

pub fn create_chunk<T: Sized>(
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
    mlock: bool,
) -> Result<UniversalMmapChunk<T>, MmapError> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;
    let mmap = open_write_mmap(
        &chunk_file_path,
        AdviceSetting::Global,
        false, // don't populate newly created chunk, as it's empty and will be filled later
    )?;

    let mmap_seq = open_read_mmap(
        &chunk_file_path,
        AdviceSetting::Advice(Advice::Sequential),
        false,
    )?;

    #[cfg(unix)]
    if mlock {
        mmap.lock()?;
    }
    // If not, log warning and continue
    #[cfg(not(unix))]
    if mlock {
        log::warn!("Can't lock vectors in RAM, is not supported on this platform");
    }

    let chunk = unsafe {
        UniversalMmapChunk {
            mmap: MmapSlice::try_from(mmap)?,
            mmap_seq: MmapSliceReadOnly::try_from(mmap_seq)?,
        }
    };
    Ok(chunk)
}
