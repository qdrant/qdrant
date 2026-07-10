use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::mmap::{AdviceSetting, MULTI_MMAP_IS_SUPPORTED, create_and_ensure_length};
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, TypedStorage, UniversalIoError, UniversalRead,
    UniversalReadFs, UniversalWrite,
};

use super::config::{MMAP_CHUNKS_PATTERN_END, MMAP_CHUNKS_PATTERN_START};

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(MMAP_CHUNKS_PATTERN_START)
        .and_then(|file_name| file_name.strip_suffix(MMAP_CHUNKS_PATTERN_END))
        .and_then(|file_name| file_name.parse::<usize>().ok())
}

pub fn chunk_open_options(
    advice: AdviceSetting,
    populate: Populate,
    writeable: bool,
) -> OpenOptions {
    OpenOptions {
        writeable,
        need_sequential: *MULTI_MMAP_IS_SUPPORTED,
        populate,
        advice,
    }
}

/// Schedule background prefetch of every chunk file [`read_chunks`] will open.
pub fn preopen_chunks(
    fs: &impl CachedReadFs,
    directory: &Path,
    advice: AdviceSetting,
    populate: Populate,
) -> Result<(), UniversalIoError> {
    let chunks_prefix = directory.join(MMAP_CHUNKS_PATTERN_START);
    for listed in fs.list_files(&chunks_prefix)? {
        let is_chunk = listed
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(check_mmap_file_name_pattern)
            .is_some();

        if is_chunk {
            fs.schedule_prefetch(
                &listed.path,
                Some(chunk_open_options(advice, populate, false)),
                None,
            )?;
        }
    }
    Ok(())
}

pub fn read_chunks<T: bytemuck::Pod + Send, S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
    advice: AdviceSetting,
    populate: Populate,
    writeable: bool,
) -> Result<Vec<TypedStorage<S, T>>, UniversalIoError> {
    read_chunks_from(fs, directory, 0, advice, populate, writeable)
}

/// Open chunk files with id `>= start_chunk_id`, in ascending order.
pub fn read_chunks_from<T: bytemuck::Pod + Send, S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
    start_chunk_id: usize,
    advice: AdviceSetting,
    populate: Populate,
    writeable: bool,
) -> Result<Vec<TypedStorage<S, T>>, UniversalIoError> {
    // List only the chunk files via the prefix, so unrelated files in the
    // directory are never enumerated.
    let chunks_prefix = directory.join(MMAP_CHUNKS_PATTERN_START);
    let mut chunks_files: AHashMap<usize, _> = AHashMap::new();
    for listed in fs.list_files(&chunks_prefix)? {
        let path = listed.path;
        let chunk_id = path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(check_mmap_file_name_pattern);

        if let Some(chunk_id) = chunk_id {
            chunks_files.insert(chunk_id, path);
        }
    }

    let num_chunks = chunks_files.len();
    let mut result = Vec::with_capacity(num_chunks.saturating_sub(start_chunk_id));
    for chunk_id in start_chunk_id..num_chunks {
        let chunk_path = chunks_files.remove(&chunk_id).ok_or_else(|| {
            UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Missing chunk {chunk_id} in {}", directory.display(),),
            ))
        })?;

        let chunk = TypedStorage::open(
            fs,
            &chunk_path,
            chunk_open_options(advice, populate, writeable),
            Default::default(),
        )?;

        result.push(chunk);
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    directory.join(format!(
        "{MMAP_CHUNKS_PATTERN_START}{chunk_id}{MMAP_CHUNKS_PATTERN_END}",
    ))
}

pub fn create_chunk<T: bytemuck::Pod + Send, S: UniversalWrite>(
    fs: &S::Fs,
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
) -> Result<TypedStorage<S, T>, UniversalIoError> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;

    TypedStorage::open(
        fs,
        &chunk_file_path,
        OpenOptions {
            writeable: true,
            need_sequential: *MULTI_MMAP_IS_SUPPORTED,
            populate: Populate::No, // don't populate newly created chunk, as it's empty and will be filled later
            advice: AdviceSetting::Global,
        },
        Default::default(),
    )
}
