use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::mmap::{AdviceSetting, MULTI_MMAP_IS_SUPPORTED, create_and_ensure_length};
use common::universal_io::{
    ListedFile, OpenOptions, Populate, ReadRange, TypedStorage, UniversalIoError, UniversalRead,
    UniversalReadFs, UniversalWrite,
};

use super::config::{MMAP_CHUNKS_PATTERN_END, MMAP_CHUNKS_PATTERN_START};

/// The leading vectors of a chunked storage that reads can reach: none past
/// `len` is ever read, so the chunks holding them need not be downloaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisiblePrefix {
    /// Vectors visible to reads, counted from the first.
    pub len: usize,
    /// Elements per vector, which the chunk capacity derives from.
    pub dim: usize,
}

/// Populate for chunk `chunk_id` when only the first `visible_len` vectors are
/// ever read: a chunk wholly past them opens cold, the chunk they end in
/// populates its visible part only, and the rest keep `populate`. Never
/// promotes — a chunk `populate` leaves cold stays cold.
pub(super) fn visible_chunk_populate(
    populate: Populate,
    chunk_id: usize,
    chunk_size_vectors: usize,
    vector_size_bytes: usize,
    visible_len: Option<usize>,
) -> Populate {
    let Some(visible_len) = visible_len else {
        return populate;
    };
    if chunk_size_vectors == 0 {
        return populate;
    }
    let chunk_start = chunk_id * chunk_size_vectors;
    let visible_in_chunk = visible_len
        .saturating_sub(chunk_start)
        .min(chunk_size_vectors);
    if visible_in_chunk == chunk_size_vectors {
        return populate;
    }
    if visible_in_chunk == 0 {
        return Populate::No;
    }
    match populate {
        Populate::Blocking | Populate::PreferBackground => Populate::Partial(ReadRange::new(
            0,
            (visible_in_chunk * vector_size_bytes) as u64,
        )),
        Populate::Auto | Populate::No | Populate::Partial(_) => populate,
    }
}

/// Path prefix every chunk file shares. Listing by it keeps unrelated files in
/// the directory from being enumerated at all.
pub(super) fn chunks_prefix(directory: &Path) -> PathBuf {
    directory.join(MMAP_CHUNKS_PATTERN_START)
}

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
pub(super) fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
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

pub fn read_chunks<T: bytemuck::Pod + Send, S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
    advice: AdviceSetting,
    populate: Populate,
    writeable: bool,
) -> Result<Vec<TypedStorage<S, T>>, UniversalIoError> {
    read_chunks_from(fs, directory, 0, advice, |_| populate, writeable)
}

/// List the chunk files under `directory`, keyed by chunk id.
pub(super) fn list_chunk_files(
    fs: &impl UniversalReadFs,
    directory: &Path,
) -> Result<AHashMap<usize, ListedFile>, UniversalIoError> {
    let mut chunks_files = AHashMap::new();
    for listed in fs.list_files(&chunks_prefix(directory))? {
        let chunk_id = listed
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(check_mmap_file_name_pattern);

        if let Some(chunk_id) = chunk_id {
            chunks_files.insert(chunk_id, listed);
        }
    }
    Ok(chunks_files)
}

/// Open chunk files with id `>= start_chunk_id`, in ascending order, each with
/// the populate `populate` gives for its id.
pub fn read_chunks_from<T: bytemuck::Pod + Send, S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
    start_chunk_id: usize,
    advice: AdviceSetting,
    populate: impl Fn(usize) -> Populate,
    writeable: bool,
) -> Result<Vec<TypedStorage<S, T>>, UniversalIoError> {
    let mut chunks_files = list_chunk_files(fs, directory)?;
    let num_chunks = chunks_files.len();
    let mut result = Vec::with_capacity(num_chunks.saturating_sub(start_chunk_id));
    for chunk_id in start_chunk_id..num_chunks {
        let chunk_path = chunks_files
            .remove(&chunk_id)
            .ok_or_else(|| {
                UniversalIoError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Missing chunk {chunk_id} in {}", directory.display(),),
                ))
            })?
            .path;

        let chunk = TypedStorage::open(
            fs,
            &chunk_path,
            chunk_open_options(advice, populate(chunk_id), writeable),
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

#[cfg(test)]
mod tests {
    use super::*;

    const PER_CHUNK: usize = 100;
    const VECTOR_BYTES: usize = 16;

    fn chunk_populate(populate: Populate, chunk_id: usize, visible_len: usize) -> Populate {
        visible_chunk_populate(
            populate,
            chunk_id,
            PER_CHUNK,
            VECTOR_BYTES,
            Some(visible_len),
        )
    }

    #[test]
    fn chunks_before_the_prefix_keep_populate() {
        assert_eq!(
            chunk_populate(Populate::Blocking, 0, 250),
            Populate::Blocking
        );
        assert_eq!(
            chunk_populate(Populate::PreferBackground, 1, 200),
            Populate::PreferBackground,
        );
        // No prefix: every chunk keeps it.
        assert_eq!(
            visible_chunk_populate(Populate::Blocking, 7, PER_CHUNK, VECTOR_BYTES, None),
            Populate::Blocking,
        );
    }

    #[test]
    fn the_chunk_the_prefix_ends_in_populates_its_visible_part() {
        assert_eq!(
            chunk_populate(Populate::Blocking, 2, 250),
            Populate::Partial(ReadRange::new(0, (50 * VECTOR_BYTES) as u64)),
        );
        // Never promotes: a cold chunk stays cold, `Auto` stays the backend's call.
        assert_eq!(chunk_populate(Populate::No, 2, 250), Populate::No);
        assert_eq!(chunk_populate(Populate::Auto, 2, 250), Populate::Auto);
    }

    #[test]
    fn chunks_past_the_prefix_open_cold() {
        assert_eq!(chunk_populate(Populate::Blocking, 3, 250), Populate::No);
        assert_eq!(chunk_populate(Populate::Auto, 2, 200), Populate::No);
        assert_eq!(chunk_populate(Populate::Blocking, 0, 0), Populate::No);
    }
}
