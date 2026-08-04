use std::path::{Path, PathBuf};

use common::fs::atomic_save_json;
use common::universal_io::{
    UniversalIoError, UniversalReadFileOps, UniversalReadFs, read_json_via, read_whole_via,
};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::CHUNK_SIZE;

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.dat";

pub(super) const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
// TODO: rename for other storages?
pub(super) const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

/// Contents of the status file: the number of stored vectors, mapped writable
/// and updated in place by [`ChunkedVectors`](super::ChunkedVectors).
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct Status {
    pub len: usize,
}

/// Contents of the config file, written once when the directory is created and
/// never rewritten afterwards.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChunkedVectorsConfig {
    pub(super) chunk_size_bytes: usize,
    pub(super) chunk_size_vectors: usize,
    pub(super) dim: usize,
    #[serde(default)]
    pub(super) populate: Option<bool>,
}

impl ChunkedVectorsConfig {
    pub fn get_chunk_index(&self, key: usize) -> usize {
        key / self.chunk_size_vectors
    }

    /// Element offset of the vector within its chunk
    pub fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.chunk_size_vectors;
        chunk_vector_idx * self.dim
    }

    /// Chunk index and element offset for `count` vectors of flattened length
    /// `vectors_len` starting at `start_key`, validated to fit in one chunk.
    pub fn chunk_slot(
        &self,
        start_key: usize,
        count: usize,
        vectors_len: usize,
    ) -> OperationResult<(usize, usize)> {
        assert_eq!(vectors_len, count * self.dim, "Vector size mismatch");

        let chunk_idx = self.get_chunk_index(start_key);
        let chunk_offset = self.get_chunk_offset(start_key);

        if chunk_offset + vectors_len > self.dim * self.chunk_size_vectors {
            return Err(OperationError::service_error(format!(
                "Vectors do not fit in the chunk. Chunk idx {chunk_idx}, chunk offset {chunk_offset}, vectors count {count}",
            )));
        }

        Ok((chunk_idx, chunk_offset))
    }
}

pub(super) fn config_file(directory: &Path) -> PathBuf {
    directory.join(CONFIG_FILE_NAME)
}

pub(super) fn status_file(directory: &Path) -> PathBuf {
    directory.join(STATUS_FILE_NAME)
}

/// Path of the status file, created zeroed if missing.
pub(super) fn ensure_status_file<Fs: UniversalReadFileOps>(
    fs: &Fs,
    directory: &Path,
) -> OperationResult<PathBuf> {
    let status_file = status_file(directory);
    if !fs.exists(&status_file)? {
        // TODO(uio): migrate when UniversalWriteFileOps is available
        common::mmap::create_and_ensure_length(&status_file, size_of::<Status>())?;
    }
    Ok(status_file)
}

/// Load the config, validating `dim`, creating the file if missing or
/// unreadable.
pub(super) fn ensure_config<T, Fs: UniversalReadFs>(
    fs: &Fs,
    directory: &Path,
    dim: usize,
    populate: bool,
) -> OperationResult<ChunkedVectorsConfig> {
    let config_file = config_file(directory);
    match load_config(fs, &config_file) {
        Ok(Some(config)) => {
            if config.dim == dim {
                Ok(config)
            } else {
                Err(OperationError::service_error(format!(
                    "Wrong configuration in {}: expected {}, found {dim}",
                    config_file.display(),
                    config.dim,
                )))
            }
        }
        Ok(None) => create_config::<T>(&config_file, dim, populate),
        Err(e) => {
            log::error!("Failed to deserialize config file {config_file:?}: {e}");
            create_config::<T>(&config_file, dim, populate)
        }
    }
}

fn create_config<T>(
    config_file: &Path,
    dim: usize,
    populate: bool,
) -> OperationResult<ChunkedVectorsConfig> {
    if dim == 0 {
        return Err(OperationError::service_error(
            "The vector's dimension cannot be 0",
        ));
    }

    let chunk_size_bytes = CHUNK_SIZE;
    let vector_size_bytes = dim * size_of::<T>();
    let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
    let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

    let config = ChunkedVectorsConfig {
        chunk_size_bytes: corrected_chunk_size_bytes,
        chunk_size_vectors,
        dim,
        populate: Some(populate),
    };
    atomic_save_json(config_file, &config)?;
    Ok(config)
}

/// Read the stored config, or `None` if the file does not exist yet.
pub(super) fn load_config<Fs: UniversalReadFs>(
    fs: &Fs,
    config_file: &Path,
) -> OperationResult<Option<ChunkedVectorsConfig>> {
    match read_json_via::<Fs, ChunkedVectorsConfig>(fs, config_file) {
        Ok(config) => Ok(Some(config)),
        Err(UniversalIoError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Read the stored vector count. Always goes to storage, so the result is
/// unaffected by any handle the caller already holds.
pub(super) fn read_status_len<Fs: UniversalReadFs>(
    fs: &Fs,
    status_file: &Path,
) -> OperationResult<usize> {
    let needed = std::mem::size_of::<usize>();
    let len = read_whole_via(fs, status_file, |bytes| {
        let head = bytes.get(..needed).ok_or_else(|| {
            UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Status file {} is too short: {} < {needed}",
                    status_file.display(),
                    bytes.len(),
                ),
            ))
        })?;
        Ok(usize::from_ne_bytes(head.try_into().expect("size matches")))
    })?;
    Ok(len)
}
