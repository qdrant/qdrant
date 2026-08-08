use std::path::{Path, PathBuf};

use common::universal_io::{
    UniversalIoError, UniversalReadFs, UniversalWriteFileOps, read_json_via, read_whole_via,
};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::CHUNK_SIZE;

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.dat";

pub(super) const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
// TODO: rename for other storages?
pub(super) const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

/// Contents of the status file: the number of stored vectors, updated in
/// place by the writers.
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

    pub fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.chunk_size_vectors;
        chunk_vector_idx * self.dim
    }

    /// How many vectors still fit in the chunk holding `key`, starting at it.
    pub fn remaining_chunk_capacity(&self, key: usize) -> usize {
        self.chunk_size_vectors - key % self.chunk_size_vectors
    }
}

/// Load the stored config, or create it (and its file) on first open.
pub(super) fn ensure_config<T, Fs>(
    fs: &Fs,
    directory: &Path,
    dim: usize,
    populate: bool,
) -> OperationResult<ChunkedVectorsConfig>
where
    Fs: UniversalReadFs + UniversalWriteFileOps,
{
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
        Ok(None) => create_config::<T>(fs, &config_file, dim, populate),
        Err(e) => {
            log::error!("Failed to deserialize config file {config_file:?}: {e}");
            create_config::<T>(fs, &config_file, dim, populate)
        }
    }
}

fn create_config<T>(
    fs: &impl UniversalWriteFileOps,
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
    let vector_size_bytes = dim * std::mem::size_of::<T>();
    let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
    let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

    let config = ChunkedVectorsConfig {
        chunk_size_bytes: corrected_chunk_size_bytes,
        chunk_size_vectors,
        dim,
        populate: Some(populate),
    };
    fs.atomic_save(config_file, &serde_json::to_vec(&config)?)?;
    Ok(config)
}

pub(super) fn config_file(directory: &Path) -> PathBuf {
    directory.join(CONFIG_FILE_NAME)
}

pub(super) fn status_file(directory: &Path) -> PathBuf {
    directory.join(STATUS_FILE_NAME)
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
