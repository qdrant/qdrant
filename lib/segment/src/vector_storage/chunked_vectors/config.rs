use std::path::{Path, PathBuf};

use common::universal_io::{UniversalIoError, UniversalReadFs, read_json_via, read_whole_via};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;

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
