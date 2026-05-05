use serde::{Deserialize, Serialize};

pub(super) const CONFIG_FILE_NAME: &str = "config.json";
pub(super) const STATUS_FILE_NAME: &str = "status.dat";

pub(super) const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
// TODO: rename for other storages?
pub(super) const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

#[repr(C)]
pub struct Status {
    pub len: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChunkedVectorsConfig {
    pub(super) chunk_size_bytes: usize,
    pub(super) chunk_size_vectors: usize,
    pub(super) dim: usize,
    #[serde(default)]
    pub(super) populate: Option<bool>,
}
