use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;

pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub struct HnswGraphConfig {
    pub m: usize,
    /// Requested M
    pub m0: usize,
    /// Actual M on level 0
    pub ef_construct: usize,
    /// Number of neighbours to search on construction
    pub ef: usize,
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    #[serde(alias = "indexing_threshold")]
    pub full_scan_threshold: Option<usize>,
    #[serde(default)]
    pub max_indexing_threads: usize,
    #[serde(default)]
    pub payload_m: Option<usize>,
    #[serde(default)]
    pub payload_m0: Option<usize>,
    #[serde(default)]
    pub indexed_vector_count: Option<usize>,
}

impl HnswGraphConfig {
    pub fn new(
        m: usize,
        ef_construct: usize,
        max_indexing_threads: usize,
        payload_m: Option<usize>,
    ) -> Self {
        HnswGraphConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            full_scan_threshold: None,
            max_indexing_threads,
            payload_m,
            payload_m0: payload_m.map(|v| v * 2),
            indexed_vector_count: None,
        }
    }

    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(HNSW_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_json(path, self)?)
    }
}
