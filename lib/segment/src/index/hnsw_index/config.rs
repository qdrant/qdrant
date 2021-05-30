use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use crate::entry::entry_point::OperationResult;
use crate::common::file_operations::{read_json, atomic_save_json};

pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub struct HnswGraphConfig {
    pub m: usize,
    /// Requested M
    pub m0: usize,
    /// Actual M on level 0
    pub ef_construct: usize,
    /// Number of neighbours to search on construction
    pub ef: usize,
    /// Minimal number of vectors to perform indexing
    pub indexing_threshold: usize,
}

impl HnswGraphConfig {

    pub fn new(m: usize, ef_construct: usize, indexing_threshold: usize) -> Self {
        HnswGraphConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            indexing_threshold
        }
    }

    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(HNSW_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        read_json(path)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        atomic_save_json(path, self)
    }
}

