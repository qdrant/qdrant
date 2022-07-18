use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::common::file_operations::{atomic_save_json, read_json};
use crate::entry::entry_point::OperationResult;

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
    #[serde(default)]
    pub max_indexing_threads: usize,
}

impl HnswGraphConfig {
    pub fn new(
        m: usize,
        ef_construct: usize,
        indexing_threshold: usize,
        max_indexing_threads: usize,
    ) -> Self {
        HnswGraphConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            indexing_threshold,
            max_indexing_threads,
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

    pub fn max_rayon_threads(&self) -> usize {
        let max_threads = self.max_indexing_threads;

        if max_threads == 0 {
            let num_cpu = num_cpus::get();
            std::cmp::max(1, num_cpu - 1)
        } else {
            max_threads
        }
    }
}
