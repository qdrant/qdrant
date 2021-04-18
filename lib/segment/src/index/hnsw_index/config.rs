use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use crate::entry::entry_point::OperationResult;
use crate::common::file_operations::{read_json, atomic_save_json};

pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub struct HnswConfig {
    pub m: usize,
    // Requested M
    pub max_m: usize,
    // Actual M in top layers (might be bigger based on indexed fields)
    pub max_m0: usize,
    // Actual M on level 0
    pub ef_construct: usize,
    // Number of neighbours to search on construction
    pub ef: usize,
    // Max Number of layers
    pub level_factor: f64, // Factor of level probability
}

impl HnswConfig {

    pub fn new(m: usize, ef_construct: usize) -> Self {
        HnswConfig {
            m,
            max_m: m,
            max_m0: m * 2,
            ef_construct,
            ef: ef_construct,
            level_factor: 1.0 / (m as f64).ln()
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

