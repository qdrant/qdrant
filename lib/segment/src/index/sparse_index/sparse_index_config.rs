use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub struct SparseIndexConfig {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    pub full_scan_threshold: usize,
}

impl SparseIndexConfig {
    pub fn new(full_scan_threshold: usize) -> Self {
        SparseIndexConfig {
            full_scan_threshold,
        }
    }

    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(SPARSE_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_json(path, self)?)
    }
}
