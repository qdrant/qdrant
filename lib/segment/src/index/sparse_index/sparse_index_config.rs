use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;
use crate::common::operation_error::OperationResult;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

/// Configuration for sparse inverted index.
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexConfig {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_scan_threshold: Option<usize>,
    /// Store index on disk. If set to false, the index will be stored in RAM. Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

impl Anonymize for SparseIndexConfig {
    fn anonymize(&self) -> Self {
        SparseIndexConfig {
            full_scan_threshold: self.full_scan_threshold,
            on_disk: self.on_disk,
        }
    }
}

impl SparseIndexConfig {
    pub fn new(full_scan_threshold: Option<usize>, on_disk: Option<bool>) -> Self {
        SparseIndexConfig {
            full_scan_threshold,
            on_disk,
        }
    }

    pub fn update_from_other(&mut self, other: &SparseIndexConfig) {
        if let Some(full_scan_threshold) = other.full_scan_threshold {
            self.full_scan_threshold = Some(full_scan_threshold);
        }
        if let Some(on_disk) = other.on_disk {
            self.on_disk = Some(on_disk);
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
