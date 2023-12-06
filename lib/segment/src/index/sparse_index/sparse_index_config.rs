use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;
use crate::common::operation_error::OperationResult;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

/// Sparse index types
#[derive(Default, Hash, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq, Copy, Clone)]
pub enum SparseIndexType {
    /// Mutable RAM sparse index
    #[default]
    MutableRam,
    /// Immutable RAM sparse index
    ImmutableRam,
    /// Mmap sparse index
    Mmap,
}

/// Configuration for sparse inverted index.
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexConfig {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    pub full_scan_threshold: Option<usize>,
    /// Type of sparse index
    pub index_type: SparseIndexType,
}

impl Anonymize for SparseIndexConfig {
    fn anonymize(&self) -> Self {
        SparseIndexConfig {
            full_scan_threshold: self.full_scan_threshold,
            index_type: self.index_type,
        }
    }
}

impl SparseIndexConfig {
    pub fn new(full_scan_threshold: Option<usize>, index_type: SparseIndexType) -> Self {
        SparseIndexConfig {
            full_scan_threshold,
            index_type,
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
