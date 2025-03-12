use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;
use crate::common::operation_error::OperationResult;
use crate::types::VectorStorageDatatype;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

/// Sparse index types
#[derive(
    Default, Hash, Debug, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone,
)]
pub enum SparseIndexType {
    /// Mutable RAM sparse index
    #[default]
    MutableRam,
    /// Immutable RAM sparse index
    ImmutableRam,
    /// Mmap sparse index
    Mmap,
}

impl SparseIndexType {
    pub fn is_appendable(self) -> bool {
        self == Self::MutableRam
    }

    pub fn is_immutable(self) -> bool {
        self != Self::MutableRam
    }

    pub fn is_on_disk(self) -> bool {
        self == Self::Mmap
    }

    pub fn is_persisted(self) -> bool {
        self == Self::Mmap || self == Self::ImmutableRam
    }
}

/// Configuration for sparse inverted index.
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Anonymize, Copy, Clone, PartialEq, Eq, Default,
)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexConfig {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    #[anonymize(false)]
    pub full_scan_threshold: Option<usize>,
    /// Type of sparse index
    pub index_type: SparseIndexType,
    /// Datatype used to store weights in the index.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl SparseIndexConfig {
    pub fn new(
        full_scan_threshold: Option<usize>,
        index_type: SparseIndexType,
        datatype: Option<VectorStorageDatatype>,
    ) -> Self {
        SparseIndexConfig {
            full_scan_threshold,
            index_type,
            datatype,
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
