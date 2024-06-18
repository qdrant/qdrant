use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::sparse_vector_index::USE_COMPRESSED;
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

/// Storage types for vectors
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SparseVectorIndexDatatype {
    // Single-precision floating point
    #[default]
    Float32,
    // Half-precision floating point
    Float16,
}

impl SparseVectorIndexDatatype {
    fn should_hide(&self) -> bool {
        // Hide default value until the feature is released
        !USE_COMPRESSED && *self == Self::Float32
    }
}

/// Configuration for sparse inverted index.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexConfig {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    pub full_scan_threshold: Option<usize>,
    /// Type of sparse index
    pub index_type: SparseIndexType,
    /// Datatype used to store weights in the index.
    #[serde(default)]
    #[serde(skip_serializing_if = "SparseVectorIndexDatatype::should_hide")]
    #[schemars(skip) /* TODO(1.10): remove this in the next minor release to expose the compressed sparse index */]
    pub datatype: SparseVectorIndexDatatype,
}

impl Anonymize for SparseIndexConfig {
    fn anonymize(&self) -> Self {
        SparseIndexConfig {
            full_scan_threshold: self.full_scan_threshold,
            index_type: self.index_type,
            datatype: self.datatype,
        }
    }
}

impl SparseIndexConfig {
    pub fn new(
        full_scan_threshold: Option<usize>,
        index_type: SparseIndexType,
        datatype: SparseVectorIndexDatatype,
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
