use std::path::{Path, PathBuf};

use common::fs::{atomic_save_json, read_json};
use common::universal_io::{UniversalReadFs, read_json_via};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;
use crate::common::operation_error::OperationResult;
use crate::types::{Memory, VectorStorageDatatype};

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

    pub fn from_on_disk(on_disk: bool) -> Self {
        if on_disk {
            Self::Mmap
        } else {
            Self::MutableRam
        }
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
    /// Requested memory placement of the index.
    ///
    /// The structural decision is carried by `index_type`; this field additionally
    /// distinguishes `cold` from `cached` for the mmap index variant.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
}

impl SparseIndexConfig {
    pub fn new(
        full_scan_threshold: Option<usize>,
        index_type: SparseIndexType,
        datatype: Option<VectorStorageDatatype>,
        memory: Option<Memory>,
    ) -> Self {
        SparseIndexConfig {
            full_scan_threshold,
            index_type,
            datatype,
            memory,
        }
    }

    /// Effective memory placement of the index, derived from the structural `index_type` and
    /// refined by the requested `memory` placement.
    pub fn memory_placement(&self) -> Memory {
        match self.index_type {
            // Mutable and immutable RAM indexes are heap structures: pinned by construction
            SparseIndexType::MutableRam | SparseIndexType::ImmutableRam => Memory::Pinned,
            // Mmap index is cold by default, but may be requested to be cached
            SparseIndexType::Mmap => match self.memory {
                Some(Memory::Cached) => Memory::Cached,
                Some(Memory::Cold) | Some(Memory::Pinned) | None => Memory::Cold,
            },
        }
    }

    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(SPARSE_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    /// Universal-IO variant of [`Self::load`].
    pub fn load_universal<Fs: UniversalReadFs>(fs: &Fs, path: &Path) -> OperationResult<Self> {
        Ok(read_json_via(fs, path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_json(path, self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_index_memory_placement() {
        // RAM index variants are heap structures: pinned by construction
        let mutable = SparseIndexConfig::new(None, SparseIndexType::MutableRam, None, None);
        assert_eq!(mutable.memory_placement(), Memory::Pinned);
        let immutable = SparseIndexConfig::new(None, SparseIndexType::ImmutableRam, None, None);
        assert_eq!(immutable.memory_placement(), Memory::Pinned);

        // Mmap index is cold by default and may be requested to be cached
        let mmap = SparseIndexConfig::new(None, SparseIndexType::Mmap, None, None);
        assert_eq!(mmap.memory_placement(), Memory::Cold);
        let cached =
            SparseIndexConfig::new(None, SparseIndexType::Mmap, None, Some(Memory::Cached));
        assert_eq!(cached.memory_placement(), Memory::Cached);

        // The `memory` field survives a serde round-trip, and is omitted when unset
        // for backward compatibility
        let json = serde_json::to_string(&cached).unwrap();
        let restored: SparseIndexConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, cached);
        let json = serde_json::to_string(&mmap).unwrap();
        assert!(!json.contains("memory"));
    }
}
