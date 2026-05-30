use std::path::{Path, PathBuf};

use ahash::AHashMap;
use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;

use crate::common::operation_error::OperationResult;

const INDICES_TRACKER_FILE_NAME: &str = "indices_tracker.json";

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct IndicesTracker {
    pub map: AHashMap<DimId, DimId>,
}

impl IndicesTracker {
    pub fn open(path: &Path, max_index_fn: impl Fn() -> DimId) -> std::io::Result<Self> {
        let path = Self::file_path(path);
        if !path.exists() {
            let max_index = max_index_fn();
            Ok(IndicesTracker {
                map: (0..max_index).map(|i| (i, i)).collect(),
            })
        } else {
            Ok(read_json(&path)?)
        }
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        let path = Self::file_path(path);
        Ok(atomic_save_json(&path, self)?)
    }

    pub fn file_path(path: &Path) -> PathBuf {
        path.join(INDICES_TRACKER_FILE_NAME)
    }

    pub fn register_indices(&mut self, vector: &SparseVector) {
        for index in &vector.indices {
            if !self.map.contains_key(index) {
                self.map.insert(*index, self.map.len() as DimId);
            }
        }
    }

    pub fn remap_index(&self, index: DimId) -> Option<DimId> {
        self.map.get(&index).copied()
    }

    pub fn remap_vector(&self, mut vector: SparseVector) -> SparseVector {
        let mut placeholder_indices = self.map.len() as DimId;
        vector.indices.iter_mut().for_each(|index| {
            *index = if let Some(index) = self.remap_index(*index) {
                index
            } else {
                placeholder_indices += 1;
                placeholder_indices
            }
        });
        vector.sort_by_indices();
        vector
    }
}
