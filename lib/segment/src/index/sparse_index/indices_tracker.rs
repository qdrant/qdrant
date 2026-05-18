use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::fs::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::{RemappedSparseVector, SparseVector};
use sparse::common::types::{DimId, DimOffset};

use crate::common::operation_error::OperationResult;

const INDICES_TRACKER_FILE_NAME: &str = "indices_tracker.json";

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct IndicesTracker {
    pub map: AHashMap<DimId, DimOffset>,
}

impl IndicesTracker {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let path = Self::file_path(path);
        read_json(&path)
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

    pub fn remap_index(&self, index: DimId) -> Option<DimOffset> {
        self.map.get(&index).copied()
    }

    /// Remap a sparse vector to internal segment-specific indices.
    ///
    /// Unknown dimensions ids are filtered out.
    pub fn remap_vector(&self, vector: SparseVector) -> RemappedSparseVector {
        let SparseVector {
            mut indices,
            mut values,
        } = vector;

        let mut write = 0;
        for read in 0..indices.len() {
            if let Some(remapped_index) = self.remap_index(indices[read]) {
                indices[write] = remapped_index;
                values[write] = values[read];
                write += 1;
            }
        }
        indices.truncate(write);
        values.truncate(write);

        let mut remapped_vector = RemappedSparseVector { indices, values };
        remapped_vector.sort_by_indices();
        remapped_vector
    }
}
