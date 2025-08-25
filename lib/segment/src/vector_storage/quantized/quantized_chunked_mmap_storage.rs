use std::path::PathBuf;

use common::types::PointOffsetType;
use memory::mmap_type::MmapFlusher;

use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};

impl quantization::EncodedStorage for ChunkedMmapVectors<u8> {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        ChunkedVectorStorage::get(self, index as VectorOffsetType).unwrap_or_default()
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> std::io::Result<()> {
        ChunkedVectorStorage::insert(self, id as VectorOffsetType, vector, hw_counter)
            .map_err(std::io::Error::other)
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self) -> usize {
        self.len()
    }

    fn flusher(&self) -> MmapFlusher {
        let flusher = self.flusher();
        Box::new(move || {
            Ok(flusher().map_err(|e| {
                std::io::Error::other(format!("Failed to flush quantization storage: {e}"))
            })?)
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::immutable_files(self)
    }
}
