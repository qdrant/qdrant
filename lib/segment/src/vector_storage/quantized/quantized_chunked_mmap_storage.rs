use std::path::PathBuf;

use common::types::PointOffsetType;
use memory::mmap_type::MmapFlusher;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};

pub struct QuantizedChunkedMmapStorage {
    data: ChunkedMmapVectors<u8>,
}

impl QuantizedChunkedMmapStorage {
    pub fn populate(&self) -> OperationResult<()> {
        self.data.populate()
    }
}

impl quantization::EncodedStorage for QuantizedChunkedMmapStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        ChunkedVectorStorage::get(&self.data, index as VectorOffsetType).unwrap_or_default()
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> std::io::Result<()> {
        ChunkedVectorStorage::insert(&mut self.data, id as VectorOffsetType, vector, hw_counter)
            .map_err(std::io::Error::other)
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self) -> usize {
        self.data.len()
    }

    fn flusher(&self) -> MmapFlusher {
        let flusher = self.data.flusher();
        Box::new(move || {
            Ok(flusher().map_err(|e| {
                std::io::Error::other(format!("Failed to flush quantization storage: {e}"))
            })?)
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::files(&self.data)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::immutable_files(&self.data)
    }
}
