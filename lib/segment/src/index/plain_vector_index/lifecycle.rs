use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::PlainVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::VectorRef;
use crate::index::VectorIndex;
use crate::vector_storage::{VectorStorage, VectorStorageRead};

impl VectorIndex for PlainVectorIndex {
    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut vector_storage = self.vector_storage.borrow_mut();

        if let Some(vector) = vector {
            vector_storage.insert_vector(id, vector, hw_counter)?;

            let mut quantized_vectors = self.quantized_vectors.borrow_mut();
            if let Some(quantized_vectors) = quantized_vectors.as_mut() {
                quantized_vectors.upsert_vector(id, vector, hw_counter)?;
            }
        } else {
            if id as usize >= vector_storage.total_vector_count() {
                debug_assert!(id as usize == vector_storage.total_vector_count());
                // Vector doesn't exist in the storage
                // Insert default vector to keep the sequence
                let default_vector = vector_storage.default_vector();
                vector_storage.insert_vector(id, VectorRef::from(&default_vector), hw_counter)?;
            }
            vector_storage.delete_vector(id)?;
        }

        Ok(())
    }

    fn update_vector_bytes(
        &mut self,
        id: PointOffsetType,
        bytes: Option<&[u8]>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut vector_storage = self.vector_storage.borrow_mut();

        if let Some(bytes) = bytes {
            vector_storage.insert_vector_bytes(id, bytes, hw_counter)?;

            // The quantized sidecar (present only for a non-self-quantized
            // primary storage) still needs a decoded vector; decoding here is
            // acceptable since such a storage sees no benefit from the raw path.
            let mut quantized_vectors = self.quantized_vectors.borrow_mut();
            if let Some(quantized_vectors) = quantized_vectors.as_mut() {
                let decoded = vector_storage.decode_vector_bytes(bytes)?;
                quantized_vectors.upsert_vector(id, VectorRef::from(&decoded), hw_counter)?;
            }
        } else {
            if id as usize >= vector_storage.total_vector_count() {
                debug_assert!(id as usize == vector_storage.total_vector_count());
                let default_vector = vector_storage.default_vector();
                vector_storage.insert_vector(id, VectorRef::from(&default_vector), hw_counter)?;
            }
            vector_storage.delete_vector(id)?;
        }

        Ok(())
    }
}
