use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
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

    fn update_vector_raw(
        &mut self,
        id: PointOffsetType,
        vector: Option<&[u8]>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(bytes) = vector else {
            // Removal doesn't touch the vector value; reuse the decoded path.
            return self.update_vector(id, None, hw_counter);
        };

        let mut vector_storage = self.vector_storage.borrow_mut();
        vector_storage.insert_vector_bytes(id, bytes, hw_counter)?;

        let mut quantized_vectors = self.quantized_vectors.borrow_mut();
        if let Some(quantized_vectors) = quantized_vectors.as_mut() {
            // Build-time quantized copies can't ingest storage-native bytes;
            // feed them the decoded vector read back from the storage.
            let vector = vector_storage.get_vector::<Random>(id).to_owned();
            quantized_vectors.upsert_vector(id, VectorRef::from(&vector), hw_counter)?;
        }

        Ok(())
    }
}
