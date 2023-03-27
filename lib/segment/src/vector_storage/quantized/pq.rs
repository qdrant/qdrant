use std::path::{Path, PathBuf};

use bitvec::prelude::BitVec;
use quantization::EncodedVectors;

use super::scalar_quantized::{ScalarQuantizedRawScorer, QUANTIZED_DATA_PATH, QUANTIZED_META_PATH};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::RawScorer;

pub struct PQVectors<TStorage: quantization::EncodedStorage + Send + Sync> {
    storage: quantization::EncodedVectorsPQ<TStorage>,
    distance: Distance,
}

impl<TStorage: quantization::EncodedStorage + Send + Sync> PQVectors<TStorage> {
    pub fn new(storage: quantization::EncodedVectorsPQ<TStorage>, distance: Distance) -> Self {
        Self { storage, distance }
    }
}

impl<TStorage> QuantizedVectors for PQVectors<TStorage>
where
    TStorage: quantization::EncodedStorage + Send + Sync,
{
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self
            .distance
            .preprocess_vector(query)
            .unwrap_or_else(|| query.to_vec());
        let query = self.storage.encode_query(&query);
        Box::new(ScalarQuantizedRawScorer {
            query,
            deleted,
            quantized_data: &self.storage,
        })
    }

    fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        self.storage.save(&data_path, &meta_path)?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![QUANTIZED_DATA_PATH.into(), QUANTIZED_META_PATH.into()]
    }
}
