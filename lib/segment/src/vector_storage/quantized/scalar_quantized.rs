use std::path::Path;

use bitvec::prelude::BitVec;
use quantization::EncodedVectors;
use serde::{Deserialize, Serialize};

use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{Distance, QuantizationConfig, ScalarQuantizationConfig};
use crate::vector_storage::quantized::quantized_vectors_base::{
    QuantizedRawScorer, QuantizedVectors, QUANTIZED_DATA_PATH, QUANTIZED_META_PATH,
};
use crate::vector_storage::RawScorer;

fn get_vector_parameters(
    distance: Distance,
    dim: usize,
    count: usize,
) -> quantization::VectorParameters {
    quantization::VectorParameters {
        dim,
        count,
        distance_type: match distance {
            Distance::Cosine => quantization::DistanceType::Dot,
            Distance::Euclid => quantization::DistanceType::L2,
            Distance::Dot => quantization::DistanceType::Dot,
        },
        invert: distance == Distance::Euclid,
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ScalarQuantizedVectorsConfig {
    pub quantization_config: ScalarQuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
}

pub struct ScalarQuantizedVectors<TStorage> {
    storage: quantization::EncodedVectorsU8<TStorage>,
    config: QuantizedVectorsConfig,
}

impl<TStorage> ScalarQuantizedVectors<TStorage> {
    pub fn new(
        storage: quantization::EncodedVectorsU8<TStorage>,
        config: QuantizedVectorsConfig,
    ) -> Self {
        Self { storage, config }
    }
}

impl<TStorage> QuantizedVectors for ScalarQuantizedVectors<TStorage>
where
    TStorage: quantization::EncodedStorage + Send + Sync,
{
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self.storage.encode_query(query);
        Box::new(QuantizedRawScorer {
            query,
            deleted,
            quantized_data: self,
        })
    }

    fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        self.storage.save(&data_path, &meta_path)?;
        Ok(())
    }

    fn config(&self) -> QuantizationConfig {
        self.config.clone().into()
    }
}
