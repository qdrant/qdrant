use std::path::Path;

use bitvec::prelude::BitVec;
use quantization::EncodedVectors;

use crate::common::file_operations::atomic_save_json;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{
    Distance, PointOffsetType, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig,
    ScoreType,
};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::scalar_quantized_mmap_storage::QuantizedMmapStorage;
use crate::vector_storage::{quantized_vector_storage, RawScorer, ScoredPointOffset};

pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";
pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";

pub trait QuantizedVectors: Send + Sync {
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a>;

    fn save_to(&self, path: &Path) -> OperationResult<()>;

    fn config(&self) -> &QuantizationConfig;
}

pub struct QuantizedRawScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub query: TEncodedQuery,
    pub deleted: &'a BitVec,
    pub quantized_data: &'a TEncodedVectors,
}

impl<TEncodedQuery, TEncodedVectors> RawScorer
    for QuantizedRawScorer<'_, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if self.deleted[point_id as usize] {
                continue;
            }
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: self.quantized_data.score_point(&self.query, point_id),
            };
            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn check_point(&self, point: PointOffsetType) -> bool {
        (point as usize) < self.deleted.len() && !self.deleted[point as usize]
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.quantized_data.score_point(&self.query, point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data.score_internal(point_a, point_b)
    }
}

fn check_use_ram_quantization_storage(
    config: &ScalarQuantizationConfig,
    on_disk_vector_storage: bool,
) -> bool {
    !on_disk_vector_storage || config.always_ram == Some(true)
}

#[allow(clippy::too_many_arguments)]
pub fn create_quantized_vectors<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    count: usize,
    path: &Path,
    on_disk_vector_storage: bool,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let vector_parameters = quantized_vector_storage::get_vector_parameters(distance, dim, count);
    let quantized_vectors = match quantization_config {
        QuantizationConfig::Scalar(ScalarQuantization {
            scalar: scalar_u8_config,
        }) => {
            let is_ram = quantized_vector_storage::check_use_ram_quantization_storage(
                scalar_u8_config,
                on_disk_vector_storage,
            );
            if is_ram {
                quantized_vector_storage::create_scalar_quantized_vectors_ram(
                    vectors,
                    scalar_u8_config,
                    &vector_parameters,
                )?
            } else {
                quantized_vector_storage::create_scalar_quantized_vectors_mmap(
                    vectors,
                    scalar_u8_config,
                    &vector_parameters,
                    path,
                )?
            }
        }
    };

    quantized_vectors.save_to(path)?;
    atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), quantization_config)?;
    Ok(quantized_vectors)
}

pub fn load_quantized_vectors(
    distance: Distance,
    dim: usize,
    count: usize,
    data_path: &Path,
    on_disk_vector_storage: bool,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let vector_parameters = quantized_vector_storage::get_vector_parameters(distance, dim, count);
    match quantization_config {
        QuantizationConfig::Scalar(ScalarQuantization {
            scalar: scalar_u8_config,
        }) => {
            let is_ram = quantized_vector_storage::check_use_ram_quantization_storage(
                scalar_u8_config,
                on_disk_vector_storage,
            );
            if is_ram {
                Ok(Box::new(quantization::EncodedVectorsU8::<
                    ChunkedVectors<u8>,
                >::load(
                    data_path, meta_path, &vector_parameters
                )?))
            } else {
                Ok(Box::new(quantization::EncodedVectorsU8::<
                    QuantizedMmapStorage,
                >::load(
                    data_path, meta_path, &vector_parameters
                )?))
            }
        }
    }
}
