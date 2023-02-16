use std::path::Path;

use bitvec::vec::BitVec;

use super::chunked_vectors::ChunkedVectors;
use super::quantized_mmap_storage::{QuantizedMmapStorageBuilder, QuantizedMmapStorage};
use super::{RawScorer, ScoredPointOffset};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationResult, OperationError};
use crate::types::{PointOffsetType, ScoreType, QuantizationConfig, Distance};
use quantization::EncodedVectors;

pub trait QuantizedVectors: Send + Sync {
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a>;

    fn save_to_file(
        &self,
        meta_path: &Path,
        data_path: &Path,
    ) -> OperationResult<()>;
}

pub struct QuantizedRawScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub query: TEncodedQuery,
    pub deleted: &'a BitVec,
    pub quantized_data: &'a TEncodedVectors,
}

impl<TStorage> QuantizedVectors for quantization::EncodedVectorsU8<TStorage>
where
    TStorage: quantization::EncodedStorage + Send + Sync,
{
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self.encode_query(query);
        Box::new(QuantizedRawScorer {
            query,
            deleted,
            quantized_data: self,
        })
    }

    fn save_to_file(
        &self,
        meta_path: &Path,
        data_path: &Path,
    ) -> OperationResult<()> {
        self.save(data_path, meta_path)?;
        Ok(())
    }
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

#[allow(clippy::too_many_arguments)]
pub fn create_quantized_vectors<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    count: usize,
    meta_path: &Path,
    data_path: &Path,
    on_disk_vector_storage: bool,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let vector_parameters = get_vector_parameters(distance, dim, count);
    let is_ram = use_ram_quantization_storage(quantization_config, on_disk_vector_storage);
    let quantized_vectors = if is_ram {
        create_quantized_vectors_ram(
            vectors,
            quantization_config,
            &vector_parameters,
        )?
    } else {
        create_quantized_vectors_mmap(
            vectors,
            quantization_config,
            &vector_parameters,
            data_path,
        )?
    };
    quantized_vectors.save_to_file(meta_path, data_path)?;
    Ok(quantized_vectors)
}

fn use_ram_quantization_storage(
    quantization_config: &QuantizationConfig,
    on_disk_vector_storage: bool,
) -> bool {
    !on_disk_vector_storage || quantization_config.always_ram == Some(true)
}

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

fn create_quantized_vectors_ram<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    quantization_config: &QuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let quantized_vector_size =
            quantization::EncodedVectorsU8::<ChunkedVectors<u8>>::get_quantized_vector_size(
                vector_parameters,
            );
    let storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
    Ok(Box::new(
        quantization::EncodedVectorsU8::encode(
            vectors,
            storage_builder,
            vector_parameters,
            quantization_config.quantile,
        )
        .map_err(|e| {
            OperationError::service_error(format!("Cannot quantize vector data: {e}"))
        })?,
    ))
}

fn create_quantized_vectors_mmap<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    quantization_config: &QuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
    data_path: &Path,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let quantized_vector_size =
            quantization::EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
            );
    let storage_builder = QuantizedMmapStorageBuilder::new(
        data_path,
        vector_parameters.count,
        quantized_vector_size,
    )?;
    Ok(Box::new(
        quantization::EncodedVectorsU8::encode(
            vectors,
            storage_builder,
            vector_parameters,
            quantization_config.quantile,
        )
        .map_err(|e| {
            OperationError::service_error(format!("Cannot quantize vector data: {e}"))
        })?,
    ))
}

pub fn load_quantized_vectors(
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    count: usize,
    meta_path: &Path,
    data_path: &Path,
    on_disk_vector_storage: bool,
) -> OperationResult<Box<dyn QuantizedVectors>> {
    let vector_parameters = get_vector_parameters(distance, dim, count);
    let is_ram = use_ram_quantization_storage(quantization_config, on_disk_vector_storage);
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
