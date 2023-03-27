use std::path::Path;

use quantization::EncodedVectors;

use super::pq::PQVectors;
use super::scalar_quantized_mmap_storage::{QuantizedMmapStorage, QuantizedMmapStorageBuilder};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{Distance, PQQuantizationConfig};
use crate::vector_storage::quantized::scalar_quantized::{
    QUANTIZED_DATA_PATH, QUANTIZED_META_PATH,
};

pub fn create_pq_vectors_mmap<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    config: &PQQuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
    data_path: &Path,
    distance: Distance,
) -> OperationResult<PQVectors<QuantizedMmapStorage>> {
    let quantized_vector_size =
        quantization::EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
            vector_parameters,
            config.bucket_size,
        );
    let mmap_data_path = data_path.join(QUANTIZED_DATA_PATH);

    let storage_builder = QuantizedMmapStorageBuilder::new(
        mmap_data_path.as_path(),
        vector_parameters.count,
        quantized_vector_size,
    )?;
    let quantized_vectors = quantization::EncodedVectorsPQ::encode(
        vectors,
        storage_builder,
        vector_parameters,
        config.bucket_size,
    )
    .map_err(|e| OperationError::service_error(format!("Cannot quantize vector data: {e}")))?;

    Ok(PQVectors::new(quantized_vectors, distance))
}

pub fn load_pq_vectors_mmap(
    path: &Path,
    vector_parameters: &quantization::VectorParameters,
    distance: Distance,
) -> OperationResult<PQVectors<QuantizedMmapStorage>> {
    let data_path = path.join(QUANTIZED_DATA_PATH);
    let meta_path = path.join(QUANTIZED_META_PATH);

    let storage = quantization::EncodedVectorsPQ::<QuantizedMmapStorage>::load(
        &data_path,
        &meta_path,
        vector_parameters,
    )?;

    Ok(PQVectors::new(storage, distance))
}
