use std::path::Path;

use quantization::EncodedVectors;

use super::pq::PQVectors;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{Distance, PQQuantizationConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::scalar_quantized::{
    QUANTIZED_DATA_PATH, QUANTIZED_META_PATH,
};

pub fn create_pq_vectors_ram<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    config: &PQQuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
    distance: Distance,
) -> OperationResult<PQVectors<ChunkedVectors<u8>>> {
    let quantized_vector_size =
        quantization::EncodedVectorsPQ::<ChunkedVectors<u8>>::get_quantized_vector_size(
            vector_parameters,
            config.bucket_size,
        );
    let storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
    let quantized_vectors = quantization::EncodedVectorsPQ::encode(
        vectors,
        storage_builder,
        vector_parameters,
        config.bucket_size,
    )
    .map_err(|e| OperationError::service_error(format!("Cannot quantize vector data: {e}")))?;

    Ok(PQVectors::new(quantized_vectors, distance))
}

pub fn load_pq_vectors_ram(
    path: &Path,
    vector_parameters: &quantization::VectorParameters,
    distance: Distance,
) -> OperationResult<PQVectors<ChunkedVectors<u8>>> {
    let data_path = path.join(QUANTIZED_DATA_PATH);
    let meta_path = path.join(QUANTIZED_META_PATH);

    let storage = quantization::EncodedVectorsPQ::<ChunkedVectors<u8>>::load(
        &data_path,
        &meta_path,
        vector_parameters,
    )?;

    Ok(PQVectors::new(storage, distance))
}
