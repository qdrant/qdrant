use std::path::Path;

use quantization::EncodedVectors;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::ScalarQuantizationConfig;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::scalar_quantized::{
    ScalarQuantizedVectors, QUANTIZED_DATA_PATH, QUANTIZED_META_PATH,
};

pub fn create_scalar_quantized_vectors_ram<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    config: &ScalarQuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
) -> OperationResult<ScalarQuantizedVectors<ChunkedVectors<u8>>> {
    let quantized_vector_size =
        quantization::EncodedVectorsU8::<ChunkedVectors<u8>>::get_quantized_vector_size(
            vector_parameters,
        );
    let storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
    let quantized_vectors = quantization::EncodedVectorsU8::encode(
        vectors,
        storage_builder,
        vector_parameters,
        config.quantile,
    )
    .map_err(|e| OperationError::service_error(format!("Cannot quantize vector data: {e}")))?;

    Ok(ScalarQuantizedVectors::new(quantized_vectors))
}

pub fn load_scalar_quantized_vectors_ram(
    path: &Path,
    vector_parameters: &quantization::VectorParameters,
) -> OperationResult<ScalarQuantizedVectors<ChunkedVectors<u8>>> {
    let data_path = path.join(QUANTIZED_DATA_PATH);
    let meta_path = path.join(QUANTIZED_META_PATH);

    let storage = quantization::EncodedVectorsU8::<ChunkedVectors<u8>>::load(
        &data_path,
        &meta_path,
        vector_parameters,
    )?;

    Ok(ScalarQuantizedVectors::new(storage))
}
