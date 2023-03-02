use super::chunked_vectors::ChunkedVectors;
use super::RawScorer;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::ScalarQuantizationConfig;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::quantized::scalar_quantized::{
    ScalarQuantizedVectors, ScalarQuantizedVectorsConfig,
};

pub fn create_scalar_quantized_vectors_ram<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
    config: &ScalarQuantizationConfig,
    vector_parameters: &quantization::VectorParameters,
) -> OperationResult<Box<dyn QuantizedVectors>> {
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

    let quantized_vectors_config = ScalarQuantizedVectorsConfig {
        quantization_config: config.clone(),
        vector_parameters: vector_parameters.clone(),
    };

    Ok(Box::new(ScalarQuantizedVectors::new(
        quantized_vectors,
        quantized_vectors_config,
    )))
}
