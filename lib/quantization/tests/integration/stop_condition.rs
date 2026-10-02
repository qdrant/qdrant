#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use quantization::encoded_storage::TestEncodedStorageBuilder;
    use quantization::encoded_vectors::{DistanceType, VectorParameters};
    use quantization::encoded_vectors_u8::{self, EncodedVectorsU8, ScalarQuantizationMethod};
    use quantization::{EncodedVectorsPQ, EncodingError, encoded_vectors_pq};

    /// Yields `count` copies of `vector` and raises `stopped` after the first 1,000, so the stop
    /// happens while encoding reads the data, independent of machine speed.
    fn stop_while_reading<'a>(
        stopped: &'a AtomicBool,
        count: usize,
        vector: &'a [f32],
    ) -> impl Iterator<Item = &'a [f32]> + Clone + Send + 'a {
        (0..count).map(move |i| {
            if i == 1_000 {
                stopped.store(true, Ordering::Relaxed);
            }
            vector
        })
    }

    #[test]
    fn stop_condition_u8() {
        let stopped = AtomicBool::new(false);
        let vectors_count = 10_000;
        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let zero_vector = vec![0.0; vector_dim];

        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
        assert_eq!(
            EncodedVectorsU8::encode(
                stop_while_reading(&stopped, vectors_count, &zero_vector),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                vectors_count,
                None,
                ScalarQuantizationMethod::Int8,
                None,
                &stopped,
            )
            .err(),
            Some(EncodingError::Stopped)
        );
    }

    #[test]
    fn stop_condition_pq() {
        let stopped = AtomicBool::new(false);
        let vectors_count = 10_000;
        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let zero_vector = vec![0.0; vector_dim];

        let quantized_vector_size =
            encoded_vectors_pq::get_quantized_vector_size(&vector_parameters, 2);
        assert_eq!(
            EncodedVectorsPQ::encode(
                stop_while_reading(&stopped, vectors_count, &zero_vector),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                vectors_count,
                2,
                1,
                None,
                &stopped,
            )
            .err(),
            Some(EncodingError::Stopped)
        );
    }
}
