#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, VectorParameters};
    use quantization::encoded_vectors_u8;
    use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
    use tempfile::Builder;

    /// The hot-path `score_bytes` asserts (in debug builds) that each encoded vector is at least
    /// as large as the metadata expects. `load` performs a stricter, exact-size check for
    /// storage-derived vectors once at load time, so the invariant also holds in release builds.
    /// This verifies that a storage whose vector stride does not exactly match the metadata is
    /// rejected at load time instead of reaching the hot path and reading out of bounds.
    #[test]
    fn load_requires_storage_vector_size_to_match_metadata() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 4;
        let vector_dim = 256;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|i| vec![i as f32; vector_dim])
            .collect();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
        EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(Some(data_path.as_path()), quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            ScalarQuantizationMethod::Int8,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        let try_load = |stride: usize| {
            EncodedVectorsU8::<TestEncodedStorage>::load(
                TestEncodedStorage::from_file(data_path.as_path(), stride).unwrap(),
                meta_path.as_path(),
            )
        };

        // Loading with the exact stride succeeds.
        try_load(quantized_vector_size).unwrap();

        // Loading with a stride that does not exactly match the metadata must be rejected,
        // whether it is smaller or larger than expected. Both strides still divide the data
        // file, so they pass the storage's own divisibility check and only our load-time
        // consistency check catches the mismatch.
        for stride in [quantized_vector_size / 2, quantized_vector_size * 2] {
            match try_load(stride) {
                Ok(_) => panic!(
                    "loading a storage with vector stride {stride} should fail, \
                     metadata expects {quantized_vector_size}"
                ),
                Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidData),
            }
        }
    }
}
