#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8::EncodedVectorsU8;
    use quantization::EncodedVectorsPQ;
    use tempfile::Builder;

    #[test]
    fn empty_data_u8() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 0;
        let vector_dim = 256;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = Default::default();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &vector_parameters,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        encoded
            .save(data_path.as_path(), meta_path.as_path())
            .unwrap();

        EncodedVectorsU8::<Vec<u8>>::load(
            data_path.as_path(),
            meta_path.as_path(),
            &vector_parameters,
        )
        .unwrap();
    }

    #[test]
    fn empty_data_pq() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 0;
        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = Default::default();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &vector_parameters,
            2,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        encoded
            .save(data_path.as_path(), meta_path.as_path())
            .unwrap();

        EncodedVectorsPQ::<Vec<u8>>::load(
            data_path.as_path(),
            meta_path.as_path(),
            &vector_parameters,
        )
        .unwrap();
    }
}
