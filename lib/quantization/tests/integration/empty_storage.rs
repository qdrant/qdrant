#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::EncodedVectorsPQ;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_binary::{EncodedVectorsBin, QueryEncoding};
    use quantization::encoded_vectors_u8::EncodedVectorsU8;
    use tempfile::Builder;

    #[test]
    fn empty_data_u8() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 0;
        let vector_dim = 256;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = Default::default();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        let _encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(Some(&data_path)),
            &vector_parameters,
            vectors_count,
            None,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        EncodedVectorsU8::<TestEncodedStorage>::load(
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
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = Default::default();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        let _encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(Some(&data_path)),
            &vector_parameters,
            vectors_count,
            2,
            1,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        EncodedVectorsPQ::<TestEncodedStorage>::load(
            data_path.as_path(),
            meta_path.as_path(),
            &vector_parameters,
        )
        .unwrap();
    }

    #[test]
    fn empty_data_bq() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: true,
        };
        let vector_data: Vec<Vec<f32>> = Default::default();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        let _encoded = EncodedVectorsBin::<u8, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(Some(&data_path)),
            &vector_parameters,
            quantization::encoded_vectors_binary::Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        EncodedVectorsBin::<u8, TestEncodedStorage>::load(
            data_path.as_path(),
            meta_path.as_path(),
            &vector_parameters,
        )
        .unwrap();
    }
}
