#[cfg(test)]
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_neon(#[case] method: ScalarQuantizationMethod) {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsU8::<TestEncodedStorage>::get_quantized_vector_size(&vector_parameters);
        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            method,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let quantized_vector = encoded.get_quantized_vector(index as u32);
            let score = encoded.score_point_neon(&query_u8, quantized_vector);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l2_neon(#[case] method: ScalarQuantizationMethod) {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L2,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsU8::<TestEncodedStorage>::get_quantized_vector_size(&vector_parameters);
        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            method,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let quantized_vector = encoded.get_quantized_vector(index as u32);
            let score = encoded.score_point_neon(&query_u8, quantized_vector);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l1_neon(#[case] method: ScalarQuantizationMethod) {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L1,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsU8::<TestEncodedStorage>::get_quantized_vector_size(&vector_parameters);
        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            method,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let quantized_vector = encoded.get_quantized_vector(index as u32);
            let score = encoded.score_point_neon(&query_u8, quantized_vector);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }
}
