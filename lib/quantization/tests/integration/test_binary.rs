#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::EncodingError;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_binary::{
        BitsStoreType, EncodedVectorsBin, Encoding, QueryEncoding,
    };
    use rand::{Rng, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
        let n = f32::signum(rng.random_range(-1.0..1.0));
        if n == 0.0 { 1.0 } else { n }
    }

    fn generate_vector(dim: usize, rng: &mut rand::rngs::StdRng) -> Vec<f32> {
        (0..dim).map(|_| generate_number(rng)).collect()
    }

    #[test]
    fn test_binary_dot() {
        test_binary_dot_impl::<u8>(0);
        test_binary_dot_impl::<u8>(1);
        test_binary_dot_impl::<u8>(8);
        test_binary_dot_impl::<u8>(33);
        test_binary_dot_impl::<u8>(65);
        test_binary_dot_impl::<u8>(3 * 129);
        test_binary_dot_impl::<u128>(1);
        test_binary_dot_impl::<u128>(3 * 129);
    }

    fn test_binary_dot_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_encoded = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_encoded, index as u32, &counter);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() <= error);
        }
    }

    #[test]
    fn test_binary_dot_inverted() {
        test_binary_dot_inverted_impl::<u8>(0);
        test_binary_dot_inverted_impl::<u8>(1);
        test_binary_dot_inverted_impl::<u8>(8);
        test_binary_dot_inverted_impl::<u8>(33);
        test_binary_dot_inverted_impl::<u8>(65);
        test_binary_dot_inverted_impl::<u8>(3 * 129);
        test_binary_dot_inverted_impl::<u128>(1);
        test_binary_dot_inverted_impl::<u128>(3 * 129);
    }

    fn test_binary_dot_inverted_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_encoded = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_encoded, index as u32, &counter);
            let original_score = -dot_similarity(&query, vector);
            assert!((score - original_score).abs() <= error);
        }
    }

    #[test]
    fn test_binary_dot_internal() {
        test_binary_dot_internal_impl::<u8>(0);
        test_binary_dot_internal_impl::<u8>(1);
        test_binary_dot_internal_impl::<u8>(8);
        test_binary_dot_internal_impl::<u8>(33);
        test_binary_dot_internal_impl::<u8>(65);
        test_binary_dot_internal_impl::<u8>(3 * 129);
        test_binary_dot_internal_impl::<u128>(1);
        test_binary_dot_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_dot_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() <= error);
        }
    }

    #[test]
    fn test_binary_dot_inverted_internal() {
        test_binary_dot_inverted_internal_impl::<u8>(0);
        test_binary_dot_inverted_internal_impl::<u8>(1);
        test_binary_dot_inverted_internal_impl::<u8>(8);
        test_binary_dot_inverted_internal_impl::<u8>(33);
        test_binary_dot_inverted_internal_impl::<u8>(65);
        test_binary_dot_inverted_internal_impl::<u8>(3 * 129);
        test_binary_dot_inverted_internal_impl::<u128>(1);
        test_binary_dot_inverted_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_dot_inverted_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() <= error);
        }
    }

    #[test]
    fn test_binary_l1() {
        test_binary_l1_impl::<u8>(0);
        test_binary_l1_impl::<u8>(1);
        test_binary_l1_impl::<u8>(8);
        test_binary_l1_impl::<u8>(33);
        test_binary_l1_impl::<u8>(65);
        test_binary_l1_impl::<u8>(3 * 129);
        test_binary_l1_impl::<u128>(1);
        test_binary_l1_impl::<u128>(3 * 129);
    }

    fn test_binary_l1_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L1,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_b = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_point(&query_b, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l1_similarity(&query, v), i))
            .collect();

        original_scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l1_inverted() {
        test_binary_l1_inverted_impl::<u8>(0);
        test_binary_l1_inverted_impl::<u8>(1);
        test_binary_l1_inverted_impl::<u8>(8);
        test_binary_l1_inverted_impl::<u8>(33);
        test_binary_l1_inverted_impl::<u8>(65);
        test_binary_l1_inverted_impl::<u8>(3 * 129);
        test_binary_l1_inverted_impl::<u128>(1);
        test_binary_l1_inverted_impl::<u128>(3 * 129);
    }

    fn test_binary_l1_inverted_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L1,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_b = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_point(&query_b, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l1_similarity(&query, v), i))
            .collect();

        original_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l1_internal() {
        test_binary_l1_internal_impl::<u8>(0);
        test_binary_l1_internal_impl::<u8>(1);
        test_binary_l1_internal_impl::<u8>(8);
        test_binary_l1_internal_impl::<u8>(33);
        test_binary_l1_internal_impl::<u8>(65);
        test_binary_l1_internal_impl::<u8>(3 * 129);
        test_binary_l1_internal_impl::<u128>(1);
        test_binary_l1_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_l1_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L1,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l1_similarity(&vector_data[0], v), i))
            .collect();

        original_scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l1_inverted_internal() {
        test_binary_l1_inverted_internal_impl::<u8>(0);
        test_binary_l1_inverted_internal_impl::<u8>(1);
        test_binary_l1_inverted_internal_impl::<u8>(8);
        test_binary_l1_inverted_internal_impl::<u8>(33);
        test_binary_l1_inverted_internal_impl::<u8>(65);
        test_binary_l1_inverted_internal_impl::<u8>(3 * 129);
        test_binary_l1_inverted_internal_impl::<u128>(1);
        test_binary_l1_inverted_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_l1_inverted_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L1,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l1_similarity(&vector_data[0], v), i))
            .collect();

        original_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l2() {
        test_binary_l2_impl::<u8>(0);
        test_binary_l2_impl::<u8>(1);
        test_binary_l2_impl::<u8>(8);
        test_binary_l2_impl::<u8>(33);
        test_binary_l2_impl::<u8>(65);
        test_binary_l2_impl::<u8>(3 * 129);
        test_binary_l2_impl::<u128>(1);
        test_binary_l2_impl::<u128>(3 * 129);
    }

    fn test_binary_l2_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L2,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_b = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_point(&query_b, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l2_similarity(&query, v), i))
            .collect();

        original_scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l2_inverted() {
        test_binary_l2_inverted_impl::<u8>(0);
        test_binary_l2_inverted_impl::<u8>(1);
        test_binary_l2_inverted_impl::<u8>(8);
        test_binary_l2_inverted_impl::<u8>(33);
        test_binary_l2_inverted_impl::<u8>(65);
        test_binary_l2_inverted_impl::<u8>(3 * 129);
        test_binary_l2_inverted_impl::<u128>(1);
        test_binary_l2_inverted_impl::<u128>(3 * 129);
    }

    fn test_binary_l2_inverted_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L2,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_b = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_point(&query_b, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l2_similarity(&query, v), i))
            .collect();

        original_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l2_internal() {
        test_binary_l2_internal_impl::<u8>(0);
        test_binary_l2_internal_impl::<u8>(1);
        test_binary_l2_internal_impl::<u8>(8);
        test_binary_l2_internal_impl::<u8>(33);
        test_binary_l2_internal_impl::<u8>(65);
        test_binary_l2_internal_impl::<u8>(3 * 129);
        test_binary_l2_internal_impl::<u128>(1);
        test_binary_l2_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_l2_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L2,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l2_similarity(&vector_data[0], v), i))
            .collect();

        original_scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_binary_l2_inverted_internal() {
        test_binary_l2_inverted_internal_impl::<u8>(0);
        test_binary_l2_inverted_internal_impl::<u8>(1);
        test_binary_l2_inverted_internal_impl::<u8>(8);
        test_binary_l2_inverted_internal_impl::<u8>(33);
        test_binary_l2_inverted_internal_impl::<u8>(65);
        test_binary_l2_inverted_internal_impl::<u8>(3 * 129);
        test_binary_l2_inverted_internal_impl::<u128>(1);
        test_binary_l2_inverted_internal_impl::<u128>(3 * 129);
    }

    fn test_binary_l2_inverted_internal_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 128;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size = EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
            vector_dim,
            Encoding::OneBit,
        );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L2,
                invert: true,
            },
            Encoding::OneBit,
            QueryEncoding::SameAsStorage,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();

        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let sorted_indices: Vec<_> = scores.into_iter().map(|(_, i)| i).collect();

        let mut original_scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (l1_similarity(&vector_data[0], v), i))
            .collect();

        original_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        let sorted_original_indices: Vec<_> = original_scores.into_iter().map(|(_, i)| i).collect();

        assert_eq!(sorted_original_indices, sorted_indices);
    }

    #[test]
    fn test_uncompressed_query_dot_product() {
        test_uncompressed_query_dot_product_impl::<u8>(8);
        test_uncompressed_query_dot_product_impl::<u8>(16);
        test_uncompressed_query_dot_product_impl::<u8>(33);
        test_uncompressed_query_dot_product_impl::<u128>(16);
        test_uncompressed_query_dot_product_impl::<u128>(128);
    }

    fn test_uncompressed_query_dot_product_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize) {
        let vectors_count = 32;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let quantized_vector_size =
            EncodedVectorsBin::<TBitsStoreType, TestEncodedStorage>::get_quantized_vector_size_from_params(
                vector_dim,
                Encoding::OneBit,
            );
        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter().map(|v| v.as_slice()),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::Uncompressed,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let query = generate_vector(vector_dim, &mut rng);
        let encoded_query = encoded.encode_query(&query);

        let hw_counter = HardwareCounterCell::default();

        // Compute expected scores using direct dot product
        let mut expected_scores: Vec<(f32, usize)> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (dot_similarity(&query, v), i))
            .collect();

        // Compute actual scores using uncompressed query encoding
        let mut actual_scores: Vec<(f32, usize)> = (0..vectors_count)
            .map(|i| {
                let score = encoded.score_point(&encoded_query, i as u32, &hw_counter);
                (score, i)
            })
            .collect();

        expected_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        actual_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        // Verify scores match (within error tolerance)
        for ((expected_score, expected_idx), (actual_score, actual_idx)) in
            expected_scores.iter().zip(actual_scores.iter())
        {
            assert_eq!(expected_idx, actual_idx, "Ranking order should match");
            assert!(
                (expected_score - actual_score).abs() < error,
                "Score mismatch: expected {}, got {} for vector {}",
                expected_score,
                actual_score,
                expected_idx
            );
        }
    }

    #[test]
    fn test_uncompressed_query_l1_l2_distance_returns_error() {
        let vector_dim = 16;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let vector_data = vec![generate_vector(vector_dim, &mut rng)];

        let quantized_vector_size =
            EncodedVectorsBin::<u8, TestEncodedStorage>::get_quantized_vector_size_from_params(
                vector_dim,
                Encoding::OneBit,
            );
        let result = EncodedVectorsBin::<u8, _>::encode(
            vector_data.iter().map(|v| v.as_slice()),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L1,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::Uncompressed,
            None,
            &AtomicBool::new(false),
        );

        assert!(result.is_err());
        match result {
            Err(EncodingError::ArgumentsError(msg)) => {
                assert!(msg.contains(
                    "Uncompressed query encoding is only supported for dot product distance"
                ));
                assert!(msg.contains("L1"));
            }
            _ => panic!("Expected ArgumentsError"),
        }

        // Expect error for L2 distance
        let result = EncodedVectorsBin::<u8, _>::encode(
            vector_data.iter().map(|v| v.as_slice()),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type: DistanceType::L2,
                invert: false,
            },
            Encoding::OneBit,
            QueryEncoding::Uncompressed,
            None,
            &AtomicBool::new(false),
        );

        assert!(result.is_err());
        match result {
            Err(EncodingError::ArgumentsError(msg)) => {
                assert!(msg.contains(
                    "Uncompressed query encoding is only supported for dot product distance"
                ));
                assert!(msg.contains("L2"));
            }
            _ => panic!("Expected ArgumentsError"),
        }
    }
}
