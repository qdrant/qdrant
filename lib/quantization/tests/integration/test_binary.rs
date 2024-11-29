#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_binary::{BitsStoreType, EncodedVectorsBin};
    use rand::{Rng, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
        let n = f32::signum(rng.gen_range(-1.0..1.0));
        if n == 0.0 {
            1.0
        } else {
            n
        }
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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() <= error);
        }
        counter.discard_results();
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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() <= error);
        }
        counter.discard_results();
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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() <= error);
        }
        counter.discard_results();
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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() <= error);
        }
        counter.discard_results();
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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L1,
                invert: false,
            },
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
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L1,
                invert: true,
            },
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
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L1,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L1,
                invert: true,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L2,
                invert: false,
            },
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
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L2,
                invert: true,
            },
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
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L2,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();
        counter.discard_results();

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

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded = EncodedVectorsBin::<TBitsStoreType, _>::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L2,
                invert: true,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        let mut scores: Vec<_> = vector_data
            .iter()
            .enumerate()
            .map(|(i, _)| (encoded.score_internal(0, i as u32, &counter), i))
            .collect();
        counter.discard_results();

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
}
