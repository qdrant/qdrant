#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8::EncodedVectorsU8;
    use rand::{Rng, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    #[test]
    fn test_dot_simple() {
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

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l2_simple() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>()).collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::L2,
                invert: false,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l1_simple() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim)
                .map(|_| rng.random_range(-1.0..=1.0))
                .collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim)
            .map(|_| rng.random_range(-1.0..=1.0))
            .collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::L1,
                invert: false,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_dot_inverted_simple() {
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

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = -dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l2_inverted_simple() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>()).collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::L2,
                invert: true,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = -l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l1_inverted_simple() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim)
                .map(|_| rng.random_range(-1.0..=1.0))
                .collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim)
            .map(|_| rng.random_range(-1.0..=1.0))
            .collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::L1,
                invert: true,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = -l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_dot_internal_simple() {
        let vectors_count: usize = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_dot_inverted_internal_simple() {
        let vectors_count: usize = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            vectors_count,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_u8_large_quantile() {
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

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            vectors_count,
            Some(1.0 - f32::EPSILON), // almost 1.0 value, but not 1.0
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_simple(&query_u8, index as u32);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_sq_u8_encode_internal() {
        let vectors_count = 129;
        let vector_dim = 70;
        let error = 1e-3;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim)
                .map(|_| 2.0 * rng.random::<f32>() - 1.0)
                .collect();
            vector_data.push(vector);
        }

        for distance_type in [DistanceType::Dot, DistanceType::L2] {
            let encoded = EncodedVectorsU8::encode(
                vector_data.iter(),
                Vec::<u8>::new(),
                &VectorParameters {
                    dim: vector_dim,
                    count: None,
                    distance_type,
                    invert: false,
                },
                vectors_count,
                Some(1.0 - f32::EPSILON), // almost 1.0 value, but not 1.0
                &AtomicBool::new(false),
            )
            .unwrap();

            let hw = HardwareCounterCell::new();
            for (i, vector) in vector_data.iter().enumerate() {
                // encode vector using the encode_query method
                let query = encoded.encode_query(vector);
                // encode vector using the encode_internal_vector method
                let query_internal = encoded.encode_internal_vector(i as u32).unwrap();

                let score_query = encoded.score_point(&query, 0, &hw);
                let score_internal_query = encoded.score_point(&query_internal, 0, &hw);
                let score_internal = encoded.score_internal(i as u32, 0, &hw);

                assert!((score_query - score_internal).abs() < error);
                assert!((score_internal_query - score_internal).abs() < error);
                assert!((score_query - score_internal_query).abs() < error);
            }
        }
    }
}
