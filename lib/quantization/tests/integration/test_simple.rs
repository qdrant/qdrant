#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_storage::TestEncodedStorageBuilder;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8;
    use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_simple(#[case] method: ScalarQuantizationMethod) {
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
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_query_positive_scaling_keeps_quantized_order(
        #[case] method: ScalarQuantizationMethod,
    ) {
        let vectors_count = 3000;
        let vector_dim = 16;
        let scale = 2.0;

        let mut rng = rand::rngs::StdRng::seed_from_u64(9000 + vector_dim as u64);
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|_| {
                (0..vector_dim)
                    .map(|i| {
                        if i % 17 == 0 {
                            rng.random_range(-20.0..20.0)
                        } else {
                            rng.random_range(-1.0..1.0)
                        }
                    })
                    .collect()
            })
            .collect();
        let query: Vec<f32> = (0..vector_dim)
            .map(|_| rng.random_range(-1.0..1.0))
            .collect();
        let scaled_query: Vec<f32> = query.iter().map(|v| v * scale).collect();

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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

        let query_u8 = encoded.encode_query_scaled(&query);
        let scaled_query_u8 = encoded.encode_query_scaled(&scaled_query);

        let score_all = |encoded_query| {
            let mut scores: Vec<_> = (0..vectors_count)
                .map(|idx| {
                    let quantized_vector = encoded.get_quantized_vector(idx as u32);
                    (
                        idx,
                        encoded.score_point_simple(encoded_query, &quantized_vector),
                    )
                })
                .collect();
            scores.sort_by(|(_, left), (_, right)| right.total_cmp(left));
            scores
        };

        let scores = score_all(&query_u8);
        let scaled_scores = score_all(&scaled_query_u8);

        let ids: Vec<_> = scores.iter().take(10).map(|(idx, _)| *idx).collect();
        let scaled_ids: Vec<_> = scaled_scores
            .iter()
            .take(10)
            .map(|(idx, _)| *idx)
            .collect();

        assert_eq!(ids, scaled_ids);
        for ((idx, score), (scaled_idx, scaled_score)) in scores.iter().zip(&scaled_scores) {
            assert_eq!(idx, scaled_idx);
            let tolerance = (*score).abs() * 1e-5 + 1e-4;
            assert!((*scaled_score - *score * scale).abs() <= tolerance);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l2_simple(#[case] method: ScalarQuantizationMethod) {
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

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L2,
            invert: false,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l1_simple(#[case] method: ScalarQuantizationMethod) {
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

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L1,
            invert: false,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_inverted_simple(#[case] method: ScalarQuantizationMethod) {
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
            invert: true,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = -dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l2_inverted_simple(#[case] method: ScalarQuantizationMethod) {
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

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L2,
            invert: true,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = -l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_l1_inverted_simple(#[case] method: ScalarQuantizationMethod) {
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

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::L1,
            invert: true,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = -l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_internal_simple(#[case] method: ScalarQuantizationMethod) {
        let vectors_count: usize = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_dot_inverted_internal_simple(#[case] method: ScalarQuantizationMethod) {
        let vectors_count: usize = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
            vector_data.push(vector);
        }

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: true,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
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

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8)]
    fn test_u8_large_quantile(#[case] method: ScalarQuantizationMethod) {
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
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            Some(1.0 - f32::EPSILON), // almost 1.0 value, but not 1.0
            method,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let quantized_vector = encoded.get_quantized_vector(index as u32);
            let score = encoded.score_point_simple(&query_u8, &quantized_vector);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[rstest]
    #[case(ScalarQuantizationMethod::Int8, false)]
    #[case(ScalarQuantizationMethod::Int8, true)]
    fn test_sq_u8_encode_internal(#[case] method: ScalarQuantizationMethod, #[case] invert: bool) {
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

        for distance_type in [DistanceType::Dot, DistanceType::L2, DistanceType::L1] {
            let vector_parameters = VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type,
                invert,
            };
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);

            let encoded = EncodedVectorsU8::encode(
                vector_data.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                vectors_count,
                Some(1.0 - f32::EPSILON), // almost 1.0 value, but not 1.0
                method.clone(),
                None,
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
