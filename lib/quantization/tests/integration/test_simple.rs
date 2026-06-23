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

    fn scalar_quantized_scores_for(
        distance_type: DistanceType,
        invert: bool,
        vector_data: &[Vec<f32>],
        query: &[f32],
    ) -> Vec<f32> {
        let stopped = AtomicBool::new(false);
        let vectors_count = vector_data.len();
        let vector_dim = query.len();

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
            Some(0.99),
            ScalarQuantizationMethod::Int8,
            None,
            &stopped,
        )
        .unwrap();
        let query_u8 = encoded.encode_query(query);

        (0..vectors_count)
            .map(|index| {
                let quantized_vector = encoded.get_quantized_vector(index as u32);
                encoded.score_point_simple(&query_u8, &quantized_vector)
            })
            .collect()
    }

    fn deterministic_affine_vectors() -> (Vec<Vec<f32>>, Vec<f32>) {
        let vectors_count = 129;
        let vector_dim = 8;
        let mut rng = rand::rngs::StdRng::seed_from_u64(1008);
        let vector_data = (0..vectors_count)
            .map(|_| {
                (0..vector_dim)
                    .map(|_| rng.random_range(-100.0..=100.0))
                    .collect()
            })
            .collect();
        let query = (0..vector_dim)
            .map(|_| rng.random_range(-100.0..=100.0))
            .collect();

        (vector_data, query)
    }

    fn map_affine(
        vector_data: &[Vec<f32>],
        query: &[f32],
        scale: f32,
        offset: f32,
    ) -> (Vec<Vec<f32>>, Vec<f32>) {
        let mapped_vectors = vector_data
            .iter()
            .map(|vector| vector.iter().map(|value| value * scale + offset).collect())
            .collect();
        let mapped_query = query.iter().map(|value| value * scale + offset).collect();

        (mapped_vectors, mapped_query)
    }

    fn assert_scores_match_transform(
        base_scores: &[f32],
        transformed_scores: &[f32],
        expected_scale: f32,
        tolerance: f32,
    ) {
        for (index, (&score, &transformed_score)) in
            base_scores.iter().zip(transformed_scores).enumerate()
        {
            let expected = score * expected_scale;
            assert!(
                (expected - transformed_score).abs() < tolerance,
                "score transform drifted at index {index}: expected {expected}, got {transformed_score}"
            );
        }
    }

    #[rstest]
    #[case(DistanceType::L1, false)]
    #[case(DistanceType::L1, true)]
    #[case(DistanceType::L2, false)]
    #[case(DistanceType::L2, true)]
    fn test_scalar_quantized_distance_scores_are_translation_invariant(
        #[case] distance_type: DistanceType,
        #[case] invert: bool,
    ) {
        let (vector_data, query) = deterministic_affine_vectors();
        let (shifted_vector_data, shifted_query) = map_affine(&vector_data, &query, 1.0, 1000.0);

        let base_scores = scalar_quantized_scores_for(distance_type, invert, &vector_data, &query);
        let shifted_scores = scalar_quantized_scores_for(
            distance_type,
            invert,
            &shifted_vector_data,
            &shifted_query,
        );

        assert_scores_match_transform(&base_scores, &shifted_scores, 1.0, 0.25);
    }

    #[rstest]
    #[case(DistanceType::Dot, false, 6.25)]
    #[case(DistanceType::Dot, true, 6.25)]
    #[case(DistanceType::L1, false, 2.5)]
    #[case(DistanceType::L1, true, 2.5)]
    #[case(DistanceType::L2, false, 6.25)]
    #[case(DistanceType::L2, true, 6.25)]
    fn test_scalar_quantized_scores_follow_positive_scaling(
        #[case] distance_type: DistanceType,
        #[case] invert: bool,
        #[case] expected_scale: f32,
    ) {
        let scale = 2.5;
        let (vector_data, query) = deterministic_affine_vectors();
        let (scaled_vector_data, scaled_query) = map_affine(&vector_data, &query, scale, 0.0);

        let base_scores = scalar_quantized_scores_for(distance_type, invert, &vector_data, &query);
        let scaled_scores =
            scalar_quantized_scores_for(distance_type, invert, &scaled_vector_data, &scaled_query);

        assert_scores_match_transform(&base_scores, &scaled_scores, expected_scale, 1.0);
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
