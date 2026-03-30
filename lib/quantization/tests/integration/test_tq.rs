#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_tq::{DEFAULT_TURBO_QUANT_LEVELS, EncodedVectorsTQ};
    use rand::{RngExt, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    const VECTORS_COUNT: usize = 513;
    const VECTOR_DIM: usize = 65;
    const DOT_ERROR: f32 = VECTOR_DIM as f32 * 0.05;
    const L2_ERROR: f32 = VECTOR_DIM as f32 * 0.15;
    const INTERNAL_ERROR: f32 = VECTOR_DIM as f32 * 0.15;

    #[test]
    fn test_tq_dot() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < DOT_ERROR);
        }
    }

    #[test]
    fn test_tq_l2() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::L2,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < L2_ERROR);
        }
    }

    #[test]
    #[should_panic(expected = "TurboQuant does not support L1")]
    fn test_tq_l1() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::L1,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < L2_ERROR);
        }
    }

    #[test]
    fn test_tq_dot_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: true,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < DOT_ERROR);
        }
    }

    #[test]
    fn test_tq_l2_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::L2,
            invert: true,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < L2_ERROR);
        }
    }

    #[test]
    #[should_panic(expected = "TurboQuant does not support L1")]
    fn test_tq_l1_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::L1,
            invert: true,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < L2_ERROR);
        }
    }

    #[test]
    fn test_tq_dot_internal() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..VECTORS_COUNT {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < INTERNAL_ERROR);
        }
    }

    #[test]
    fn test_tq_dot_inverted_internal() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.random()).collect());
        }

        let vector_parameters = VectorParameters {
            dim: VECTOR_DIM,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: true,
        };
        let quantized_vector_size =
            EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                DEFAULT_TURBO_QUANT_LEVELS,
            );
        let encoded = EncodedVectorsTQ::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            VECTORS_COUNT,
            DEFAULT_TURBO_QUANT_LEVELS,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..VECTORS_COUNT {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < INTERNAL_ERROR);
        }
    }
}
