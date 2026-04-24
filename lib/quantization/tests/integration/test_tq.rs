#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::turboquant::{EncodedVectorsTQ, TQBits, TQMode};
    use rand::{RngExt, SeedableRng};

    use crate::metrics::dot_similarity;
    // TODO(turbo): uncomment when L1/L2 metrics are implemented in TQ.
    // use crate::metrics::{l1_similarity, l2_similarity};

    const VECTORS_COUNT: usize = 513;

    const DIMS: &[usize] = &[16, 64, 65, 128, 384, 512, 768, 1536];
    /// Bit widths exercised by each test. `Bits1_5` is excluded: its
    /// `bit_size` is not yet implemented and panics.
    const BITS: &[TQBits] = &[TQBits::Bits4, TQBits::Bits2, TQBits::Bits1];
    const MODE: TQMode = TQMode::Normal;

    /// Tolerance for Dot-product scoring. For centered U[-1, 1] inputs the dot
    /// product magnitude scales as sqrt(dim) (std of the true score plus the
    /// Lloyd-Max quantization error both scale the same way), so the tolerance
    /// follows suit. Coefficients are empirically calibrated (~1.8x observed
    /// max across VECTORS_COUNT trials on both symmetric and asymmetric paths).
    fn error_dot(dim: usize, bits: TQBits) -> f32 {
        let per_sqrt_dim = match bits {
            TQBits::Bits4 => 0.3,
            TQBits::Bits2 => 1.0,
            TQBits::Bits1 => 1.7,
            TQBits::Bits1_5 => unreachable!("Bits1_5 is not implemented"),
        };
        per_sqrt_dim * (dim as f32).sqrt()
    }

    /// Tolerance for Cosine scoring. Unit-norm inputs mean cos(θ) ∈ [-1, 1] and
    /// the quantization error on the score shrinks as 1/sqrt(dim). Coefficients
    /// are empirically calibrated (~1.8x observed max).
    fn error_cosine(dim: usize, bits: TQBits) -> f32 {
        let per_inv_sqrt_dim = match bits {
            TQBits::Bits4 => 1.0,
            TQBits::Bits2 => 3.0,
            TQBits::Bits1 => 5.0,
            TQBits::Bits1_5 => unreachable!("Bits1_5 is not implemented"),
        };
        let error = per_inv_sqrt_dim / (dim as f32).sqrt();
        assert!(error < 1.0);
        error
    }

    /// Per-bits minimum dim for meaningful absolute-error testing. At low dim
    /// the Hadamard rotation has too few coordinates to Gaussianize well and
    /// the Lloyd-Max quantization error grows too large relative to the signal
    /// for the tolerance formulas above to be useful.
    fn should_test(dim: usize, bits: TQBits) -> bool {
        let min_dim = match bits {
            TQBits::Bits4 => 8,
            TQBits::Bits2 => 32,
            TQBits::Bits1 => 64,
            TQBits::Bits1_5 => unreachable!("Bits1_5 is not implemented"),
        };
        dim >= min_dim
    }

    fn l2_norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    fn normalize(v: &[f32]) -> Vec<f32> {
        let n = l2_norm(v);
        v.iter().map(|x| x / n).collect()
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        dot_similarity(a, b) / (l2_norm(a) * l2_norm(b))
    }

    #[test]
    fn test_tq_dot() {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_dot(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }
                let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Dot,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for (index, vector) in vector_data.iter().enumerate() {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    let original_score = dot_similarity(&query, vector);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_cosine() {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_cosine(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                    vector_data.push(normalize(&v));
                }
                let raw_query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let query = normalize(&raw_query);

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Cosine,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for (index, vector) in vector_data.iter().enumerate() {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    let original_score = cosine_similarity(&query, vector);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_dot_internal() {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_dot(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Dot,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();

                let counter = HardwareCounterCell::new();
                for i in 1..VECTORS_COUNT {
                    let score = encoded.score_internal(0, i as u32, &counter);
                    let original_score = dot_similarity(&vector_data[0], &vector_data[i]);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, i={i}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_cosine_internal() {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_cosine(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                    vector_data.push(normalize(&v));
                }

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Cosine,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();

                let counter = HardwareCounterCell::new();
                for i in 1..VECTORS_COUNT {
                    let score = encoded.score_internal(0, i as u32, &counter);
                    let original_score = cosine_similarity(&vector_data[0], &vector_data[i]);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, i={i}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_zero_vector_dot() {
        // A zero vector stored in the dataset, scored with Dot against a
        // non-zero query, should produce a score close to the true dot
        // product, which is exactly 0.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_dot(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                // Place the zero vector at index 0; fill the rest with random
                // data so the encoded batch resembles a realistic input.
                let mut vector_data: Vec<Vec<f32>> = vec![vec![0.0f32; dim]];
                for _ in 1..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }
                let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Dot,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                let score = encoded.score_point(&query_u8, 0u32, &counter);
                assert!(
                    score.abs() < error,
                    "bits={bits:?}, dim={dim}, score={score} (expected ~0)"
                );
            }
        }
    }

    #[test]
    fn test_tq_zero_query_dot() {
        // A zero query, scored with Dot against any encoded vector, should
        // produce a score close to the true dot product, which is exactly 0.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_dot(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }
                let query: Vec<f32> = vec![0.0f32; dim];

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Dot,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for index in 0..VECTORS_COUNT {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    assert!(
                        score.abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score} (expected ~0)"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_zero_vector_cosine() {
        // A zero vector stored in the dataset, scored with Cosine against a
        // unit-norm query, should produce a score close to 0. Cosine of a
        // zero vector is mathematically undefined; the implementation's
        // convention (preserve zero through preprocessing) yields a true
        // dot of zero post-rotation, so the encoded score should be ~0.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_cosine(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                // Index 0 is the zero vector (not routed through `normalize`,
                // which would divide by zero); the rest are unit-norm random.
                let mut vector_data: Vec<Vec<f32>> = vec![vec![0.0f32; dim]];
                for _ in 1..VECTORS_COUNT {
                    let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                    vector_data.push(normalize(&v));
                }
                let raw_query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let query = normalize(&raw_query);

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Cosine,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                let score = encoded.score_point(&query_u8, 0u32, &counter);
                assert!(
                    score.abs() < error,
                    "bits={bits:?}, dim={dim}, score={score} (expected ~0)"
                );
            }
        }
    }

    #[test]
    fn test_tq_zero_query_cosine() {
        // A zero query, scored with Cosine against any unit-norm vector,
        // should produce a score close to 0. Same convention as above:
        // zero is preserved through query preprocessing.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_cosine(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                    vector_data.push(normalize(&v));
                }
                let query: Vec<f32> = vec![0.0f32; dim];

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::Cosine,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        MODE,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    MODE,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for index in 0..VECTORS_COUNT {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    assert!(
                        score.abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score} (expected ~0)"
                    );
                }
            }
        }
    }

    #[test]
    fn test_tq_dim_one_dot() {
        // dim=1 is degenerate (no room for Hadamard Gaussianization, quant
        // noise dominates). Just verify the path doesn't panic and yields
        // finite, sanely bounded scores; accuracy bounds elsewhere don't apply.
        let dim = 1;
        for &bits in BITS {
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let mut vector_data: Vec<Vec<f32>> = vec![];
            for _ in 0..VECTORS_COUNT {
                vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
            }
            let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

            let vector_parameters = VectorParameters {
                dim,
                deprecated_count: None,
                distance_type: DistanceType::Dot,
                invert: false,
            };
            let quantized_vector_size =
                EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                    &vector_parameters,
                    bits,
                    MODE,
                );
            let encoded = EncodedVectorsTQ::encode(
                vector_data.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                VECTORS_COUNT,
                bits,
                MODE,
                None,
                &AtomicBool::new(false),
            )
            .unwrap();
            let query_u8 = encoded.encode_query(&query);

            let counter = HardwareCounterCell::new();
            for index in 0..VECTORS_COUNT {
                let score = encoded.score_point(&query_u8, index as u32, &counter);
                assert!(
                    score.is_finite() && score.abs() < 10.0,
                    "bits={bits:?}, index={index}, score={score}"
                );
            }
        }
    }

    // The L1 and L2 tests below are disabled because TQ does not yet implement
    // those metrics (encoding panics with `unimplemented!()`). They are kept
    // here so they can be enabled once support lands — uncomment the matching
    // `use crate::metrics::{l1_similarity, l2_similarity};` above as well.
    // The `error_dot` tolerance is a placeholder; L1/L2 error scales differently
    // and will need its own calibrated formula.

    // #[test]
    // fn test_tq_l2() {
    //     for &bits in BITS {
    //         for &dim in DIMS {
    //             if !should_test(dim, bits) {
    //                 continue;
    //             }
    //             let error = error_dot(dim, bits);
    //             let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    //             let mut vector_data: Vec<Vec<f32>> = vec![];
    //             for _ in 0..VECTORS_COUNT {
    //                 vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
    //             }
    //             let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
    //
    //             let vector_parameters = VectorParameters {
    //                 dim,
    //                 deprecated_count: None,
    //                 distance_type: DistanceType::L2,
    //                 invert: false,
    //             };
    //             let quantized_vector_size =
    //                 EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
    //                     &vector_parameters,
    //                     bits,
    //                     MODE,
    //                 );
    //             let encoded = EncodedVectorsTQ::encode(
    //                 vector_data.iter(),
    //                 TestEncodedStorageBuilder::new(None, quantized_vector_size),
    //                 &vector_parameters,
    //                 VECTORS_COUNT,
    //                 bits,
    //                 MODE,
    //                 None,
    //                 &AtomicBool::new(false),
    //             )
    //             .unwrap();
    //             let query_u8 = encoded.encode_query(&query);
    //
    //             let counter = HardwareCounterCell::new();
    //             for (index, vector) in vector_data.iter().enumerate() {
    //                 let score = encoded.score_point(&query_u8, index as u32, &counter);
    //                 let original_score = l2_similarity(&query, vector);
    //                 assert!(
    //                     (score - original_score).abs() < error,
    //                     "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
    //                 );
    //             }
    //         }
    //     }
    // }

    // #[test]
    // fn test_tq_l1() {
    //     for &bits in BITS {
    //         for &dim in DIMS {
    //             if !should_test(dim, bits) {
    //                 continue;
    //             }
    //             let error = error_dot(dim, bits);
    //             let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    //             let mut vector_data: Vec<Vec<f32>> = vec![];
    //             for _ in 0..VECTORS_COUNT {
    //                 vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
    //             }
    //             let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
    //
    //             let vector_parameters = VectorParameters {
    //                 dim,
    //                 deprecated_count: None,
    //                 distance_type: DistanceType::L1,
    //                 invert: false,
    //             };
    //             let quantized_vector_size =
    //                 EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
    //                     &vector_parameters,
    //                     bits,
    //                     MODE,
    //                 );
    //             let encoded = EncodedVectorsTQ::encode(
    //                 vector_data.iter(),
    //                 TestEncodedStorageBuilder::new(None, quantized_vector_size),
    //                 &vector_parameters,
    //                 VECTORS_COUNT,
    //                 bits,
    //                 MODE,
    //                 None,
    //                 &AtomicBool::new(false),
    //             )
    //             .unwrap();
    //             let query_u8 = encoded.encode_query(&query);
    //
    //             let counter = HardwareCounterCell::new();
    //             for (index, vector) in vector_data.iter().enumerate() {
    //                 let score = encoded.score_point(&query_u8, index as u32, &counter);
    //                 let original_score = l1_similarity(&query, vector);
    //                 assert!(
    //                     (score - original_score).abs() < error,
    //                     "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
    //                 );
    //             }
    //         }
    //     }
    // }
}
