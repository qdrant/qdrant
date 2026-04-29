#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::turboquant::{EncodedVectorsTQ, TQBits, TQMode};
    use rand::{RngExt, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    const VECTORS_COUNT: usize = 513;

    const DIMS: &[usize] = &[16, 64, 65, 128, 384, 512];
    const BITS: &[TQBits] = &[TQBits::Bits4, TQBits::Bits2, TQBits::Bits1_5, TQBits::Bits1];

    /// Absolute tolerance for an approximate score: an empirical per-bit
    /// coefficient (≈ 1.8x observed max across VECTORS_COUNT trials) times
    /// the signal-std of the input data. The mean-error / signal-std ratio
    /// is shared by the Dot, Cosine, and L2 paths, so the same coefficient
    /// table is used; only the caller's `signal_std` differs by metric and
    /// data distribution.
    fn error(bits: TQBits, signal_std: f32) -> f32 {
        let coef = match bits {
            TQBits::Bits1 => 5.1,
            TQBits::Bits1_5 => 4.0,
            TQBits::Bits2 => 3.0,
            TQBits::Bits4 => 0.9,
        };
        coef * signal_std
    }

    /// Signal-std of dot products of two independent U[-1, 1]^d vectors.
    fn dot_signal_std(dim: usize) -> f32 {
        (dim as f32 / 9.0).sqrt()
    }

    /// Signal-std of cosine of two independent unit-norm random vectors in d-dim.
    fn cosine_signal_std(dim: usize) -> f32 {
        1.0 / (dim as f32).sqrt()
    }

    /// Signal-std of the L2-squared score for two independent U[-1, 1]^d
    /// vectors. The score is `‖q‖² + ‖v‖² − 2·<q, v>`; the norms are stored
    /// exactly as extras and contribute no quantization noise, so the
    /// signal variance is dominated by the `−2·<q, v>` term — i.e. twice
    /// the dot-product std.
    fn l2_signal_std(dim: usize) -> f32 {
        2.0 * dot_signal_std(dim)
    }

    /// Tolerance for L1 scoring. Has its own per-bit coefficient table
    /// because the L1 path's noise/signal ratio doesn't track the dot/cosine
    /// pattern: it dequantizes both sides (full inverse rotation + Lloyd-Max
    /// noise per coord) before summing |a − b|. Per-coord errors enter the
    /// sum signed (sign(a_i − b_i)·δ_i), so cancellation makes the total
    /// scale as sqrt(dim). Coefficients are empirically calibrated
    /// (~1.8x observed max across both symmetric and asymmetric paths);
    /// the symmetric path dominates at low bits because both vectors carry
    /// dequantization noise.
    fn error_l1(dim: usize, bits: TQBits) -> f32 {
        let per_sqrt_dim = match bits {
            TQBits::Bits1 => 7.5,
            TQBits::Bits1_5 => 4.5,
            TQBits::Bits2 => 3.0,
            TQBits::Bits4 => 0.7,
        };
        per_sqrt_dim * (dim as f32).sqrt()
    }

    /// Per-bits minimum dim for meaningful absolute-error testing. At low dim
    /// the Hadamard rotation has too few coordinates to Gaussianize well and
    /// the Lloyd-Max quantization error grows too large relative to the signal
    /// for the tolerance formulas above to be useful.
    fn should_test(dim: usize, bits: TQBits) -> bool {
        let min_dim = match bits {
            TQBits::Bits1 => 64,
            TQBits::Bits1_5 => 48,
            TQBits::Bits2 => 32,
            TQBits::Bits4 => 8,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_dot(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, dot_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_cosine(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, cosine_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_dot_internal(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, dot_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_cosine_internal(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, cosine_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_zero_vector_dot(#[case] mode: TQMode) {
        // A zero vector stored in the dataset, scored with Dot against a
        // non-zero query, should produce a score close to the true dot
        // product, which is exactly 0.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, dot_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_zero_query_dot(#[case] mode: TQMode) {
        // A zero query, scored with Dot against any encoded vector, should
        // produce a score close to the true dot product, which is exactly 0.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, dot_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_zero_vector_cosine(#[case] mode: TQMode) {
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
                let error = error(bits, cosine_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_zero_query_cosine(#[case] mode: TQMode) {
        // A zero query, scored with Cosine against any unit-norm vector,
        // should produce a score close to 0. Same convention as above:
        // zero is preserved through query preprocessing.
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, cosine_signal_std(dim));
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
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
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

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_dim_one_dot(#[case] mode: TQMode) {
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
                    mode,
                );
            let encoded = EncodedVectorsTQ::encode(
                vector_data.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                VECTORS_COUNT,
                bits,
                mode,
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

    // L2 (squared) and L1 score tests. Tolerance for L2 reuses the unified
    // `error()` with `l2_signal_std`; L1 has its own coefficient table
    // because its noise/signal scaling per bit doesn't match dot/cosine.

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_l2(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, l2_signal_std(dim));
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }
                let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::L2,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for (index, vector) in vector_data.iter().enumerate() {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    let original_score = l2_similarity(&query, vector);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_l2_internal(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error(bits, l2_signal_std(dim));
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::L2,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();

                let counter = HardwareCounterCell::new();
                for i in 1..VECTORS_COUNT {
                    let score = encoded.score_internal(0, i as u32, &counter);
                    let original_score = l2_similarity(&vector_data[0], &vector_data[i]);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, i={i}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_l1(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_l1(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }
                let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::L1,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();
                let query_u8 = encoded.encode_query(&query);

                let counter = HardwareCounterCell::new();
                for (index, vector) in vector_data.iter().enumerate() {
                    let score = encoded.score_point(&query_u8, index as u32, &counter);
                    let original_score = l1_similarity(&query, vector);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, index={index}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    #[rstest::rstest]
    #[case::normal(TQMode::Normal)]
    #[case::plus(TQMode::Plus)]
    fn test_tq_l1_internal(#[case] mode: TQMode) {
        for &bits in BITS {
            for &dim in DIMS {
                if !should_test(dim, bits) {
                    continue;
                }
                let error = error_l1(dim, bits);
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);
                let mut vector_data: Vec<Vec<f32>> = vec![];
                for _ in 0..VECTORS_COUNT {
                    vector_data.push((0..dim).map(|_| rng.random_range(-1.0..1.0)).collect());
                }

                let vector_parameters = VectorParameters {
                    dim,
                    deprecated_count: None,
                    distance_type: DistanceType::L1,
                    invert: false,
                };
                let quantized_vector_size =
                    EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(
                        &vector_parameters,
                        bits,
                        mode,
                    );
                let encoded = EncodedVectorsTQ::encode(
                    vector_data.iter(),
                    TestEncodedStorageBuilder::new(None, quantized_vector_size),
                    &vector_parameters,
                    VECTORS_COUNT,
                    bits,
                    mode,
                    None,
                    &AtomicBool::new(false),
                )
                .unwrap();

                let counter = HardwareCounterCell::new();
                for i in 1..VECTORS_COUNT {
                    let score = encoded.score_internal(0, i as u32, &counter);
                    let original_score = l1_similarity(&vector_data[0], &vector_data[i]);
                    assert!(
                        (score - original_score).abs() < error,
                        "bits={bits:?}, dim={dim}, i={i}, score={score}, expected={original_score}"
                    );
                }
            }
        }
    }

    /// Recall regression probe — non-uniform per-coordinate variance data
    /// (the case where TQ+ should HELP, not hurt). Asserts Plus-mode recall@k
    /// stays close to Normal-mode on data with a few high-variance "spike"
    /// directions. Catches the failure mode where renorm's `cn` drifts because
    /// `‖X+‖` has chi-squared spread across vectors — see
    /// [`TurboQuantizer::compute_centroid_norm`] for the EC-revert that fixes it.
    #[rstest::rstest]
    #[case::bits1(TQBits::Bits1)]
    #[case::bits2(TQBits::Bits2)]
    #[case::bits4(TQBits::Bits4)]
    fn recall_skewed_data(#[case] bits: TQBits) {
        use rand::prelude::StdRng;
        let dim = 256;
        let n = 1000;
        let n_queries = 50;
        let topk = 10;
        let mut rng = StdRng::seed_from_u64(42);
        // Pre-rotation per-coord scale: a few coords have huge variance, most
        // have small. Realistic embeddings have similar structure (a few
        // "spike" directions).
        let coord_scales: Vec<f32> = (0..dim).map(|i| if i < 8 { 100.0 } else { 1.0 }).collect();
        let make_vec = |rng: &mut StdRng| -> Vec<f32> {
            coord_scales
                .iter()
                .map(|&s| s * (rng.random_range(-1.0..1.0)))
                .collect()
        };
        let vectors: Vec<Vec<f32>> = (0..n).map(|_| make_vec(&mut rng)).collect();
        let queries: Vec<Vec<f32>> = (0..n_queries).map(|_| make_vec(&mut rng)).collect();
        let true_dot =
            |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum() };

        let recall_for = |mode: TQMode| -> f32 {
            let vp = VectorParameters {
                dim,
                distance_type: DistanceType::Dot,
                invert: false,
                deprecated_count: None,
            };
            let qsize =
                EncodedVectorsTQ::<TestEncodedStorage>::get_quantized_vector_size(&vp, bits, mode);
            let encoded = EncodedVectorsTQ::encode(
                vectors.iter(),
                TestEncodedStorageBuilder::new(None, qsize),
                &vp,
                n,
                bits,
                mode,
                None,
                &AtomicBool::new(false),
            )
            .unwrap();
            let counter = HardwareCounterCell::new();
            let mut total = 0.0;
            for q in &queries {
                let mut truth: Vec<(usize, f32)> = vectors
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (i, true_dot(q, v)))
                    .collect();
                truth.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                let truth_top: Vec<usize> = truth.iter().take(topk).map(|x| x.0).collect();

                let qq = encoded.encode_query(q);
                let mut q_scores: Vec<(usize, f32)> = (0..n)
                    .map(|i| (i, encoded.score_point(&qq, i as u32, &counter)))
                    .collect();
                q_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                let q_top: Vec<usize> = q_scores.iter().take(topk).map(|x| x.0).collect();

                let hits = truth_top.iter().filter(|i| q_top.contains(i)).count();
                total += hits as f32 / topk as f32;
            }
            total / n_queries as f32
        };

        let normal = recall_for(TQMode::Normal);
        let plus = recall_for(TQMode::Plus);
        // Plus must not be meaningfully worse than Normal. We allow a small
        // 2% slack since the codebook is the same and EC's value comes from
        // distribution fit, which on this seed is marginal.
        assert!(
            plus >= normal - 0.02,
            "bits={bits:?}: Plus recall regressed (Normal={normal:.3}, Plus={plus:.3})"
        );
    }
}
