//! End-to-end test of the 2-bit LUT scan path
//! (`QDRANT_TQ_LUT_SCAN`): with the flag set, `EncodedVectorsTQ::score_points`
//! over long runs must take the blocked-shadow LUT kernel and produce scores
//! close to — but not identical with — the production per-vector kernel.

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_storage::TestEncodedStorageBuilder;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ};
    use quantization::turboquant::simd::query2bit_lut;
    use quantization::turboquant::{TQBits, TQMode, TQRotation};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const DIM: usize = 128;
    /// Crosses many 32-vector blocks and ends on a partial one.
    const COUNT: usize = 700;
    /// Max LUT-vs-production score difference for unit-norm vectors: the
    /// 7-bit LUT quantization stays well under this (bench-measured max
    /// ≈ 4e-3 at dim 768; error shrinks with dim).
    const LUT_TOLERANCE: f32 = 0.02;

    fn random_unit_vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
        let vector: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        vector.into_iter().map(|x| x / norm).collect()
    }

    fn build_storage(
        data: &[Vec<f32>],
        mode: TQMode,
    ) -> EncodedVectorsTQ<quantization::encoded_storage::TestEncodedStorage> {
        let vector_parameters = VectorParameters {
            dim: DIM,
            distance_type: DistanceType::Dot,
            invert: false,
            deprecated_count: None,
        };
        let quantized_vector_size =
            encoded_vectors_tq::get_quantized_vector_size(&vector_parameters, TQBits::Bits2, mode);
        EncodedVectorsTQ::encode(
            data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            COUNT,
            TQBits::Bits2,
            mode,
            TQRotation::Padded,
            false,
            1,
            None,
            &AtomicBool::new(false),
        )
        .expect("encoding succeeds")
    }

    /// With the flag on, a contiguous `score_points` call must engage the
    /// LUT path (scores measurably diverge from the per-vector kernel, which
    /// never uses the LUT) while staying within the 7-bit quantization
    /// tolerance. With the flag off, `score_points` must match the
    /// per-vector kernel to rounding noise. Covers Normal and TQ+ modes —
    /// TQ+ exercises the `ec_correction` fold into the LUT bias.
    #[test]
    fn lut_scan_engages_and_matches_production() {
        if !query2bit_lut::is_supported() {
            eprintln!("skipped: no AVX2+FMA");
            return;
        }

        let mut rng = StdRng::seed_from_u64(42);
        let data: Vec<Vec<f32>> = (0..COUNT)
            .map(|_| random_unit_vector(&mut rng, DIM))
            .collect();
        let query = random_unit_vector(&mut rng, DIM);
        let ids: Vec<u32> = (0..COUNT as u32).collect();
        let hw_counter = HardwareCounterCell::new();

        for mode in [TQMode::Normal, TQMode::Plus] {
            let encoded = build_storage(&data, mode);

            // Reference: the per-vector production kernel, which never takes
            // the LUT path regardless of the flag.
            // SAFETY: nextest runs each test in its own process; under plain
            // `cargo test`, no other test in this binary touches this
            // variable.
            unsafe { std::env::set_var("QDRANT_TQ_LUT_SCAN", "1") };
            let flagged_query = encoded.encode_query(&query);
            let production: Vec<f32> = ids
                .iter()
                .map(|&id| encoded.score_point(&flagged_query, id, &hw_counter))
                .collect();

            let mut lut_scores = vec![0.0f32; COUNT];
            encoded.score_points(&flagged_query, &ids, &mut lut_scores, &hw_counter);

            let max_diff = lut_scores
                .iter()
                .zip(&production)
                .map(|(lut, production)| (lut - production).abs())
                .fold(0.0f32, f32::max);
            assert!(
                max_diff > 0.0,
                "mode {mode:?}: LUT path did not engage — batch scores are \
                 bit-identical to the production kernel",
            );
            assert!(
                max_diff <= LUT_TOLERANCE,
                "mode {mode:?}: LUT scores diverge from production by {max_diff}",
            );

            // Flag off: the batch path must agree with the per-vector kernel
            // up to batch-reduction rounding.
            unsafe { std::env::remove_var("QDRANT_TQ_LUT_SCAN") };
            let plain_query = encoded.encode_query(&query);
            let mut plain_scores = vec![0.0f32; COUNT];
            encoded.score_points(&plain_query, &ids, &mut plain_scores, &hw_counter);
            for (v, (plain, reference)) in plain_scores.iter().zip(&production).enumerate() {
                assert!(
                    (plain - reference).abs() <= 1e-4,
                    "mode {mode:?}, vector {v}: flag-off batch score {plain} \
                     vs per-vector {reference}",
                );
            }
        }
    }
}
