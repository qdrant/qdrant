#[cfg(target_feature = "neon")]
use common::types::ScoreType;
use half::f16;

#[cfg(target_feature = "neon")]
use crate::data_types::vectors::VectorElementTypeHalf;

#[cfg(target_feature = "neon")]
unsafe extern "C" {
    fn manhattanDist_half_4x4(v1: *const f16, v2: *const f16, n: i32) -> f32;
}

#[allow(clippy::missing_safety_doc)]
#[cfg(target_feature = "neon")]
pub unsafe fn neon_manhattan_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let n = v1.len();
    unsafe { -manhattanDist_half_4x4(v1.as_ptr(), v2.as_ptr(), n.try_into().unwrap()) }
}

#[cfg(test)]
mod tests {
    #[cfg(target_feature = "neon")]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::spaces::metric_f16::simple_manhattan::*;

        if std::arch::is_aarch64_feature_detected!("neon")
            && std::arch::is_aarch64_feature_detected!("fp16")
        {
            let v1_f32: Vec<f32> = vec![
                3.7, 4.3, 5.6, 7.7, 7.6, 4.2, 4.2, 7.3, 4.1, 6., 6.4, 1., 2.4, 7., 2.4, 6.4, 4.8,
                2.4, 2.9, 3.9, 3.9, 7.4, 6.9, 5.3, 6.2, 5.2, 5.2, 4.2, 5.9, 1.8, 4.5, 3.5, 3.1,
                6.1, 6.5, 2.4, 2.1, 7.5, 2.3, 5.9, 3.6, 2.9, 6.1, 5.9, 3.3, 2.9, 3.7, 6.8, 7.2,
                6.5, 3.1, 5.7, 1.1, 7.2, 5.6, 5.1, 7., 2.5, 6.2, 7.6, 7., 6.9, 7.5, 3.2, 5.4, 5.8,
                1.9, 4.9, 7.7, 6.5, 3., 2., 6.9, 6.8, 3.3, 1.4, 4.7, 3.7, 1.9, 3.6, 3.9, 7.2, 7.7,
                7., 6.9, 5.8, 4.4, 1.8, 4.9, 3.1, 7.9, 6.5, 7.5, 3.7, 4.6, 1.5, 3.4, 1.7, 6.4, 7.3,
                4.7, 1.9, 7.7, 8., 4.3, 3.9, 1.5, 6.1, 2.1, 6.9, 2.5, 7.2, 4.1, 4.8, 1., 4.1, 6.3,
                5.9, 6.2, 3.9, 4.1, 1.2, 7.3, 1., 4., 3.1, 6., 5.8, 6.8, 2.6, 5.1, 2.3, 1.2, 5.6,
                3.3, 1.6, 4.7, 7., 4.7, 7.7, 1.5, 4.1, 4.1, 5.8, 7.5, 7.6, 5.2, 2.8, 6.9, 6.1, 4.3,
                5.9, 5.2, 8., 2.1, 1.3, 3.2, 4.3, 5.5, 7.7, 6.8, 2.6, 5.2, 4.1, 4.9, 3.7, 6.2, 1.6,
                4.9, 2.6, 6.9, 2.3, 3.9, 7.7, 6.6, 5.3, 3.1, 5.5, 3., 2.4, 1.9, 6.7, 7.1, 6.3, 7.4,
                6.8, 2.3, 6.1, 3.6, 1.1, 2.8, 7., 3.5, 4.1, 3.4, 7.4, 1.4, 5.5, 6.3, 6.8, 2., 2.1,
                2.7, 7.8, 6., 3.6, 5.9, 3.9, 3.6, 7.8, 5.4, 6.8, 4.6, 7.8, 2.3, 6.2, 7.6, 5.8, 3.3,
                3.2, 6.2, 1.9, 6., 5.3, 3.2, 5.8, 7., 1.6, 1.3, 7.7, 6.1, 1.2, 2.8, 2., 2.2, 2.2,
                5.4, 4.8, 1.8, 3.6, 1.9, 6., 3.3, 3.1, 4.9, 6.2, 2.9, 6.1, 6.6, 3.9, 3.8, 4.8, 6.1,
                6.9, 6.7, 5.9, 6.3, 3.3, 3.2, 5.9,
            ];
            let v2_f32: Vec<f32> = vec![
                1.5, 1.3, 1.7, 6.4, 4.6, 6.2, 1.7, 2.6, 4.3, 6.1, 7.2, 3.7, 1.3, 7.3, 3.6, 5.6,
                5.9, 5.6, 2.3, 3.7, 7.4, 3.6, 7.5, 7.6, 4.8, 5.6, 2.2, 4.3, 4.4, 4.9, 6.1, 2.9,
                5.6, 1.6, 2.4, 7.6, 6., 6.3, 7.3, 1., 3.1, 7., 3.1, 5.5, 2.6, 6.7, 2.2, 1.8, 6.6,
                7.1, 1.6, 3.7, 7.7, 6.3, 2.8, 3., 6.5, 3.3, 3.6, 2.7, 7., 4.2, 7.7, 5.6, 3., 7.4,
                1.6, 4.2, 3.7, 2.7, 3.4, 7., 2.9, 6.6, 8., 5.7, 4.9, 3.8, 4.9, 7.1, 3.9, 4.8, 5.3,
                4.2, 7.2, 6.3, 2.4, 1.5, 3.9, 5.5, 4.1, 6.2, 1., 2.8, 2.7, 6.8, 1.7, 6.7, 1.7, 7.2,
                2.1, 6.3, 5.1, 7.3, 4.7, 1.1, 4.4, 6.4, 4.9, 5.8, 5., 7.6, 6.5, 4., 4., 5.9, 5.3,
                2.1, 3., 7.9, 6.1, 6.1, 5.3, 5.8, 1.4, 3.2, 3.3, 1.2, 1., 6.2, 4.2, 4.5, 3.5, 5.1,
                7., 6., 3.9, 5.5, 6.6, 6.9, 5., 1., 4.8, 4.2, 5.1, 1.1, 1.3, 1.5, 7.9, 7.7, 5.2,
                5.4, 1.4, 1.4, 4.6, 4., 3.2, 2.2, 4.3, 7.1, 3.9, 4.5, 6.1, 5.3, 3.2, 1.4, 6.7, 1.6,
                2.2, 2.8, 4.7, 6.1, 6.2, 6.1, 1.4, 7., 7.4, 7.3, 4.1, 1.5, 3.3, 7.4, 5.3, 7.9, 4.3,
                2.6, 3.6, 4.1, 5.1, 6.4, 5.8, 2.4, 1.8, 4.8, 6.2, 3.5, 5.9, 6.3, 5.1, 4.9, 7.5,
                7.1, 2.4, 1.9, 6.3, 4.2, 7.9, 7.4, 5.6, 4.7, 7.4, 7.9, 3.2, 4.8, 5.7, 5.9, 7.4,
                2.8, 5.2, 6.4, 5.1, 4., 7.2, 3.6, 2., 3.1, 7.5, 3.7, 2.9, 3.4, 6.1, 1., 1.2, 1.3,
                3.8, 2.7, 7.4, 6.6, 5.3, 4.6, 1.8, 3.7, 1.4, 1.1, 1.9, 5.9, 6.5, 4.1, 4.9, 5.7,
                3.9, 4.1, 7.2, 5., 7.3, 2.8, 7.1, 7.2, 4., 2.7,
            ];

            let v1: Vec<f16> = v1_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let v2: Vec<f16> = v2_f32.iter().map(|x| f16::from_f32(*x)).collect();

            let manhattan_simd = unsafe { neon_manhattan_similarity_half(&v1, &v2) };
            let manhattan = manhattan_similarity_half(&v1, &v2);
            assert!((manhattan_simd - manhattan).abs() / manhattan.abs() < 0.0005);
            // Regression test: https://github.com/qdrant/qdrant/issues/10350
            // Manhattan: a lane accumulates 4 iterations x 20000 = 80000, overflows f16 (max 65504). The NEON kernel must accumulate in f32 to stay
            // consistent with the scalar path; an f16 accumulator would saturate
            // to +inf and serialise the score as JSON null.
            let v1_ovf_f32: Vec<f32> = vec![20000.0; 128];
            let v2_ovf_f32: Vec<f32> = vec![0.0; 128];
            let v1_ovf: Vec<f16> = v1_ovf_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let v2_ovf: Vec<f16> = v2_ovf_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let manhattan_simd_ovf = unsafe { neon_manhattan_similarity_half(&v1_ovf, &v2_ovf) };
            let manhattan_ovf = manhattan_similarity_half(&v1_ovf, &v2_ovf);
            assert!(manhattan_simd_ovf.is_finite(), "NEON manhattan.rs must not overflow f16 accumulation");
            assert!((manhattan_simd_ovf - manhattan_ovf).abs() / manhattan_ovf.abs() < 0.0005);

            // Regression: 33-element vector exercises the scalar remainder path
            // (32 elements in the SIMD loop + 1 in the tail). The f16 difference
            // 65504.0 - (-65504.0) = 131008 overflows f16 (max 65504) and would
            // saturate to +inf under f16 remainder arithmetic; the scalar path
            // returns a finite value.
            let mut v1_rem_f32: Vec<f32> = vec![0.0; 32];
            v1_rem_f32.push(65504.0);
            let mut v2_rem_f32: Vec<f32> = vec![0.0; 32];
            v2_rem_f32.push(-65504.0);
            let v1_rem: Vec<f16> = v1_rem_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let v2_rem: Vec<f16> = v2_rem_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let manhattan_simd_rem = unsafe { neon_manhattan_similarity_half(&v1_rem, &v2_rem) };
            let manhattan_rem = manhattan_similarity_half(&v1_rem, &v2_rem);
            assert!(
                manhattan_simd_rem.is_finite(),
                "NEON manhattan.rs remainder path must not overflow f16"
            );
            assert!((manhattan_simd_rem - manhattan_rem).abs() / manhattan_rem.abs() < 0.0005);

        } else {
            println!("neon test skipped");
        }
    }
}
