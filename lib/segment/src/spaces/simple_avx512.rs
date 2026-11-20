use std::arch::x86_64::*;

use common::types::ScoreType;

use crate::data_types::vectors::VectorElementType;

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn dot_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 64);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum512_1: __m512 = _mm512_setzero_ps();
        let mut sum512_2: __m512 = _mm512_setzero_ps();
        let mut sum512_3: __m512 = _mm512_setzero_ps();
        let mut sum512_4: __m512 = _mm512_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            sum512_1 = _mm512_fmadd_ps(_mm512_loadu_ps(ptr1), _mm512_loadu_ps(ptr2), sum512_1);
            sum512_2 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(16)),
                _mm512_loadu_ps(ptr2.add(16)),
                sum512_2,
            );
            sum512_3 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(32)),
                _mm512_loadu_ps(ptr2.add(32)),
                sum512_3,
            );
            sum512_4 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(48)),
                _mm512_loadu_ps(ptr2.add(48)),
                sum512_4,
            );

            ptr1 = ptr1.add(64);
            ptr2 = ptr2.add(64);
            i += 64;
        }

        let mut result = _mm512_reduce_add_ps(_mm512_add_ps(
            _mm512_add_ps(sum512_1, sum512_2),
            _mm512_add_ps(sum512_3, sum512_4),
        ));

        for j in 0..n - m {
            result += (*ptr1.add(j)) * (*ptr2.add(j));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dot_avx512() {
        use crate::spaces::simple::dot_similarity;

        if !is_x86_feature_detected!("avx512f") {
            println!("avx512 test skipped");
            return;
        }
        // Sizes around the 64-element unroll boundary, including remainders.
        for n in [64, 65, 127, 128, 512, 517] {
            let v1: Vec<f32> = (0..n).map(|i| (i as f32).sin()).collect();
            let v2: Vec<f32> = (0..n).map(|i| (i as f32 * 0.7).cos()).collect();
            let simd = unsafe { super::dot_similarity_avx512(&v1, &v2) };
            let scalar = dot_similarity(&v1, &v2);
            assert!(
                (simd - scalar).abs() <= scalar.abs().max(1.0) * 1e-5,
                "n={n}: {simd} vs {scalar}",
            );
        }
    }
}
