use std::arch::x86_64::*;

use common::types::ScoreType;

use crate::data_types::vectors::VectorElementType;

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn dot_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    const STEP: usize = 16;
    unsafe {
        let mut n = v1.len();
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum: __m512 = _mm512_setzero_ps();
        while n >= STEP {
            let a1 = _mm512_loadu_ps(ptr1);
            let a2 = _mm512_loadu_ps(ptr2);
            ptr1 = ptr1.add(STEP);
            ptr2 = ptr2.add(STEP);
            n -= STEP;

            sum = _mm512_fmadd_ps(a1, a2, sum);
        }
        let sum = _mm512_reduce_add_ps(sum);
        dot_similarity_tail(sum, ptr1, ptr2, n)
    }
}

// Handle the tail of the dot product.  This is a separate function, because otherwise the compiler
// generates general-case code with AVX512 and AVX, ignoring the fact that the length is below 16
// (even explicit `assert!(len < 16)` doesn't change it).
//
// Actually, inline(never) is only marginally faster than no attribute macro, but also makes it more
// future-proof.
#[inline(never)]
unsafe fn dot_similarity_tail(acc: f32, ptr1: *const f32, ptr2: *const f32, len: usize) -> f32 {
    unsafe {
        let mut sum = acc;
        for i in 0..len {
            sum += ptr1.add(i).read() * ptr2.add(i).read()
        }
        sum
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
