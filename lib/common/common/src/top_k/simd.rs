use crate::types::ScoreType;

/// Return a mask whose bit `i` is set exactly when `scores[i] > threshold`.
///
/// Requires at most 64 scores. Equal scores and NaNs leave their bits unset.
/// Dispatches to an available SIMD implementation or the scalar fallback.
#[inline]
pub(super) fn threshold_mask(scores: &[ScoreType], threshold: ScoreType) -> u64 {
    debug_assert!(scores.len() <= 64);

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 support was checked above.
            return unsafe { threshold_mask_avx2(scores, threshold) };
        }
        // SAFETY: SSE2 is part of the x86-64 baseline.
        unsafe { threshold_mask_sse2(scores, threshold) }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: NEON support was checked above.
            return unsafe { threshold_mask_neon(scores, threshold) };
        }
        threshold_mask_scalar(scores, threshold)
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    threshold_mask_scalar(scores, threshold)
}

/// Build the strict-comparison mask for at most 64 scores without explicit SIMD.
#[inline]
fn threshold_mask_scalar(scores: &[ScoreType], threshold: ScoreType) -> u64 {
    scores.iter().enumerate().fold(0, |mask, (i, &score)| {
        mask | (u64::from(score > threshold) << i)
    })
}

/// Build the strict-comparison mask for at most 64 scores using AVX2 and a scalar tail.
///
/// # Safety
///
/// The CPU must support AVX2. The slice does not need SIMD alignment.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn threshold_mask_avx2(scores: &[ScoreType], threshold: ScoreType) -> u64 {
    use std::arch::x86_64::*;

    let threshold_simd = _mm256_set1_ps(threshold);
    let mut mask = 0;
    let (chunks, remainder) = scores.as_chunks::<8>();
    for (i, chunk) in chunks.iter().enumerate() {
        // SAFETY: Each chunk contains eight f32 values; unaligned loads are allowed.
        let values = unsafe { _mm256_loadu_ps(chunk.as_ptr()) };
        let above = _mm256_cmp_ps::<_CMP_GT_OQ>(values, threshold_simd);
        mask |= (_mm256_movemask_ps(above) as u64) << (8 * i);
    }
    if !remainder.is_empty() {
        mask |= threshold_mask_scalar(remainder, threshold) << (scores.len() - remainder.len());
    }
    mask
}

/// Build the strict-comparison mask for at most 64 scores using SSE2 and a scalar tail.
///
/// # Safety
///
/// The CPU must support SSE2, as guaranteed by x86-64. No SIMD alignment is required.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn threshold_mask_sse2(scores: &[ScoreType], threshold: ScoreType) -> u64 {
    use std::arch::x86_64::*;

    let threshold_simd = _mm_set1_ps(threshold);
    let mut mask = 0;
    let (chunks, remainder) = scores.as_chunks::<4>();
    for (i, chunk) in chunks.iter().enumerate() {
        // SAFETY: Each chunk contains four f32 values; unaligned loads are allowed.
        let values = unsafe { _mm_loadu_ps(chunk.as_ptr()) };
        mask |= (_mm_movemask_ps(_mm_cmpgt_ps(values, threshold_simd)) as u64) << (4 * i);
    }
    if !remainder.is_empty() {
        mask |= threshold_mask_scalar(remainder, threshold) << (scores.len() - remainder.len());
    }
    mask
}

/// Build the strict-comparison mask for at most 64 scores using NEON and a scalar tail.
///
/// # Safety
///
/// The CPU must support NEON. The slice does not need SIMD alignment.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn threshold_mask_neon(scores: &[ScoreType], threshold: ScoreType) -> u64 {
    use std::arch::aarch64::*;

    let threshold_simd = vdupq_n_f32(threshold);
    let weights = [1, 2, 4, 8];
    // SAFETY: The array contains four u32 values; unaligned loads are allowed.
    let weights = unsafe { vld1q_u32(weights.as_ptr()) };
    let mut mask = 0;
    let (chunks, remainder) = scores.as_chunks::<4>();
    for (i, chunk) in chunks.iter().enumerate() {
        // SAFETY: Each chunk contains four f32 values; unaligned loads are allowed.
        let values = unsafe { vld1q_f32(chunk.as_ptr()) };
        let above = vcgtq_f32(values, threshold_simd);
        mask |= u64::from(vaddvq_u32(vandq_u32(above, weights))) << (4 * i);
    }
    if !remainder.is_empty() {
        mask |= threshold_mask_scalar(remainder, threshold) << (scores.len() - remainder.len());
    }
    mask
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compare available SIMD masks with scalar results for every tail length,
    /// unaligned starting offsets and special floating-point values.
    #[test]
    fn masks_match_scalar() {
        let values = [
            ScoreType::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            ScoreType::INFINITY,
            ScoreType::NAN,
            -ScoreType::NAN,
            ScoreType::from_bits(1),
            -ScoreType::from_bits(1),
        ];
        // Include every SIMD tail length and unaligned starting addresses.
        for offset in 0..8 {
            let scores: Vec<_> = values.into_iter().cycle().take(72).collect();
            for len in 0..=64 {
                let scores = &scores[offset..offset + len];
                for threshold in values {
                    let expected = threshold_mask_scalar(scores, threshold);
                    assert_eq!(threshold_mask(scores, threshold), expected);
                    #[cfg(target_arch = "x86_64")]
                    {
                        // SAFETY: SSE2 is part of the x86-64 baseline.
                        assert_eq!(unsafe { threshold_mask_sse2(scores, threshold) }, expected);
                        if is_x86_feature_detected!("avx2") {
                            // SAFETY: AVX2 support was checked above.
                            assert_eq!(unsafe { threshold_mask_avx2(scores, threshold) }, expected);
                        }
                    }
                }
            }
        }
    }
}
