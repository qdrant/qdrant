use std::arch::x86_64::*;

use common::types::ScoreType;

use super::{BLOCK, MAX_PASS, prefetch_block};
use crate::data_types::vectors::VectorElementType;
use crate::spaces::simple_avx::four_way_hsum;

/// Floats per register.
const LANES: usize = 16;

/// Floats per step of `dot_similarity_avx`, which this kernel reproduces.
const STEP: usize = 2 * LANES;

/// Blocked dot products (see the module docs) with a query block in 4 registers.
///
/// Every vector keeps the 4 sums of `dot_similarity_avx` (8 floats each) lane by lane in two
/// registers, `[sum 1 | sum 2]` and `[sum 3 | sum 4]`, adds the same products to them in the
/// same order and adds them up the same way, so the scores are bit for bit those of
/// `dot_similarity_avx`.
///
/// # Safety
/// The CPU must support AVX-512F. `query` has at least [`BLOCK`] floats, every vector as many
/// as `query`, `scores` one slot per vector, and there are at most [`MAX_PASS`] vectors.
#[target_feature(enable = "avx512f")]
pub(super) unsafe fn dot_similarity_blocked_avx512(
    query: &[VectorElementType],
    vectors: &[&[VectorElementType]],
    scores: &mut [ScoreType],
) {
    debug_assert!(query.len() >= BLOCK && vectors.len() <= MAX_PASS);
    debug_assert_eq!(vectors.len(), scores.len());

    unsafe {
        let n = query.len();
        let blocked = n - n % BLOCK;
        let q = query.as_ptr();
        let mut sums = [[_mm512_setzero_ps(); 2]; MAX_PASS];

        let mut b = 0;
        while b < blocked {
            let mut query_block = [_mm512_setzero_ps(); BLOCK / LANES];
            for (k, reg) in query_block.iter_mut().enumerate() {
                *reg = _mm512_loadu_ps(q.add(b + k * LANES));
            }
            let has_next = b + BLOCK < blocked;
            for (vector, sums) in vectors.iter().zip(&mut sums) {
                let p = vector.as_ptr().add(b);
                if has_next {
                    prefetch_block(p.add(BLOCK));
                }
                for (k, reg) in query_block.iter().enumerate() {
                    let sum = &mut sums[k % 2];
                    *sum = _mm512_fmadd_ps(*reg, _mm512_loadu_ps(p.add(k * LANES)), *sum);
                }
            }
            b += BLOCK;
        }

        for ((score, vector), sums) in scores.iter_mut().zip(vectors).zip(&mut sums) {
            let p = vector.as_ptr();
            let mut i = blocked;
            while i + STEP <= n {
                for (k, sum) in sums.iter_mut().enumerate() {
                    let offset = i + k * LANES;
                    *sum = _mm512_fmadd_ps(
                        _mm512_loadu_ps(q.add(offset)),
                        _mm512_loadu_ps(p.add(offset)),
                        *sum,
                    );
                }
                i += STEP;
            }
            let [sum12, sum34] = *sums;
            let mut result = four_way_hsum(
                low_half(sum12),
                high_half(sum12),
                low_half(sum34),
                high_half(sum34),
            );
            while i < n {
                result += *q.add(i) * *p.add(i);
                i += 1;
            }
            *score = result;
        }
    }
}

/// Lanes 0 to 7.
#[target_feature(enable = "avx512f")]
fn low_half(x: __m512) -> __m256 {
    _mm512_castps512_ps256(x)
}

/// Lanes 8 to 15.
#[target_feature(enable = "avx512f")]
fn high_half(x: __m512) -> __m256 {
    _mm256_castpd_ps(_mm512_extractf64x4_pd::<1>(_mm512_castps_pd(x)))
}
