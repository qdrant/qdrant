use std::arch::aarch64::*;

use common::types::ScoreType;

use super::{BLOCK, MAX_PASS, prefetch_block};
use crate::data_types::vectors::VectorElementType;

/// Floats per register.
const LANES: usize = 4;

/// Floats per step of `dot_similarity_neon`: one register for each of its 4 sums.
const STEP: usize = 4 * LANES;

/// Blocked dot products (see the module docs) with a query block in 16 registers.
///
/// Every vector keeps the 4 sums of `dot_similarity_neon`, adds the same products to them in
/// the same order and adds them up the same way, so the scores are bit for bit those of
/// `dot_similarity_neon`.
///
/// # Safety
/// The CPU must support NEON. `query` has at least [`BLOCK`] floats, every vector as many as
/// `query`, `scores` one slot per vector, and there are at most [`MAX_PASS`] vectors.
pub(super) unsafe fn dot_similarity_blocked_neon(
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
        let mut sums = [[vdupq_n_f32(0.0); 4]; MAX_PASS];

        let mut b = 0;
        while b < blocked {
            let mut query_block = [vdupq_n_f32(0.0); BLOCK / LANES];
            for (k, reg) in query_block.iter_mut().enumerate() {
                *reg = vld1q_f32(q.add(b + k * LANES));
            }
            let has_next = b + BLOCK < blocked;
            for (vector, sums) in vectors.iter().zip(&mut sums) {
                let p = vector.as_ptr().add(b);
                if has_next {
                    prefetch_block(p.add(BLOCK));
                }
                for (k, reg) in query_block.iter().enumerate() {
                    let sum = &mut sums[k % 4];
                    *sum = vfmaq_f32(*sum, *reg, vld1q_f32(p.add(k * LANES)));
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
                    *sum = vfmaq_f32(*sum, vld1q_f32(q.add(offset)), vld1q_f32(p.add(offset)));
                }
                i += STEP;
            }
            let mut result = vaddvq_f32(sums[0])
                + vaddvq_f32(sums[1])
                + vaddvq_f32(sums[2])
                + vaddvq_f32(sums[3]);
            while i < n {
                result += *q.add(i) * *p.add(i);
                i += 1;
            }
            *score = result;
        }
    }
}
