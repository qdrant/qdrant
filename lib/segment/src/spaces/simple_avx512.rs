use std::arch::x86_64::*;

use common::types::ScoreType;

use crate::data_types::vectors::VectorElementType;

/// Floats consumed per iteration of the main loop: four independent 16-lane
/// accumulators, so consecutive FMAs do not wait on each other.
const BLOCK: usize = 64;

/// Lanes in one 512-bit register.
const LANES: usize = 16;

/// Mask selecting the first `n` lanes, for the final partial register.
#[inline(always)]
fn tail_mask(n: usize) -> __mmask16 {
    debug_assert!(n < LANES);
    ((1u32 << n) - 1) as __mmask16
}

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn euclid_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        debug_assert_eq!(n, v2.len());
        let p1 = v1.as_ptr();
        let p2 = v2.as_ptr();
        let mut acc = [_mm512_setzero_ps(); 4];
        let mut i = 0;
        while i + BLOCK <= n {
            for (k, acc) in acc.iter_mut().enumerate() {
                let off = i + k * LANES;
                let d = _mm512_sub_ps(_mm512_loadu_ps(p1.add(off)), _mm512_loadu_ps(p2.add(off)));
                *acc = _mm512_fmadd_ps(d, d, *acc);
            }
            i += BLOCK;
        }
        while i + LANES <= n {
            let d = _mm512_sub_ps(_mm512_loadu_ps(p1.add(i)), _mm512_loadu_ps(p2.add(i)));
            acc[0] = _mm512_fmadd_ps(d, d, acc[0]);
            i += LANES;
        }
        if i < n {
            let mask = tail_mask(n - i);
            let d = _mm512_sub_ps(
                _mm512_maskz_loadu_ps(mask, p1.add(i)),
                _mm512_maskz_loadu_ps(mask, p2.add(i)),
            );
            acc[1] = _mm512_fmadd_ps(d, d, acc[1]);
        }
        let sum = _mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3]));
        -_mm512_reduce_add_ps(sum)
    }
}

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn manhattan_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        debug_assert_eq!(n, v2.len());
        let p1 = v1.as_ptr();
        let p2 = v2.as_ptr();
        let mut acc = [_mm512_setzero_ps(); 4];
        let mut i = 0;
        while i + BLOCK <= n {
            for (k, acc) in acc.iter_mut().enumerate() {
                let off = i + k * LANES;
                let d = _mm512_sub_ps(_mm512_loadu_ps(p1.add(off)), _mm512_loadu_ps(p2.add(off)));
                *acc = _mm512_add_ps(_mm512_abs_ps(d), *acc);
            }
            i += BLOCK;
        }
        while i + LANES <= n {
            let d = _mm512_sub_ps(_mm512_loadu_ps(p1.add(i)), _mm512_loadu_ps(p2.add(i)));
            acc[0] = _mm512_add_ps(_mm512_abs_ps(d), acc[0]);
            i += LANES;
        }
        if i < n {
            let mask = tail_mask(n - i);
            let d = _mm512_sub_ps(
                _mm512_maskz_loadu_ps(mask, p1.add(i)),
                _mm512_maskz_loadu_ps(mask, p2.add(i)),
            );
            acc[1] = _mm512_add_ps(_mm512_abs_ps(d), acc[1]);
        }
        let sum = _mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3]));
        -_mm512_reduce_add_ps(sum)
    }
}

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn dot_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        debug_assert_eq!(n, v2.len());
        let p1 = v1.as_ptr();
        let p2 = v2.as_ptr();
        let mut acc = [_mm512_setzero_ps(); 4];
        let mut i = 0;
        while i + BLOCK <= n {
            for (k, acc) in acc.iter_mut().enumerate() {
                let off = i + k * LANES;
                *acc = _mm512_fmadd_ps(
                    _mm512_loadu_ps(p1.add(off)),
                    _mm512_loadu_ps(p2.add(off)),
                    *acc,
                );
            }
            i += BLOCK;
        }
        while i + LANES <= n {
            acc[0] = _mm512_fmadd_ps(
                _mm512_loadu_ps(p1.add(i)),
                _mm512_loadu_ps(p2.add(i)),
                acc[0],
            );
            i += LANES;
        }
        if i < n {
            let mask = tail_mask(n - i);
            acc[1] = _mm512_fmadd_ps(
                _mm512_maskz_loadu_ps(mask, p1.add(i)),
                _mm512_maskz_loadu_ps(mask, p2.add(i)),
                acc[1],
            );
        }
        let sum = _mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3]));
        _mm512_reduce_add_ps(sum)
    }
}

#[cfg(test)]
mod tests {
    use rand::RngExt;

    use super::*;
    use crate::spaces::simple::{dot_similarity, euclid_similarity, manhattan_similarity};

    /// Every length class: below one register, the masked tail alone, one
    /// main block, a block plus the 16-lane loop plus a tail, and the widths
    /// of the benchmark corpora.
    const DIMS: [usize; 12] = [1, 7, 15, 16, 17, 33, 64, 100, 128, 513, 1536, 2048];

    fn assert_close(simd: f32, scalar: f32, what: &str, dim: usize) {
        // Summation order differs from the scalar loop, so allow a few ulps of
        // the larger magnitude.
        let tol = 1e-5_f32.max(64.0 * f32::EPSILON * simd.abs().max(scalar.abs()));
        assert!(
            (simd - scalar).abs() <= tol,
            "{what} d={dim}: avx512 {simd} vs scalar {scalar}",
        );
    }

    #[test]
    fn test_spaces_avx512_match_scalar() {
        if !is_x86_feature_detected!("avx512f") {
            println!("avx512 test skipped");
            return;
        }
        let mut rng = rand::rng();
        for dim in DIMS {
            let v1: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let v2: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let euclid = unsafe { euclid_similarity_avx512(&v1, &v2) };
            assert_close(euclid, euclid_similarity(&v1, &v2), "euclid", dim);
            let manhattan = unsafe { manhattan_similarity_avx512(&v1, &v2) };
            assert_close(manhattan, manhattan_similarity(&v1, &v2), "manhattan", dim);
            let dot = unsafe { dot_similarity_avx512(&v1, &v2) };
            assert_close(dot, dot_similarity(&v1, &v2), "dot", dim);
        }
    }

    /// The masked tail must not read past the end: lanes beyond `n` come
    /// from memory the slice does not own, here a value that would poison
    /// every result if it were loaded.
    #[test]
    fn test_spaces_avx512_tail_reads_nothing_past_the_slice() {
        if !is_x86_feature_detected!("avx512f") {
            println!("avx512 test skipped");
            return;
        }
        let backing = [1.0_f32; 16]
            .into_iter()
            .chain([f32::NAN; 16])
            .collect::<Vec<_>>();
        for n in 1..16 {
            let v = &backing[16 - n..16];
            let dot = unsafe { dot_similarity_avx512(v, v) };
            assert_eq!(dot, n as f32, "n={n}");
            let euclid = unsafe { euclid_similarity_avx512(v, v) };
            assert_eq!(euclid, 0.0, "n={n}");
        }
    }
}
