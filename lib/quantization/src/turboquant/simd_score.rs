//! SIMD-optimized score computation and QJL dequantization.
//!
//! Provides [`score_fused`] for computing the dot product between two quantized
//! vectors using the full pipeline: unpack → QJL dequantize → reconstruct in
//! rotated domain → inverse Hadamard → truncated dot product.
//!
//! Also exposes [`qjl_dequantize`] as a standalone SIMD-accelerated QJL inverse
//! that can replace `Qjl::dequantize`.
//!
//! On x86_64 with AVX2+FMA the hot paths use fused multiply-add for the QJL
//! matrix-vector product, gather instructions for centroid lookup, and SIMD
//! butterfly operations for the Walsh-Hadamard transform.
//!
//! # Benchmarks (AVX2+FMA vs scalar, AMD Ryzen 9950X)
//!
//! | Config           | `score` (scalar) | `score_simd` (AVX2) | Speedup |
//! |------------------|-------------------|----------------------|---------|
//! | 3-bit / dim=128  | 134 µs            | 7.1 µs               | **19×** |
//! | 4-bit / dim=128  | 134 µs            | 6.9 µs               | **19×** |
//! | 4-bit / dim=256  | 136 µs            | 6.9 µs               | **20×** |
//! | 4-bit / dim=512  | 912 µs            | 30 µs                | **30×** |
//! | 4-bit / dim=4096 | 263 ms            | 3.6 ms               | **72×** |
//!
//! The speedup grows with dimension because the QJL matrix-vector multiply
//! (O(d²)) dominates and benefits most from SIMD.

use super::centroids::get_centroids;
use super::packing::unpack_indices;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Hadamard chunk size (must match `hadamard::CHUNK_SIZE`).
const CHUNK_SIZE: usize = 256;

// ===========================================================================
// Public API
// ===========================================================================

/// Compute the dot-product score between two quantized vectors.
///
/// This is the SIMD-optimized equivalent of
/// `dot(dequantize(a)[..dim], dequantize(b)[..dim])`.
///
/// # Parameters
///
/// | Name | Description |
/// |------|-------------|
/// | `dim` | Original vector dimension (output is truncated to this). |
/// | `padded_dim` | Padded dimension (multiple of 256). |
/// | `padded_dim_sqrt` | `1.0 / sqrt(padded_dim)` — centroid scale factor. |
/// | `mse_bits` | Scalar quantization bits (`total_bits − 1`). |
/// | `residual_norm_{a,b}` | L2 norm of the quantization residual. |
/// | `packed_indices_{a,b}` | Packed centroid indices. |
/// | `packed_qjl_signs_{a,b}` | Packed QJL sign bits. |
/// | `qjl_projection_data` | Projection matrix **S**, column-major, `padded_dim × padded_dim` (from `DMatrix::as_slice()`). |
/// | `hadamard_signs` | Random ±1.0 signs, length `padded_dim`. |
/// | `hadamard_scale` | `1.0 / sqrt(256)`. |
#[allow(clippy::too_many_arguments)]
pub fn score_fused(
    dim: usize,
    padded_dim: usize,
    padded_dim_sqrt: f32,
    mse_bits: u8,
    residual_norm_a: f32,
    packed_indices_a: &[u8],
    packed_qjl_signs_a: &[u8],
    residual_norm_b: f32,
    packed_indices_b: &[u8],
    packed_qjl_signs_b: &[u8],
    qjl_projection_data: &[f32],
    hadamard_signs: &[f32],
    hadamard_scale: f32,
) -> f32 {
    let centroids = get_centroids(mse_bits);

    // 1. Unpack centroid indices and QJL sign bits.
    let indices_a = unpack_indices(packed_indices_a, mse_bits, padded_dim);
    let indices_b = unpack_indices(packed_indices_b, mse_bits, padded_dim);
    let signs_a = unpack_indices(packed_qjl_signs_a, 1, padded_dim);
    let signs_b = unpack_indices(packed_qjl_signs_b, 1, padded_dim);

    // 2. QJL dequantize: (sqrt(π/2) / d) · Sᵀ · (2·signs − 1).
    let qjl_a = qjl_dequant_inner(&signs_a, padded_dim, qjl_projection_data);
    let qjl_b = qjl_dequant_inner(&signs_b, padded_dim, qjl_projection_data);

    // 3. Reconstruct rotated-domain vectors: centroid[j]·scale + rn·qjl[j].
    let mut recon_a = vec![0.0f32; padded_dim];
    let mut recon_b = vec![0.0f32; padded_dim];
    reconstruct_rotated(&mut recon_a, &indices_a, centroids, padded_dim_sqrt, residual_norm_a, &qjl_a);
    reconstruct_rotated(&mut recon_b, &indices_b, centroids, padded_dim_sqrt, residual_norm_b, &qjl_b);

    // 4. Inverse Hadamard per CHUNK_SIZE chunk.
    inverse_hadamard(&mut recon_a, hadamard_signs, hadamard_scale);
    inverse_hadamard(&mut recon_b, hadamard_signs, hadamard_scale);

    // 5. Dot product of first `dim` elements.
    dot_f32(&recon_a[..dim], &recon_b[..dim])
}

/// SIMD-optimized QJL dequantization from packed sign bits.
///
/// Computes `(√(π/2) / d) · Sᵀ · (2·signs − 1)`.
///
/// Drop-in replacement for `Qjl::dequantize` that uses AVX2+FMA when available.
pub fn qjl_dequantize(packed_signs: &[u8], d: usize, projection_data: &[f32]) -> Vec<f32> {
    let signs = unpack_indices(packed_signs, 1, d);
    qjl_dequant_inner(&signs, d, projection_data)
}

// ===========================================================================
// QJL matrix-vector multiply: result = scale · Sᵀ · z
//
// S is d×d stored column-major.  Column i of S is contiguous at
// projection_data[i*d..(i+1)*d].
//
// result[i] = scale · dot(S_column_i, z).
//
// The AVX2 path processes 4 output elements per outer iteration so that
// each load of z is reused across 4 independent dot products.
// ===========================================================================

fn qjl_dequant_inner(signs: &[u8], d: usize, projection_data: &[f32]) -> Vec<f32> {
    let scale = (std::f32::consts::PI / 2.0).sqrt() / d as f32;
    let z: Vec<f32> = signs.iter().map(|&s| s as f32 * 2.0 - 1.0).collect();
    let mut result = vec![0.0f32; d];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            unsafe { qjl_matvec_avx2(&z, projection_data, d, scale, &mut result) };
            return result;
        }
    }

    qjl_matvec_scalar(&z, projection_data, d, scale, &mut result);
    result
}

fn qjl_matvec_scalar(z: &[f32], proj: &[f32], d: usize, scale: f32, result: &mut [f32]) {
    for i in 0..d {
        let col = &proj[i * d..(i + 1) * d];
        let mut sum = 0.0f32;
        for k in 0..d {
            sum += col[k] * z[k];
        }
        result[i] = sum * scale;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn qjl_matvec_avx2(
    z: &[f32],
    proj: &[f32],
    d: usize,
    scale: f32,
    result: &mut [f32],
) {
    let chunks8 = d / 8;
    let d4 = d / 4 * 4;

    // 4-output-blocked: each z load is reused for 4 independent dot products.
    for i in (0..d4).step_by(4) {
        let col0 = proj.as_ptr().add(i * d);
        let col1 = proj.as_ptr().add((i + 1) * d);
        let col2 = proj.as_ptr().add((i + 2) * d);
        let col3 = proj.as_ptr().add((i + 3) * d);

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();

        for c in 0..chunks8 {
            let k = c * 8;
            let zv = _mm256_loadu_ps(z.as_ptr().add(k));
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(col0.add(k)), zv, acc0);
            acc1 = _mm256_fmadd_ps(_mm256_loadu_ps(col1.add(k)), zv, acc1);
            acc2 = _mm256_fmadd_ps(_mm256_loadu_ps(col2.add(k)), zv, acc2);
            acc3 = _mm256_fmadd_ps(_mm256_loadu_ps(col3.add(k)), zv, acc3);
        }

        *result.get_unchecked_mut(i) = hsum_avx2(acc0) * scale;
        *result.get_unchecked_mut(i + 1) = hsum_avx2(acc1) * scale;
        *result.get_unchecked_mut(i + 2) = hsum_avx2(acc2) * scale;
        *result.get_unchecked_mut(i + 3) = hsum_avx2(acc3) * scale;
    }

    // Remainder (d not divisible by 4 — shouldn't happen for d = multiple of 256).
    for i in d4..d {
        let col = proj.as_ptr().add(i * d);
        let mut acc = _mm256_setzero_ps();
        for c in 0..chunks8 {
            let k = c * 8;
            acc = _mm256_fmadd_ps(
                _mm256_loadu_ps(col.add(k)),
                _mm256_loadu_ps(z.as_ptr().add(k)),
                acc,
            );
        }
        *result.get_unchecked_mut(i) = hsum_avx2(acc) * scale;
    }
}

// ===========================================================================
// Reconstruct rotated-domain vector:
//   out[j] = centroids[idx[j]] * scale + residual_norm * qjl[j]
// ===========================================================================

fn reconstruct_rotated(
    out: &mut [f32],
    indices: &[u8],
    centroids: &[f32],
    scale: f32,
    residual_norm: f32,
    qjl: &[f32],
) {
    let d = out.len();

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            unsafe { reconstruct_avx2(out, indices, centroids, scale, residual_norm, qjl, d) };
            return;
        }
    }

    for j in 0..d {
        out[j] = centroids[indices[j] as usize] * scale + residual_norm * qjl[j];
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn reconstruct_avx2(
    out: &mut [f32],
    indices: &[u8],
    centroids: &[f32],
    scale: f32,
    residual_norm: f32,
    qjl: &[f32],
    d: usize,
) {
    let mut scaled = [0.0f32; 16];
    for (i, &c) in centroids.iter().enumerate() {
        scaled[i] = c * scale;
    }

    let v_rn = _mm256_set1_ps(residual_norm);

    for c in 0..d / 8 {
        let j = c * 8;
        let idx = _mm256_setr_epi32(
            *indices.get_unchecked(j) as i32,
            *indices.get_unchecked(j + 1) as i32,
            *indices.get_unchecked(j + 2) as i32,
            *indices.get_unchecked(j + 3) as i32,
            *indices.get_unchecked(j + 4) as i32,
            *indices.get_unchecked(j + 5) as i32,
            *indices.get_unchecked(j + 6) as i32,
            *indices.get_unchecked(j + 7) as i32,
        );
        let cent = _mm256_i32gather_ps::<4>(scaled.as_ptr(), idx);
        let q = _mm256_loadu_ps(qjl.as_ptr().add(j));
        _mm256_storeu_ps(out.as_mut_ptr().add(j), _mm256_fmadd_ps(v_rn, q, cent));
    }
}

// ===========================================================================
// Inverse Hadamard transform (per CHUNK_SIZE chunk)
//
// For each 256-element chunk:  FWHT(chunk), then multiply by scale * signs[j].
// ===========================================================================

fn inverse_hadamard(buf: &mut [f32], signs: &[f32], scale: f32) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            for (chunk_idx, chunk) in buf.chunks_exact_mut(CHUNK_SIZE).enumerate() {
                unsafe { fwht_256_avx2(chunk) };
                let sign_off = chunk_idx * CHUNK_SIZE;
                unsafe {
                    let v_scale = _mm256_set1_ps(scale);
                    for j in (0..CHUNK_SIZE).step_by(8) {
                        let v = _mm256_loadu_ps(chunk.as_ptr().add(j));
                        let s = _mm256_loadu_ps(signs.as_ptr().add(sign_off + j));
                        _mm256_storeu_ps(
                            chunk.as_mut_ptr().add(j),
                            _mm256_mul_ps(_mm256_mul_ps(v, s), v_scale),
                        );
                    }
                }
            }
            return;
        }
    }

    for (chunk_idx, chunk) in buf.chunks_exact_mut(CHUNK_SIZE).enumerate() {
        fwht_inplace(chunk);
        let sign_off = chunk_idx * CHUNK_SIZE;
        for j in 0..CHUNK_SIZE {
            chunk[j] *= scale * signs[sign_off + j];
        }
    }
}

// ===========================================================================
// Fast Walsh-Hadamard Transform
// ===========================================================================

fn fwht_inplace(x: &mut [f32]) {
    let n = x.len();
    debug_assert!(n.is_power_of_two());
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let a = x[j];
                let b = x[j + h];
                x[j] = a + b;
                x[j + h] = a - b;
            }
        }
        h *= 2;
    }
}

/// AVX2-optimized FWHT on a 256-element buffer.
///
/// Strides 1, 2, 4 are handled entirely within 256-bit registers using
/// permute/blend.  Strides 8–128 use the standard two-load butterfly.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fwht_256_avx2(x: &mut [f32]) {
    debug_assert!(x.len() >= CHUNK_SIZE);
    let p = x.as_mut_ptr();

    // h = 1: butterfly on adjacent pairs.
    // [a b ...] → [a+b, a−b, ...]
    for i in (0..CHUNK_SIZE).step_by(8) {
        let v = _mm256_loadu_ps(p.add(i));
        // Swap adjacent elements within each 128-bit lane: [1,0,3,2].
        let v_swap = _mm256_permute_ps::<0xB1>(v);
        let add = _mm256_add_ps(v, v_swap);
        let diff = _mm256_sub_ps(v_swap, v);
        _mm256_storeu_ps(p.add(i), _mm256_blend_ps::<0xAA>(add, diff));
    }

    // h = 2: butterfly on groups of 2.
    // [a0 a1 b0 b1 ...] → [a0+b0, a1+b1, a0−b0, a1−b1, ...]
    for i in (0..CHUNK_SIZE).step_by(8) {
        let v = _mm256_loadu_ps(p.add(i));
        // Swap pairs within each 128-bit lane: [2,3,0,1].
        let v_swap = _mm256_permute_ps::<0x4E>(v);
        let add = _mm256_add_ps(v, v_swap);
        let diff = _mm256_sub_ps(v_swap, v);
        _mm256_storeu_ps(p.add(i), _mm256_blend_ps::<0xCC>(add, diff));
    }

    // h = 4: butterfly across 128-bit lane halves.
    for i in (0..CHUNK_SIZE).step_by(8) {
        let v = _mm256_loadu_ps(p.add(i));
        let v_swap = _mm256_permute2f128_ps::<0x01>(v, v);
        let add = _mm256_add_ps(v, v_swap);
        let diff = _mm256_sub_ps(v_swap, v);
        _mm256_storeu_ps(p.add(i), _mm256_blend_ps::<0xF0>(add, diff));
    }

    // h = 8, 16, 32, 64, 128: standard two-register butterfly.
    let mut h = 8;
    while h < CHUNK_SIZE {
        for i in (0..CHUNK_SIZE).step_by(h * 2) {
            for j in (i..i + h).step_by(8) {
                let a = _mm256_loadu_ps(p.add(j));
                let b = _mm256_loadu_ps(p.add(j + h));
                _mm256_storeu_ps(p.add(j), _mm256_add_ps(a, b));
                _mm256_storeu_ps(p.add(j + h), _mm256_sub_ps(a, b));
            }
        }
        h *= 2;
    }
}

// ===========================================================================
// Dot product
// ===========================================================================

fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_avx2(a, b) };
        }
    }

    a.iter().zip(b).map(|(&x, &y)| x * y).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks8 = n / 8;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();

    let pairs = chunks8 / 2;
    for c in 0..pairs {
        let k = c * 16;
        acc0 = _mm256_fmadd_ps(
            _mm256_loadu_ps(a.as_ptr().add(k)),
            _mm256_loadu_ps(b.as_ptr().add(k)),
            acc0,
        );
        acc1 = _mm256_fmadd_ps(
            _mm256_loadu_ps(a.as_ptr().add(k + 8)),
            _mm256_loadu_ps(b.as_ptr().add(k + 8)),
            acc1,
        );
    }
    // Odd trailing 8-wide chunk.
    if chunks8 % 2 != 0 {
        let k = pairs * 16;
        acc0 = _mm256_fmadd_ps(
            _mm256_loadu_ps(a.as_ptr().add(k)),
            _mm256_loadu_ps(b.as_ptr().add(k)),
            acc0,
        );
    }
    let mut sum = hsum_avx2(_mm256_add_ps(acc0, acc1));
    for k in (chunks8 * 8)..n {
        sum += *a.get_unchecked(k) * *b.get_unchecked(k);
    }
    sum
}

// ===========================================================================
// AVX2 helpers
// ===========================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum_avx2(v: __m256) -> f32 {
    let hi128 = _mm256_extractf128_ps(v, 1);
    let lo128 = _mm256_castps256_ps128(v);
    let sum4 = _mm_add_ps(lo128, hi128);
    let hi64 = _mm_movehl_ps(sum4, sum4);
    let sum2 = _mm_add_ps(sum4, hi64);
    let hi32 = _mm_shuffle_ps(sum2, sum2, 0x01);
    let sum1 = _mm_add_ss(sum2, hi32);
    _mm_cvtss_f32(sum1)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packing::pack_indices;
    use crate::{cosine_preprocess, TurboQuantizer};

    #[test]
    fn qjl_dequant_identity_matrix() {
        // With S = I, dequantize should just scale the ±1 sign vector.
        let d = 8;
        let mut proj = vec![0.0f32; d * d];
        for i in 0..d {
            proj[i + i * d] = 1.0; // column-major identity
        }

        let signs: Vec<u8> = vec![1, 0, 1, 1, 0, 0, 1, 0];
        let packed = pack_indices(&signs, 1);
        let result = qjl_dequantize(&packed, d, &proj);

        let scale = (std::f32::consts::PI / 2.0f32).sqrt() / d as f32;
        let expected_z = [1.0f32, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0];
        for i in 0..d {
            assert!(
                (result[i] - expected_z[i] * scale).abs() < 1e-6,
                "i={i}: expected={}, got={}",
                expected_z[i] * scale,
                result[i],
            );
        }
    }

    #[test]
    fn qjl_dequant_matches_nalgebra() {
        // Compare against the reference Qjl::dequantize for a real projection.
        use crate::qjl::Qjl;
        let d = 256;
        let qjl = Qjl::new(d, 42);
        let signs: Vec<u8> = (0..d).map(|i| (i % 3 == 0) as u8).collect();
        let packed = pack_indices(&signs, 1);

        let reference = qjl.dequantize(&signs);
        let simd_result = qjl_dequantize(&packed, d, qjl.projection.as_slice());

        for i in 0..d {
            assert!(
                (reference[i] - simd_result[i]).abs() < 1e-4,
                "i={i}: reference={}, simd={}",
                reference[i],
                simd_result[i],
            );
        }
    }

    #[test]
    fn fwht_scalar_roundtrip() {
        let mut x = [0.0f32; 256];
        for i in 0..256 {
            x[i] = (i as f32).sin();
        }
        let original = x;
        fwht_inplace(&mut x);
        fwht_inplace(&mut x);
        // Two FWHTs = N × original.
        for i in 0..256 {
            assert!(
                (x[i] - original[i] * 256.0).abs() < 1e-1,
                "i={i}: expected={}, got={}",
                original[i] * 256.0,
                x[i],
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn fwht_avx2_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }
        let mut scalar = [0.0f32; 256];
        let mut simd = [0.0f32; 256];
        for i in 0..256 {
            let v = (i as f32 * 0.1).sin();
            scalar[i] = v;
            simd[i] = v;
        }
        fwht_inplace(&mut scalar);
        unsafe { fwht_256_avx2(&mut simd) };
        for i in 0..256 {
            assert!(
                (scalar[i] - simd[i]).abs() < 1e-4,
                "i={i}: scalar={}, simd={}",
                scalar[i],
                simd[i],
            );
        }
    }

    #[test]
    fn dot_product_correct() {
        let a: Vec<f32> = (0..100).map(|i| i as f32 * 0.1).collect();
        let b: Vec<f32> = (0..100).map(|i| (i as f32 * 0.2).sin()).collect();
        let expected: f32 = a.iter().zip(&b).map(|(&x, &y)| x * y).sum();
        let got = dot_f32(&a, &b);
        assert!(
            (got - expected).abs() < 1e-3,
            "expected={expected}, got={got}",
        );
    }

    /// Integration: score_simd must match score for all bit widths and dimensions.
    #[test]
    fn fused_matches_score() {
        fn random_vector(dim: usize, seed: u64) -> Vec<f32> {
            let mut state = seed.wrapping_add(1);
            let raw: Vec<f32> = (0..dim)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state as i64 as f32) / (i64::MAX as f32)
                })
                .collect();
            cosine_preprocess(&raw)
        }

        for bits in 2..=4u8 {
            for dim in [32, 64, 128, 256, 512] {
                let q = TurboQuantizer::new(dim, bits, 42);
                let v1 = random_vector(dim, 1);
                let v2 = random_vector(dim, 2);
                let q1 = q.quantize(&v1);
                let q2 = q.quantize(&v2);

                let expected = q.score(&q1, &q2);
                let got = q.score_simd(&q1, &q2);

                let rel_err = (got - expected).abs() / expected.abs().max(1e-10);
                assert!(
                    rel_err < 1e-4,
                    "bits={bits} dim={dim}: expected={expected}, got={got}, rel_err={rel_err}",
                );
            }
        }
    }
}