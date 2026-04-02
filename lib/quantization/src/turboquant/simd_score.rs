//! SIMD-optimized score computation.
//!
//! Provides allocation-free alternatives to `score_inner` that operate
//! directly on packed index and sign bytes, avoiding four `Vec<u8>`
//! allocations per call.
//!
//! On x86_64 with AVX2+FMA the hot loop uses gather instructions for
//! centroid lookup, variable shifts for bit-unpacking, and fused
//! multiply-add for accumulation.

use super::centroids::get_centroids;

const FRAC_2_PI_SQRT: f32 = 1.2533141f32;

/// Compute the fused dot-product score directly on packed data.
///
/// Dispatches to AVX2+FMA when available, otherwise falls back to an
/// allocation-free scalar implementation.
#[allow(clippy::too_many_arguments)]
pub fn score_fused(
    padded_dim: usize,
    padded_dim_sqrt: f32,
    mse_bits: u8,
    residual_norm_a: f32,
    packed_indices_a: &[u8],
    qjl_signs_a: &[u8],
    residual_norm_b: f32,
    packed_indices_b: &[u8],
    qjl_signs_b: &[u8],
) -> f32 {
    let centroids = get_centroids(mse_bits);

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe {
                score_fused_avx2(
                    padded_dim,
                    padded_dim_sqrt,
                    mse_bits,
                    centroids,
                    residual_norm_a,
                    packed_indices_a,
                    qjl_signs_a,
                    residual_norm_b,
                    packed_indices_b,
                    qjl_signs_b,
                )
            };
        }
    }

    score_fused_scalar(
        padded_dim,
        padded_dim_sqrt,
        mse_bits,
        centroids,
        residual_norm_a,
        packed_indices_a,
        qjl_signs_a,
        residual_norm_b,
        packed_indices_b,
        qjl_signs_b,
    )
}

// ---------------------------------------------------------------------------
// Allocation-free scalar fallback
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn score_fused_scalar(
    padded_dim: usize,
    scale: f32,
    mse_bits: u8,
    centroids: &[f32],
    residual_norm_a: f32,
    packed_indices_a: &[u8],
    qjl_signs_a: &[u8],
    residual_norm_b: f32,
    packed_indices_b: &[u8],
    qjl_signs_b: &[u8],
) -> f32 {
    let qjl_scale = FRAC_2_PI_SQRT / padded_dim as f32;
    let qjl_a = qjl_scale * residual_norm_a;
    let qjl_b = qjl_scale * residual_norm_b;

    // Pre-scale centroids (at most 16 entries for 4-bit).
    let mut scaled = [0.0f32; 16];
    for (i, &c) in centroids.iter().enumerate() {
        scaled[i] = c * scale;
    }

    let chunks = padded_dim / 8;
    let mut sum = 0.0f32;

    for chunk in 0..chunks {
        let sign_byte_a = qjl_signs_a[chunk];
        let sign_byte_b = qjl_signs_b[chunk];

        let mut idx_a = [0u8; 8];
        let mut idx_b = [0u8; 8];
        decode_8_indices(packed_indices_a, mse_bits, chunk, &mut idx_a);
        decode_8_indices(packed_indices_b, mse_bits, chunk, &mut idx_b);

        for i in 0..8 {
            let ca = scaled[idx_a[i] as usize];
            let cb = scaled[idx_b[i] as usize];
            let sa = ((sign_byte_a >> i) & 1) as f32 * 2.0 - 1.0;
            let sb = ((sign_byte_b >> i) & 1) as f32 * 2.0 - 1.0;
            sum += (ca + qjl_a * sa) * (cb + qjl_b * sb);
        }
    }

    sum
}

/// Decode 8 packed indices at group `chunk` into `out`.
#[inline]
fn decode_8_indices(packed: &[u8], bits: u8, chunk: usize, out: &mut [u8; 8]) {
    match bits {
        3 => {
            let off = chunk * 3;
            let p = packed[off] as u32
                | ((packed[off + 1] as u32) << 8)
                | ((packed[off + 2] as u32) << 16);
            for i in 0..8 {
                out[i] = ((p >> (i * 3)) & 0x07) as u8;
            }
        }
        2 => {
            let off = chunk * 2;
            let b0 = packed[off];
            let b1 = packed[off + 1];
            out[0] = b0 & 0x03;
            out[1] = (b0 >> 2) & 0x03;
            out[2] = (b0 >> 4) & 0x03;
            out[3] = (b0 >> 6) & 0x03;
            out[4] = b1 & 0x03;
            out[5] = (b1 >> 2) & 0x03;
            out[6] = (b1 >> 4) & 0x03;
            out[7] = (b1 >> 6) & 0x03;
        }
        1 => {
            let byte = packed[chunk];
            for i in 0..8 {
                out[i] = (byte >> i) & 1;
            }
        }
        _ => panic!("unsupported mse_bits: {bits}"),
    }
}

// ---------------------------------------------------------------------------
// AVX2 + FMA implementation (x86_64 only)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[allow(clippy::too_many_arguments)]
unsafe fn score_fused_avx2(
    padded_dim: usize,
    scale: f32,
    mse_bits: u8,
    centroids: &[f32],
    residual_norm_a: f32,
    packed_indices_a: &[u8],
    qjl_signs_a: &[u8],
    residual_norm_b: f32,
    packed_indices_b: &[u8],
    qjl_signs_b: &[u8],
) -> f32 {
    let qjl_scale = FRAC_2_PI_SQRT / padded_dim as f32;
    let qjl_a = qjl_scale * residual_norm_a;
    let qjl_b = qjl_scale * residual_norm_b;

    // Pre-scale centroids into a 16-entry aligned array for gather.
    let mut scaled = [0.0f32; 16];
    for (i, &c) in centroids.iter().enumerate() {
        scaled[i] = c * scale;
    }

    // SAFETY: all intrinsics below require AVX2+FMA, guaranteed by
    // #[target_feature] and the runtime check in `score_fused`.
    unsafe {
        let v_qjl_a = _mm256_set1_ps(qjl_a);
        let v_qjl_b = _mm256_set1_ps(qjl_b);
        let v_two = _mm256_set1_ps(2.0);
        let v_neg_one = _mm256_set1_ps(-1.0);

        // Shift vectors for variable-shift unpacking of packed indices.
        let idx_shifts = match mse_bits {
            3 => _mm256_setr_epi32(0, 3, 6, 9, 12, 15, 18, 21),
            2 => _mm256_setr_epi32(0, 2, 4, 6, 8, 10, 12, 14),
            1 => _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7),
            _ => unreachable!(),
        };
        let idx_mask = _mm256_set1_epi32((1i32 << mse_bits) - 1);

        // Shift vector for expanding sign bits.
        let sign_shifts = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);

        // Byte stride per 8-element chunk in the packed index array.
        let idx_bytes_per_chunk: usize = match mse_bits {
            3 => 3,
            2 => 2,
            1 => 1,
            _ => unreachable!(),
        };

        let chunks = padded_dim / 8;
        let mut acc = _mm256_setzero_ps();

        for chunk in 0..chunks {
            // -- Decode 8 centroid indices for a and b via variable shift --
            let raw_a = load_packed_u32(packed_indices_a, mse_bits, chunk, idx_bytes_per_chunk);
            let raw_b = load_packed_u32(packed_indices_b, mse_bits, chunk, idx_bytes_per_chunk);

            let vi_a = _mm256_and_si256(_mm256_srlv_epi32(raw_a, idx_shifts), idx_mask);
            let vi_b = _mm256_and_si256(_mm256_srlv_epi32(raw_b, idx_shifts), idx_mask);

            // Gather pre-scaled centroids.
            let ca = _mm256_i32gather_ps::<4>(scaled.as_ptr(), vi_a);
            let cb = _mm256_i32gather_ps::<4>(scaled.as_ptr(), vi_b);

            // -- Decode 8 sign bits --
            let sa_raw = _mm256_set1_epi32(*qjl_signs_a.get_unchecked(chunk) as i32);
            let sb_raw = _mm256_set1_epi32(*qjl_signs_b.get_unchecked(chunk) as i32);

            let sa_bits =
                _mm256_and_si256(_mm256_srlv_epi32(sa_raw, sign_shifts), _mm256_set1_epi32(1));
            let sb_bits =
                _mm256_and_si256(_mm256_srlv_epi32(sb_raw, sign_shifts), _mm256_set1_epi32(1));

            // 0/1 → -1.0/1.0  :  sign = bits * 2.0 - 1.0
            let sa = _mm256_fmadd_ps(_mm256_cvtepi32_ps(sa_bits), v_two, v_neg_one);
            let sb = _mm256_fmadd_ps(_mm256_cvtepi32_ps(sb_bits), v_two, v_neg_one);

            // (ca + qjl_a * sa) * (cb + qjl_b * sb)
            let va = _mm256_fmadd_ps(v_qjl_a, sa, ca);
            let vb = _mm256_fmadd_ps(v_qjl_b, sb, cb);
            acc = _mm256_fmadd_ps(va, vb, acc);
        }

        hsum_avx2(acc)
    }
}

/// Load the packed bits for one 8-element chunk as a broadcast `__m256i`.
///
/// For 3-bit packing the chunk occupies 3 bytes (24 bits), for 2-bit it
/// occupies 2 bytes (16 bits), and for 1-bit it occupies 1 byte.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn load_packed_u32(
    packed: &[u8],
    mse_bits: u8,
    chunk: usize,
    bytes_per_chunk: usize,
) -> __m256i {
    // SAFETY: caller guarantees `off + bytes_per_chunk <= packed.len()`
    // and that AVX2 is available.
    unsafe {
        let off = chunk * bytes_per_chunk;
        let val: u32 = match mse_bits {
            3 => {
                *packed.get_unchecked(off) as u32
                    | ((*packed.get_unchecked(off + 1) as u32) << 8)
                    | ((*packed.get_unchecked(off + 2) as u32) << 16)
            }
            2 => *packed.get_unchecked(off) as u32 | ((*packed.get_unchecked(off + 1) as u32) << 8),
            1 => *packed.get_unchecked(off) as u32,
            _ => unreachable!(),
        };
        _mm256_set1_epi32(val as i32)
    }
}

/// Horizontal sum of all 8 lanes of a 256-bit float vector.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turboquant::{TurboQuantizer, cosine_preprocess};
    use crate::unpack_indices;

    /// Verify the scalar fused function matches the original unpack-then-loop
    /// approach for all supported bit widths.
    #[test]
    fn fused_scalar_matches_original() {
        // Use the same test helper from lib.rs tests.
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
                    "bits={bits} dim={dim}: expected={expected}, got={got}, rel_err={rel_err}"
                );
            }
        }
    }

    /// The decode_8_indices helper must match unpack_indices element-by-element.
    #[test]
    fn decode_8_matches_unpack() {
        let packed_3bit: Vec<u8> = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56];
        let unpacked = unpack_indices(&packed_3bit, 3, 16);
        for chunk in 0..2 {
            let mut out = [0u8; 8];
            decode_8_indices(&packed_3bit, 3, chunk, &mut out);
            for i in 0..8 {
                assert_eq!(out[i], unpacked[chunk * 8 + i], "3-bit chunk={chunk} i={i}");
            }
        }

        let packed_2bit: Vec<u8> = vec![0xE4, 0x1B, 0x39, 0xC6];
        let unpacked = unpack_indices(&packed_2bit, 2, 16);
        for chunk in 0..2 {
            let mut out = [0u8; 8];
            decode_8_indices(&packed_2bit, 2, chunk, &mut out);
            for i in 0..8 {
                assert_eq!(out[i], unpacked[chunk * 8 + i], "2-bit chunk={chunk} i={i}");
            }
        }

        let packed_1bit: Vec<u8> = vec![0xA5, 0x5A];
        let unpacked = unpack_indices(&packed_1bit, 1, 16);
        for chunk in 0..2 {
            let mut out = [0u8; 8];
            decode_8_indices(&packed_1bit, 1, chunk, &mut out);
            for i in 0..8 {
                assert_eq!(out[i], unpacked[chunk * 8 + i], "1-bit chunk={chunk} i={i}");
            }
        }
    }
}
