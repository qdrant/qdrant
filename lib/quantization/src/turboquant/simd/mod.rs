//! SIMD query-encoding + dot-product routines, one submodule per bit-width.
//!
//! Every `query{N}bit` submodule exposes two public entry points:
//!
//! * [`Query{N}bitSimd`](query4bit::Query4bitSimd) — a rotation-applied query
//!   precomputed for fast asymmetric scoring (original-query × PQ-vector):
//!   the shared [`query::QuerySimd`] kernels instantiated at the width, which
//!   dispatch to the best SIMD backend available on the host CPU.
//! * [`score_{N}bit_internal`](query4bit::score_4bit_internal) — dot product of
//!   two already-encoded PQ vectors (symmetric scoring), same runtime dispatch.
//!
//! Available SIMD backends:
//!
//! | Path              | x86_64                                | aarch64           |
//! |-------------------|---------------------------------------|-------------------|
//! | asymmetric, 1/2/4/8 | AVX-512 VNNI, AVX2, SSE4.1+SSSE3    | NEON + SDOT, NEON |
//! | symmetric, 1      | AVX-512 VPOPCNTDQ, AVX2, SSE4.1+SSSE3 | NEON              |
//! | symmetric, 2/4    | AVX-512 VNNI, AVX2, SSE4.1+SSSE3      | NEON + SDOT, NEON |
//! | symmetric, 8      | AVX-512 VNNI, AVX2, SSE4.1            | NEON + SDOT, NEON |
//! | 16, both paths    | AVX-512BW, AVX2, SSE4.1               | NEON              |
//!
//! On any other target the scalar reference kernels take over.

pub mod hadamard;
pub mod query;
pub mod query16bit;
pub mod query1bit;
pub mod query2bit;
pub mod query4bit;
pub mod query8bit;

/// Best multiply-accumulate backend the host CPU supports, in preference
/// order AVX-512 VNNI → AVX2 → SSE → NEON + SDOT → NEON → scalar.  Shared by
/// every kernel built on `u8 × i8` / `i8 × i8` products (the asymmetric
/// paths of every width and the symmetric 2- and 4-bit paths); resolve it
/// once with [`SimdBackend::detect`] and dispatch on the value, so a scoring
/// loop doesn't re-run CPU feature detection per vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimdBackend {
    #[cfg(target_arch = "x86_64")]
    Avx512Vnni,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Sse,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    NeonSdot,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
    Scalar,
}

impl SimdBackend {
    pub(crate) fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            // Every wider backend's kernels also enable these, for their 128-bit helpers
            let sse =
                std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3");
            if sse
                && std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                return SimdBackend::Avx512Vnni;
            }
            if sse && std::is_x86_feature_detected!("avx2") {
                return SimdBackend::Avx2;
            }
            if sse {
                return SimdBackend::Sse;
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("dotprod") {
                return SimdBackend::NeonSdot;
            }
            return SimdBackend::Neon;
        }
        #[allow(unreachable_code)]
        SimdBackend::Scalar
    }
}

/// A TQ+ code's signed codebook value `value` scaled by its coordinate's Q15 factor `factor`:
/// `value · factor / 2¹⁵`, rounded half up. That's what `pmulhrsw` on x86_64 and `vqrdmulh` on
/// aarch64 compute, so the scalar and SIMD paths of the folded kernels agree bit for bit. With
/// `|value| ≤ 128` and `0 ≤ factor < 2¹⁵` the result stays an i8.
#[inline(always)]
pub(crate) fn fold_q15(value: i64, factor: i16) -> i64 {
    (value * i64::from(factor) + (1 << 14)) >> 15
}

/// The folded values of TQ+ codes of `BITS` (2 or 4) each, one per position of `out`, as
/// bytes XORed with `flip`: 0 for the values as `i8`, 0x80 for `value + 128` as the 8-bit grid
/// stores them. `value` is a code's signed codebook value. `simd` folds whole groups from the
/// start, with the slices as given here, and returns the positions done.
///
/// # Panics
///
/// If `codes` hold fewer than `out.len()` codes, or `factors` fewer factors.
#[inline]
pub(crate) fn fold_codes<const BITS: usize>(
    codes: &[u8],
    factors: &[i16],
    out: &mut [u8],
    flip: u8,
    value: fn(u8) -> i64,
    simd: impl FnOnce(&[u8], &[i16], &mut [u8], u8) -> usize,
) {
    let per_byte = 8 / BITS;
    assert!(out.len() <= per_byte * codes.len() && out.len() <= factors.len());
    let done = simd(codes, factors, out, flip);
    debug_assert!(done.is_multiple_of(per_byte));
    let mask = (1 << BITS) - 1;
    let rest = out[done..]
        .chunks_mut(per_byte)
        .zip(factors[done..].chunks(per_byte))
        .zip(&codes[done / per_byte..]);
    for ((out, factors), &byte) in rest {
        for (k, (o, &factor)) in out.iter_mut().zip(factors).enumerate() {
            let code = (byte >> (BITS * k)) & mask;
            // The low byte of the value, an i8
            *o = fold_q15(value(code), factor) as u8 ^ flip;
        }
    }
}

/// The longest vectors the folded kernels take: they sum products of at most 128² in i32,
/// which holds 2¹⁶ coordinates (Qdrant's largest dimension) with room to spare.
pub(crate) const FOLDED_MAX_DIM: usize = 1 << 16;

// Re-exports below include the runtime-dispatching entry points used by the
// crate's scoring paths (`Query{N}bitSimd`, `score_{N}bit_internal`) plus
// scalar-reference and arch-specific kernels the benchmarks at
// `benches/turbo_simd.rs` target directly.  Every symbol here is consumed
// either by `turboquant::quantization` inside the crate or by benches/
// outside — narrowing them to `pub(crate)` would break the bench build.
pub use query::QuerySimd;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query1bit::score_1bit_internal_neon;
pub use query1bit::{
    Query1bitSimd, Query1bitWideSimd, score_1bit_internal, score_1bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query1bit::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
};
pub use query2bit::{
    CODEBOOK_SCALE_SQ as CODEBOOK_SCALE_SQ_2BIT, Query2bitSimd, score_2bit_internal,
    score_2bit_internal_folded, score_2bit_internal_folded_scalar, score_2bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query2bit::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query2bit::{score_2bit_internal_neon, score_2bit_internal_neon_sdot};
pub use query4bit::{
    CODEBOOK_SCALE_SQ as CODEBOOK_SCALE_SQ_4BIT, Query4bitSimd, score_4bit_internal,
    score_4bit_internal_folded, score_4bit_internal_folded_scalar, score_4bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};
pub use query8bit::{Query8bitSimd, score_8bit_internal, score_8bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query8bit::{
    score_8bit_internal_avx2, score_8bit_internal_avx512_vnni, score_8bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query8bit::{score_8bit_internal_neon, score_8bit_internal_neon_sdot};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query16bit::score_16bit_internal_neon;
pub use query16bit::{Query16bitSimd, score_16bit_internal, score_16bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query16bit::{
    score_16bit_internal_avx2, score_16bit_internal_avx512, score_16bit_internal_sse,
};

/// Test-only helpers shared by every `query{N}bit` submodule.
///
/// Per-bit-width specifics (`PARITY_DIMS`, `random_inputs`) still live in
/// their own module's `shared` — this holds only the genuinely
/// bit-width-agnostic pieces.
#[cfg(test)]
mod shared {
    use common::bitpacking::BitWriter;
    use rand::RngExt;
    use rand::prelude::StdRng;
    use rand_distr::{Distribution, StandardNormal};

    /// Uniformly random bytes — used directly by 1-bit tests (where bytes *are*
    /// the packed form) and indirectly via [`pack_codes`] for wider widths.
    pub fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.random_range(0..=u8::MAX)).collect()
    }

    /// `len` samples from N(0, 1).
    pub fn sample_normal_vec(rng: &mut StdRng, len: usize) -> Vec<f32> {
        (0..len).map(|_| StandardNormal.sample(rng)).collect()
    }

    /// Map each raw float to the index of its nearest centroid.
    pub fn encode_to_nearest_centroid(centroids: &[f32], raw: &[f32]) -> Vec<u8> {
        raw.iter()
            .map(|&v| {
                centroids
                    .iter()
                    .enumerate()
                    .min_by(|a, b| (a.1 - v).abs().partial_cmp(&(b.1 - v).abs()).unwrap())
                    .map(|(k, _)| k as u8)
                    .unwrap()
            })
            .collect()
    }

    /// Pack `indices` into bytes with `bits` bits per code, LSB-first — same
    /// layout [`crate::turboquant::quantization::TurboQuantizer::pack_vector`]
    /// produces (both go through [`BitWriter`]).  Caller guarantees every
    /// index fits in `bits` bits and `indices.len() * bits` is a multiple of 8.
    pub fn pack_codes(indices: &[u8], bits: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity((indices.len() * bits as usize).div_ceil(8));
        let mut writer = BitWriter::new(&mut out);
        for &idx in indices {
            writer.write(idx, bits);
        }
        writer.finish();
        out
    }
}
