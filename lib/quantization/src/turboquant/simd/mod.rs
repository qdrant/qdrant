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
//! | Path              | x86_64                                     | aarch64           |
//! |-------------------|--------------------------------------------|-------------------|
//! | asymmetric, 1/2/4 | AVX-512 VNNI, AVX-VNNI, AVX2, SSE4.1+SSSE3 | NEON + SDOT, NEON |
//! | symmetric, 1      | AVX-512 VPOPCNTDQ, AVX2, SSE4.1+SSSE3      | NEON              |
//! | symmetric, 2/4    | AVX-512 VNNI, AVX2, SSE4.1+SSSE3           | NEON + SDOT, NEON |
//!
//! On any other target the scalar reference kernels take over.

pub mod hadamard;
pub mod query;
pub mod query1bit;
pub mod query2bit;
pub mod query4bit;

/// Best multiply-accumulate backend the host CPU supports, in preference
/// order AVX-512 VNNI → AVX-VNNI → AVX2 → SSE → NEON + SDOT → NEON → scalar.
/// Shared by every kernel built on `u8 × i8` / `i8 × i8` products (the
/// asymmetric paths of every width and the symmetric 2- and 4-bit paths);
/// resolve it once with [`SimdBackend::detect`] and dispatch on the value, so
/// a scoring loop doesn't re-run CPU feature detection per vector.
///
/// `AvxVnni` is the VEX-encoded `VPDPBUSD` (`_mm256_dpbusd_avx_epi32`) that
/// CPUs with AVX-VNNI but no usable AVX-512 VNNI expose — client Intel from
/// Alder Lake on. Only the asymmetric [`query::QuerySimd`] kernels have an
/// AVX-VNNI variant; the symmetric 2-/4-bit paths reuse the AVX2 kernel for
/// it (the batched query scoring is where the wider MAC pays off).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimdBackend {
    #[cfg(target_arch = "x86_64")]
    Avx512Vnni,
    #[cfg(target_arch = "x86_64")]
    AvxVnni,
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
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                return SimdBackend::Avx512Vnni;
            }
            if std::is_x86_feature_detected!("avxvnni") {
                return SimdBackend::AvxVnni;
            }
            if std::is_x86_feature_detected!("avx2") {
                return SimdBackend::Avx2;
            }
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
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
    score_2bit_internal_scalar, score_2bit_internal_weighted, score_2bit_internal_weighted_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query2bit::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query2bit::{score_2bit_internal_neon, score_2bit_internal_neon_sdot};
pub use query4bit::{
    CODEBOOK_SCALE_SQ as CODEBOOK_SCALE_SQ_4BIT, Query4bitSimd, score_4bit_internal,
    score_4bit_internal_scalar, score_4bit_internal_weighted, score_4bit_internal_weighted_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};

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
