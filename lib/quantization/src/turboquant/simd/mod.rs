//! SIMD query-encoding + dot-product routines, one submodule per bit-width.
//!
//! 4-bit is the only bit-width with a full SIMD path today.  Lower bit-widths
//! (1, 2) will land alongside in their own submodules — see `lloyd_max` for
//! the corresponding codebooks.

pub mod query1bit;
pub mod query2bit;
pub mod query4bit;

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
    /// layout [`crate::turboquant::encoding::TurboQuantizer::pack_vector`]
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

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query1bit::score_1bit_internal_neon;
pub use query1bit::{Query1bitSimd, score_1bit_internal, score_1bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query1bit::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
};
pub use query2bit::{Query2bitSimd, score_2bit_internal, score_2bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query2bit::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query2bit::{score_2bit_internal_neon, score_2bit_internal_neon_sdot};
pub use query4bit::{Query4bitSimd, score_4bit_internal, score_4bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};
