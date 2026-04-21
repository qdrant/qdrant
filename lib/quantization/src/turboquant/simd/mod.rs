//! SIMD query-encoding + dot-product routines, one submodule per bit-width.
//!
//! 4-bit is the only bit-width with a full SIMD path today.  Lower bit-widths
//! (1, 2) will land alongside in their own submodules — see `lloyd_max` for
//! the corresponding codebooks.

pub mod query1bit;
pub mod query4bit;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query1bit::score_1bit_internal_neon;
pub use query1bit::{Query1bitSimd, score_1bit_internal, score_1bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query1bit::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
};
pub use query4bit::{Query4bitSimd, score_4bit_internal, score_4bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};
