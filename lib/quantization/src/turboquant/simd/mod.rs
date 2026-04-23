//! SIMD query-encoding + dot-product routines, one submodule per bit-width.

pub mod query4bit;

pub use query4bit::{Query4bitSimd, score_4bit_internal, score_4bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::score_4bit_internal_neon;
