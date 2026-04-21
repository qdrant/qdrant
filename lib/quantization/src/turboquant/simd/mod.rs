//! SIMD query-encoding + dot-product routines, one submodule per bit-width.
//!
//! 4-bit is the only bit-width with a full SIMD path today.  Lower bit-widths
//! (1, 2) will land alongside in their own submodules — see `lloyd_max` for
//! the corresponding codebooks.

pub mod query4bit;

pub use query4bit::Query4bitSimd;
