//! AVX2 kernels: an FMA tile for f32, byte multiply-adds for u8, and nibble lookups for bits.
//! AVX-512F adds a wider f32 tile.
//!
//! Each entry point is the one `#[target_feature]` function of its kernel. The drivers below
//! it are `#[inline(always)]`, so they compile with the same features.

mod avx512;
mod gemm;
mod lut;

use common::types::PointOffsetType;

use crate::{Bits, Cols, MatrixOperands, TopK, blocked};

/// # Safety
///
/// The CPU must support AVX2 and FMA.
#[target_feature(enable = "avx2,fma")]
pub(crate) unsafe fn top_k_f32<F: MatrixOperands<Elem = f32>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    blocked::top_k::<gemm::F32Tile, F>(operands, rows, cols, out)
}

/// # Safety
///
/// The CPU must support AVX-512F, AVX2, and FMA.
#[target_feature(enable = "avx512f,avx2,fma")]
pub(crate) unsafe fn top_k_f32_avx512<F: MatrixOperands<Elem = f32>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    blocked::top_k::<avx512::F32Tile, F>(operands, rows, cols, out)
}

/// # Safety
///
/// The CPU must support AVX2 and FMA.
#[target_feature(enable = "avx2,fma")]
pub(crate) unsafe fn top_k_u8<F: MatrixOperands<Elem = u8>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    blocked::top_k::<gemm::U8Tile, F>(operands, rows, cols, out)
}

/// # Safety
///
/// The CPU must support AVX2 and FMA, and `operands.depth() * 8` must fit a `u16`.
#[target_feature(enable = "avx2,fma")]
pub(crate) unsafe fn top_k_bits<F: MatrixOperands<Elem = Bits>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    lut::top_k(operands, rows, cols, out)
}
