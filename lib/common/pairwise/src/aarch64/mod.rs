//! NEON kernels: an FMA tile for f32, dot product instructions for u8, and XOR with byte
//! popcounts for bits. All three are tiles of the blocked product.
//!
//! Each entry point is the one `#[target_feature]` function of its kernel. The drivers below
//! it are `#[inline(always)]`, so they compile with the same features.

mod bits;
mod gemm;

use common::types::PointOffsetType;

use crate::{Bits, Cols, MatrixOperands, TopK, blocked};

/// # Safety
///
/// The CPU must support NEON.
#[target_feature(enable = "neon")]
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
/// The CPU must support NEON and the dot product instructions.
#[target_feature(enable = "neon,dotprod")]
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
/// The CPU must support NEON, and `operands.depth() * 8` must fit a `u16`.
#[target_feature(enable = "neon")]
pub(crate) unsafe fn top_k_bits<F: MatrixOperands<Elem = Bits>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    assert!(operands.depth() * 8 <= usize::from(u16::MAX));
    blocked::top_k::<bits::BitsTile, F>(operands, rows, cols, out)
}
