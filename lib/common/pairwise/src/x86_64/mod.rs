//! AVX2 kernels: an FMA tile for f32, byte multiply-adds for u8, nibble lookups for bits, and
//! i16 multiply-adds for the code types.
//! AVX-512F adds a wider f32 tile, AVX-512 VNNI a byte tile for u8 and the code types.
//!
//! Each entry point is the one `#[target_feature]` function of its kernel. The drivers below
//! it are `#[inline(always)]`, so they compile with the same features.

mod avx512;
mod codes;
mod gemm;
mod lut;
mod vnni;

use common::types::PointOffsetType;

use self::codes::CodeTile;
pub(crate) use self::codes::{MAX_CODES, Unpack};
pub(crate) use self::vnni::Vnni;
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

/// # Safety
///
/// The CPU must support AVX2 and FMA, and `operands.depth() * U::CODES` must be at most
/// [`MAX_CODES`].
#[target_feature(enable = "avx2,fma")]
pub(crate) unsafe fn top_k_codes<U: Unpack, F: MatrixOperands<Elem = U>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    blocked::top_k::<CodeTile<U>, F>(operands, rows, cols, out)
}

/// # Safety
///
/// The CPU must support AVX-512F, AVX-512 VNNI, AVX2, and FMA, and for the code types
/// `operands.depth() * U::VALUES` must be at most [`MAX_CODES`].
#[target_feature(enable = "avx512f,avx512vnni,avx2,fma")]
pub(crate) unsafe fn top_k_vnni<U: Vnni, F: MatrixOperands<Elem = U>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    blocked::top_k::<vnni::VnniTile<U>, F>(operands, rows, cols, out)
}

/// [`top_k_vnni`] with the register tile emulated, see [`vnni::EmulatedTile`].
///
/// # Safety
///
/// The CPU must support AVX2 and FMA, and for the code types `operands.depth() * U::VALUES`
/// must be at most [`MAX_CODES`].
#[cfg(test)]
#[target_feature(enable = "avx2,fma")]
pub(crate) unsafe fn top_k_vnni_emulated<U: Vnni, F: MatrixOperands<Elem = U>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    out.reset(rows.len());
    blocked::top_k::<vnni::EmulatedTile<U>, F>(operands, rows, cols, out)
}
