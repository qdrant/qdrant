//! Pairwise scoring of stored vectors as matrix products, for index building.
//!
//! The crate knows nothing about storages or their scores: the kernels compute inner products.
//! A storage describes its vectors through [`MatrixOperands`]: how to decode them into a
//! kernel element type, which values per vector they carry, and its [`Combine`], which turns an
//! inner product and those values into the score of `score_internal`. [`top_k`] then returns
//! the best columns of each row, without materializing the score matrix.

#[cfg(target_arch = "aarch64")]
mod aarch64;
mod blocked;
mod codes;
mod combine;
mod fallback;
mod kernel;
mod operands;
mod scratch;
#[cfg(test)]
mod tests;
mod top_k;
#[cfg(target_arch = "x86_64")]
mod x86_64;

use common::types::PointOffsetType;

pub use crate::codes::{Codebook, Codes2, Codes4, Codes8};
pub use crate::combine::{Aux, Combine, CombineDispatch, CombineFn};
#[doc(hidden)]
pub use crate::kernel::Kernel;
use crate::operands::sealed::Sealed as _;
pub use crate::operands::{Bits, KernelElem, MatrixOperands};
pub use crate::top_k::TopK;

/// The columns of a matrix call.
#[derive(Clone, Copy, Debug)]
pub enum Cols<'a> {
    /// All pairs within the rows, each point without itself. Computes one triangle. The rows
    /// must not repeat a point: positions are compared, not ids.
    Rows,
    /// The rows against these points. A point in both scores against itself.
    Other(&'a [PointOffsetType]),
}

/// For each `rows[r]`, the `out.k()` best columns by score, best first, ties to the lower
/// column position. Column positions index `cols`, or `rows` for [`Cols::Rows`].
pub fn top_k<F: MatrixOperands>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    top_k_with(Kernel::best(), operands, rows, cols, out)
}

/// [`top_k`] with a chosen kernel, for tests and benchmarks.
///
/// # Panics
///
/// If the CPU doesn't support `kernel`.
#[doc(hidden)]
pub fn top_k_with<F: MatrixOperands>(
    kernel: Kernel,
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    assert!(
        kernel.is_supported(),
        "{kernel:?} isn't supported by this CPU"
    );
    debug_assert!(
        !matches!(cols, Cols::Rows) || all_distinct(rows),
        "`Cols::Rows` needs rows without repeated points",
    );
    out.reset(rows.len());
    // With `Cols::Rows`, kernels exclude a point's pair with itself by position: the check
    // above guarantees distinct rows.
    match kernel {
        Kernel::Fallback => fallback::top_k(operands, rows, cols, out),
        // SAFETY: the CPU supports the kernel, checked above
        #[cfg(target_arch = "x86_64")]
        Kernel::Avx2 => unsafe { F::Elem::top_k_avx2(operands, rows, cols, out) },
        // SAFETY: as above
        #[cfg(target_arch = "x86_64")]
        Kernel::Avx512 => unsafe { F::Elem::top_k_avx512(operands, rows, cols, out) },
        // SAFETY: as above
        #[cfg(target_arch = "aarch64")]
        Kernel::Neon => unsafe { F::Elem::top_k_neon(operands, rows, cols, out) },
    }
}

fn all_distinct(ids: &[PointOffsetType]) -> bool {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    sorted.windows(2).all(|pair| pair[0] != pair[1])
}
