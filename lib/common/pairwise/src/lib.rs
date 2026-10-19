//! Pairwise scoring of stored vectors as matrix products, for index building.
//!
//! The crate knows nothing about storages or their scores: the kernels compute inner products.
//! A storage describes its vectors through [`MatrixOperands`]: how to decode them into a
//! kernel element type, which value per vector they carry, and its [`Combine`], which turns an
//! inner product and those values into the score of `score_internal`. [`top_k`] then returns
//! the best columns of each row, without materializing the score matrix.

mod combine;
mod fallback;
mod operands;
mod top_k;

use common::types::PointOffsetType;

pub use crate::combine::Combine;
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
    debug_assert!(
        !matches!(cols, Cols::Rows) || all_distinct(rows),
        "`Cols::Rows` needs rows without repeated points",
    );
    out.reset(rows.len());
    // Accelerated kernels go here, chosen by `F::Elem` and the CPU's features. With
    // `Cols::Rows`, they can exclude a point's pair with itself by position, as the fallback
    // does: the check above guarantees distinct rows.
    fallback::top_k(operands, rows, cols, out)
}

fn all_distinct(ids: &[PointOffsetType]) -> bool {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    sorted.windows(2).all(|pair| pair[0] != pair[1])
}
