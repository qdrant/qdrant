//! Pairwise scoring of stored vectors as matrix products, for index building.
//!
//! The crate knows nothing about storages. A storage describes its vectors through
//! [`MatrixOperands`]: how to decode them into a kernel element type, which value per vector
//! they carry, and how an inner product and those values combine into the score of
//! `score_internal`. [`top_k`] then returns the best columns of each row, without
//! materializing the score matrix.

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
    /// All pairs within the rows, each point without itself. Computes one triangle.
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
    out.reset(rows.len());
    // Accelerated kernels go here, chosen by `F::Elem` and the CPU's features.
    fallback::top_k(operands, rows, cols, out)
}
