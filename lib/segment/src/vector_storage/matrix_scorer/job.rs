//! One job of a batch, see [`MatrixPhase::for_each_top_k`].
//!
//! [`MatrixPhase::for_each_top_k`]: super::MatrixPhase::for_each_top_k

use common::types::PointOffsetType;

use super::MatrixCols;

/// The rows of one job and the columns to score them against, with the rules of
/// [`MatrixScorer::top_k`], and how many of the best columns its result keeps per row.
///
/// [`MatrixScorer::top_k`]: super::MatrixScorer::top_k
#[derive(Clone, Copy, Debug)]
pub struct MatrixJob<'a> {
    pub rows: &'a [PointOffsetType],
    pub cols: MatrixCols<'a>,
    /// Picks per row of the job's result, at most the `k` of the phase. The result of a job in
    /// progress takes 8 bytes per pick, so a job asks for no more than it reads.
    pub k: usize,
}
