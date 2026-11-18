//! The vectors an index build reads: the quantized ones if there are any, otherwise the originals.
//!
//! A build with quantized vectors scores them, as HNSW does, so everything it derives from vectors
//! must come from them too and read no original. [`SourceVectors`] makes that choice once, and
//! gives the vectors as f32 rows ([`SourceVectors::f32_rows`]) and pair scores
//! ([`SourceVectors::pair_scorer`]).

mod original;
mod pair_scorer;
mod quantized;
#[cfg(test)]
pub(crate) mod test_fixture;

use common::types::PointOffsetType;

pub(crate) use self::original::F32Element;
pub use self::pair_scorer::PairScorer;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// The vectors of one segment that an index build scores and reads.
#[derive(Clone, Copy, Debug)]
pub enum SourceVectors<'a> {
    /// The original vectors, as stored.
    Original(&'a VectorStorageEnum),
    /// The quantized vectors, which replace the originals entirely.
    Quantized(&'a QuantizedVectors),
}

impl<'a> SourceVectors<'a> {
    /// `quantized` if given, otherwise `original`.
    pub fn new(original: &'a VectorStorageEnum, quantized: Option<&'a QuantizedVectors>) -> Self {
        match quantized {
            Some(quantized) => Self::Quantized(quantized),
            None => Self::Original(original),
        }
    }

    /// The vectors as f32 rows. `None` for those that have none: sparse vectors, multivectors,
    /// the empty storage, and product quantization.
    pub fn f32_rows(self) -> Option<Box<dyn F32Rows + 'a>> {
        match self {
            Self::Original(storage) => original::f32_rows(storage),
            Self::Quantized(quantized) => quantized::f32_rows(quantized),
        }
    }
}

/// Dense vectors of one storage as f32 rows, read in batches.
///
/// Original vectors come as `score_internal` scores them: as they are stored, converted to f32,
/// and TurboQuant datatype vectors decoded but still rotated. Quantized codes come as the vectors
/// whose geometry their symmetric score scores, see `symmetric_vector` of each quantization.
pub trait F32Rows: Sync {
    /// Length of a row.
    fn dim(&self) -> usize;

    /// Number of stored vectors, deleted ones included: ids must be below it.
    fn count(&self) -> usize;

    /// Calls `f(i, row, identity)` for every `ids[i]`.
    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], RowIdentity<'_>),
    ) -> OperationResult<()>;
}

/// What identifies a row to the scorer: rows with equal identities score alike against every row,
/// up to rounding.
pub enum RowIdentity<'a> {
    /// The row's values.
    Values,
    /// The stored code the row was decoded from, which also holds values the row doesn't show.
    Code(&'a [u8]),
    /// None: the scorer scores the row NaN against itself, so it equals no row.
    ScoresNan,
}
