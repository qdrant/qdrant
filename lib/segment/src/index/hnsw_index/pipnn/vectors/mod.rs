//! The vectors a PiPNN build reads: the quantized ones if there are any, otherwise the originals.
//!
//! A build with quantized vectors scores them, as HNSW does, so everything it derives from vectors
//! must come from them too and read no original. [`IndexBuildVectors`] makes that choice once, and
//! gives the vectors as matrix scores ([`IndexBuildVectors::matrix_scorer`]), as f32
//! ([`IndexBuildVectors::f32_reader`]), and as pair scores ([`IndexBuildVectors::pair_scorer`]).

mod original;
mod pair_scorer;
mod quantized;
#[cfg(test)]
pub(crate) mod test_fixture;

use common::types::PointOffsetType;
use quantization::DuplicateKey;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::matrix_scorer::{
    MatrixScorer, original_matrix_scorer, quantized_matrix_scorer,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// The vectors of one segment that a PiPNN build scores and reads.
#[derive(Clone, Copy, Debug)]
pub enum IndexBuildVectors<'a> {
    /// The original vectors, as stored.
    Original(&'a VectorStorageEnum),
    /// The quantized vectors, which replace the originals entirely.
    Quantized(&'a QuantizedVectors),
}

impl<'a> IndexBuildVectors<'a> {
    /// `quantized` if given, otherwise `original`.
    pub fn new(original: &'a VectorStorageEnum, quantized: Option<&'a QuantizedVectors>) -> Self {
        match quantized {
            Some(quantized) => Self::Quantized(quantized),
            None => Self::Original(original),
        }
    }

    /// Pairwise scoring of these vectors as matrix products. `None` if they have no matrix
    /// operands, so the caller builds with HNSW, see [`quantized_matrix_scorer`] and
    /// [`original_matrix_scorer`].
    pub fn matrix_scorer(self) -> Option<MatrixScorer<'a>> {
        match self {
            Self::Quantized(quantized) => quantized_matrix_scorer(quantized.get_storage()),
            Self::Original(storage) => original_matrix_scorer(storage),
        }
    }

    /// Reads the vectors as f32. `None` for those that have no f32 form: sparse vectors,
    /// multivectors, the empty storage, and product quantization.
    pub fn f32_reader(self) -> Option<Box<dyn F32VectorReader + 'a>> {
        match self {
            Self::Original(storage) => original::f32_reader(storage),
            Self::Quantized(quantized) => quantized::f32_reader(quantized),
        }
    }
}

/// Reads the dense vectors of one storage as f32, in batches.
///
/// Original vectors come as `score_internal` scores them: as they are stored, converted to f32,
/// and TurboQuant datatype vectors decoded but still rotated. Quantized codes come as the vectors
/// whose geometry their symmetric score scores, see [`quantization::SymmetricVectors`].
pub trait F32VectorReader: Sync {
    /// Length of a vector.
    fn dim(&self) -> usize;

    /// Number of stored vectors, deleted ones included: ids must be below it.
    fn count(&self) -> usize;

    /// Calls `f(i, vector, key)` for every `ids[i]`.
    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], DuplicateKey<'_>),
    ) -> OperationResult<()>;
}
