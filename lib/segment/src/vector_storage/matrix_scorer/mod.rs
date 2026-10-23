//! Pairwise scoring of stored points as matrix products, for index building.
//!
//! The kernels live in the `pairwise` crate. This module picks the vectors to score, the same
//! ones as `FilteredScorer::new_internal`, and describes them to the kernels as
//! [`MatrixOperands`].

mod dense_operands;
#[cfg(test)]
mod tests;

use common::types::PointOffsetType;
use pairwise::MatrixOperands;
pub use pairwise::{Cols as MatrixCols, TopK as MatrixTopK};
use quantization::encoded_vectors_binary::BinaryOperands;
use quantization::encoded_vectors_u8::ScalarOperands;

use self::dense_operands::{DenseOperands, MatrixElement};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{DenseVectorStorageRead, VectorStorageEnum};

/// Scores pairs of stored points of one segment, shared by all threads of a build.
pub struct MatrixScorer<'a> {
    inner: Box<dyn DynMatrix + 'a>,
}

impl<'a> MatrixScorer<'a> {
    fn from_operands<F>(operands: F) -> Self
    where
        F: MatrixOperands + 'a,
        F::Error: Into<OperationError>,
    {
        Self {
            inner: Box::new(operands),
        }
    }

    /// For each `rows[r]`, the `out.k()` best columns by `score_internal`, best first, ties to
    /// the lower column position. Column positions index the points of `cols`, or `rows` for
    /// [`MatrixCols::Rows`].
    ///
    /// With [`MatrixCols::Rows`], `rows` must not repeat a point. Points are kept from scoring
    /// against themselves by position, so a repeated point is scored against its copy and can
    /// become its own neighbor. Checked only in debug builds. Deduplicate leaves built by
    /// merging clusters, since fanout can put a point into several of them.
    pub fn top_k(
        &self,
        rows: &[PointOffsetType],
        cols: MatrixCols<'_>,
        out: &mut MatrixTopK,
    ) -> OperationResult<()> {
        self.inner.top_k(rows, cols, out)
    }
}

/// Pairwise scoring of stored points for index building: the quantized vectors if configured,
/// otherwise the original ones. `None` if those have no matrix operands (Manhattan, sparse,
/// multivectors, and quantizations without them yet), so the caller builds with HNSW.
pub fn new_matrix_scorer<'a>(
    vector_storage: &'a VectorStorageEnum,
    quantized_vectors: Option<&'a QuantizedVectors>,
) -> Option<MatrixScorer<'a>> {
    match quantized_vectors {
        Some(quantized_vectors) => quantized_matrix_scorer(quantized_vectors.get_storage()),
        None => original_matrix_scorer(vector_storage),
    }
}

/// Quantized vectors without operands return `None` instead of falling back to the originals,
/// so PiPNN never scores with other vectors than HNSW would.
fn quantized_matrix_scorer(storage: &QuantizedVectorStorage) -> Option<MatrixScorer<'_>> {
    match storage {
        QuantizedVectorStorage::ScalarRam(q) => {
            ScalarOperands::new(q).map(MatrixScorer::from_operands)
        }
        QuantizedVectorStorage::ScalarMmap(q) => {
            ScalarOperands::new(q).map(MatrixScorer::from_operands)
        }
        QuantizedVectorStorage::ScalarChunkedMmap(q) => {
            ScalarOperands::new(q).map(MatrixScorer::from_operands)
        }
        QuantizedVectorStorage::BinaryRam(q) => {
            Some(MatrixScorer::from_operands(BinaryOperands::new(q)))
        }
        QuantizedVectorStorage::BinaryMmap(q) => {
            Some(MatrixScorer::from_operands(BinaryOperands::new(q)))
        }
        QuantizedVectorStorage::BinaryChunkedMmap(q) => {
            Some(MatrixScorer::from_operands(BinaryOperands::new(q)))
        }
        QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_)
        | QuantizedVectorStorage::TQRam(_)
        | QuantizedVectorStorage::TQMmap(_)
        | QuantizedVectorStorage::TQChunkedMmap(_) => None,
        QuantizedVectorStorage::ScalarRamMulti(_)
        | QuantizedVectorStorage::ScalarMmapMulti(_)
        | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
        | QuantizedVectorStorage::PQRamMulti(_)
        | QuantizedVectorStorage::PQMmapMulti(_)
        | QuantizedVectorStorage::PQChunkedMmapMulti(_)
        | QuantizedVectorStorage::BinaryRamMulti(_)
        | QuantizedVectorStorage::BinaryMmapMulti(_)
        | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
        | QuantizedVectorStorage::TQRamMulti(_)
        | QuantizedVectorStorage::TQMmapMulti(_)
        | QuantizedVectorStorage::TQChunkedMmapMulti(_) => None,
    }
}

fn original_matrix_scorer(storage: &VectorStorageEnum) -> Option<MatrixScorer<'_>> {
    match storage {
        VectorStorageEnum::DenseVolatile(vs) => dense_matrix_scorer(vs),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(vs) => dense_matrix_scorer(vs),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(vs) => dense_matrix_scorer(vs),

        VectorStorageEnum::DenseMemmap(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseMemmapByte(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseMemmapHalf(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseGraphInline(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseGraphInlineByte(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseGraphInlineHalf(vs) => dense_matrix_scorer(vs.as_ref()),

        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(vs) => dense_matrix_scorer(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(vs) => dense_matrix_scorer(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringHalf(vs) => dense_matrix_scorer(vs.as_ref()),

        VectorStorageEnum::DenseAppendableMemmap(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseAppendableMemmapByte(vs) => dense_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseAppendableMemmapHalf(vs) => dense_matrix_scorer(vs.as_ref()),

        // TurboQuant storages get operands through `TurboScoring` later
        VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboGraphInline(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_) => None,
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(_) => None,

        VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::EmptySparse(_) => None,
        VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::MultiDenseTurbo(_) => None,
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => None,
        // Nothing to score
        VectorStorageEnum::EmptyDense(_) => None,
    }
}

fn dense_matrix_scorer<T, S>(storage: &S) -> Option<MatrixScorer<'_>>
where
    T: MatrixElement,
    S: DenseVectorStorageRead<T> + Sync,
{
    DenseOperands::new(storage, storage.distance()).map(MatrixScorer::from_operands)
}

/// Object-safe face of a [`MatrixOperands`]: the only dynamic dispatch, once per matrix call.
/// Everything below it is monomorphized per implementation.
trait DynMatrix: Sync {
    fn top_k(
        &self,
        rows: &[PointOffsetType],
        cols: MatrixCols<'_>,
        out: &mut MatrixTopK,
    ) -> OperationResult<()>;
}

impl<F> DynMatrix for F
where
    F: MatrixOperands,
    F::Error: Into<OperationError>,
{
    fn top_k(
        &self,
        rows: &[PointOffsetType],
        cols: MatrixCols<'_>,
        out: &mut MatrixTopK,
    ) -> OperationResult<()> {
        pairwise::top_k(self, rows, cols, out).map_err(Into::into)
    }
}
