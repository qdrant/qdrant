//! Pairwise scoring of stored points as matrix products, for index building.
//!
//! The kernels live in the `pairwise` crate. This module picks the vectors to score, the same
//! ones as `FilteredScorer::new_internal`, and describes them to the kernels as
//! [`MatrixOperands`]. A build scores in phases: a [`MatrixPhase`] takes the jobs of a phase,
//! schedules their matrix calls, and hands each job's result, a [`MatrixPicks`], to the caller.

mod cpu_jobs;
mod dense_operands;
mod job;
mod phase;
mod picks;
mod raw_picks;
#[cfg(test)]
mod tests;
mod top_k_pool;
mod turbo_source;

use common::types::PointOffsetType;
use pairwise::MatrixOperands;
pub use pairwise::{Cols as MatrixCols, TopK as MatrixTopK};
use quantization::encoded_vectors_binary::BinaryOperands;
use quantization::encoded_vectors_u8::ScalarOperands;
use quantization::turboquant::matrix::{TurboMatrixOperands, TurboSource};

use self::dense_operands::{DenseOperands, MatrixElement};
pub use self::job::MatrixJob;
use self::phase::CpuPhase;
pub use self::phase::MatrixPhase;
pub(crate) use self::phase::{Consume, DynPhase, Scored};
pub use self::picks::MatrixPicks;
pub(crate) use self::raw_picks::RawPicks;
use self::turbo_source::TurboStorageSource;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::turbo::shared::invert_score;
use crate::vector_storage::{DenseVectorStorageRead, TurboScoring, VectorStorageEnum};

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
        Self::from_dyn(operands)
    }

    /// A scorer on `matrix`, such as one that runs elsewhere than on the CPU kernels.
    pub(crate) fn from_dyn(matrix: impl DynMatrix + 'a) -> Self {
        Self {
            inner: Box::new(matrix),
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

    /// The matrix calls of a phase of the build, whose jobs each keep at most `k` best per
    /// row, see [`MatrixPhase`].
    ///
    /// Every row of every call gets `k` slots, so a `k` above the largest of the jobs, or
    /// above the columns of the largest job, only costs memory. The caller clamps it.
    #[must_use]
    pub fn phase(&self, k: usize) -> MatrixPhase<'_> {
        MatrixPhase::new(self.inner.phase(k))
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
        QuantizedVectorStorage::TQRam(q) => turbo_matrix_scorer(q.matrix_operands()),
        QuantizedVectorStorage::TQMmap(q) => turbo_matrix_scorer(q.matrix_operands()),
        QuantizedVectorStorage::TQChunkedMmap(q) => turbo_matrix_scorer(q.matrix_operands()),
        QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_) => None,
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

        VectorStorageEnum::DenseTurboMemmap(vs) => turbo_storage_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseTurboGraphInline(vs) => turbo_storage_matrix_scorer(vs.as_ref()),
        VectorStorageEnum::DenseTurboAppendableMemmap(vs) => {
            turbo_storage_matrix_scorer(vs.as_ref())
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(vs) => turbo_storage_matrix_scorer(vs.as_ref()),

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

/// A TurboQuant datatype storage scores as TurboQuant quantization does, negated for the
/// distances.
fn turbo_storage_matrix_scorer<S>(storage: &S) -> Option<MatrixScorer<'_>>
where
    S: TurboScoring + Sync,
{
    turbo_matrix_scorer(TurboMatrixOperands::new(
        storage.quantizer(),
        TurboStorageSource(storage),
        invert_score(storage.distance()),
    ))
}

fn turbo_matrix_scorer<'a, S>(
    operands: Option<TurboMatrixOperands<'a, S>>,
) -> Option<MatrixScorer<'a>>
where
    S: TurboSource + 'a,
    S::Error: Into<OperationError>,
{
    Some(match operands? {
        TurboMatrixOperands::Bits(operands) => MatrixScorer::from_operands(operands),
        TurboMatrixOperands::Codes2(operands) => MatrixScorer::from_operands(operands),
        TurboMatrixOperands::Codes4(operands) => MatrixScorer::from_operands(operands),
        TurboMatrixOperands::Codes8(operands) => MatrixScorer::from_operands(operands),
        TurboMatrixOperands::F32(operands) => MatrixScorer::from_operands(operands),
    })
}

/// Object-safe face of a [`MatrixOperands`]: one dynamic dispatch per matrix call, and
/// everything below it is monomorphized per implementation. A phase adds one per
/// [`MatrixPhase::for_each_top_k`], and one per report and result of a job.
///
/// Also what a scorer other than the CPU kernels implements, see [`MatrixScorer::from_dyn`].
pub(crate) trait DynMatrix: Sync {
    fn top_k(
        &self,
        rows: &[PointOffsetType],
        cols: MatrixCols<'_>,
        out: &mut MatrixTopK,
    ) -> OperationResult<()>;

    /// See [`MatrixScorer::phase`]. The default is [`Self::top_k`] calls in parallel on the
    /// current rayon pool, see [`cpu_jobs::for_each_top_k`].
    fn phase<'a>(&'a self, k: usize) -> Box<dyn DynPhase + 'a> {
        Box::new(CpuPhase::new(self, k))
    }
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
