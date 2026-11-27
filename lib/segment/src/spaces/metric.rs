use common::types::ScoreType;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::DenseVector;
use crate::types::Distance;

/// Defines how to compare vectors
pub trait Metric<T: PrimitiveVectorElement> {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[T], v2: &[T]) -> ScoreType;

    /// [`Self::similarity`] of `query` with each of `vectors`, written to `scores`.
    ///
    /// The scores must be bit for bit those of [`Self::similarity`]: a point has to score the
    /// same alone and in a batch (replicas compare scores exactly). Metrics without a kernel
    /// that scores several vectors together use [`default_similarity_batch`].
    fn similarity_batch(query: &[T], vectors: &[impl AsRef<[T]>], scores: &mut [ScoreType]);

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: DenseVector) -> DenseVector;
}

pub trait MetricPostProcessing {
    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}

/// [`Metric::similarity_batch`] one vector at a time, with [`Metric::similarity`].
pub fn default_similarity_batch<T: PrimitiveVectorElement, M: Metric<T>>(
    query: &[T],
    vectors: &[impl AsRef<[T]>],
    scores: &mut [ScoreType],
) {
    debug_assert_eq!(vectors.len(), scores.len());
    for (score, vector) in scores.iter_mut().zip(vectors) {
        *score = M::similarity(query, vector.as_ref());
    }
}
