use common::types::ScoreType;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::DenseVector;
use crate::types::Distance;

/// Defines how to compare vectors
pub trait Metric<T: PrimitiveVectorElement> {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[T], v2: &[T]) -> ScoreType;

    /// Similarity between query and vector, where query can be in a different format (like quantized)
    fn query_similarity(query: &T::QueryType, vector: &[T]) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection
    /// Like normalization or rotations.
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: DenseVector) -> DenseVector;
}

pub trait MetricPostProcessing {
    /// Correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}
