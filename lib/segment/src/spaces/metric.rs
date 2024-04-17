use common::types::ScoreType;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::DenseVector;
use crate::types::Distance;

/// Defines how to compare vectors
pub trait Metric<T: PrimitiveVectorElement> {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[T], v2: &[T]) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: DenseVector) -> DenseVector;
}

pub trait MetricPostProcessing {
    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}
